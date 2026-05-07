"""
bula_gratis_crawler.py
-----------------------
Coleta bulas do bula.gratis, parseia as 9 seções, gera Medicines + Chunks
(com embeddings via OpenAI) e salva no SQLite / JSON.

Funções públicas:
  get_by_name(name, save_jsons, save_sqlite)  — busca um medicamento pelo nome
  crawl(letters, save_jsons, save_sqlite)     — varre todas as letras

Uso rápido:
    python bula_gratis_crawler.py

Dependências:
    pip install requests beautifulsoup4 openai
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
import uuid
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from bs4 import Tag

from bula_check.protocol import DEFAULT_DB_PATH
from bula_check.protocol import OPENAI_EMBEDDING_DIM
from bula_check.protocol import SECTION_PATTERNS
from bula_check.protocol import Chunks
from bula_check.protocol import Medicines
from bula_check.protocol import Section
from bula_check.protocol import init_db
from bula_check.protocol import normalize_for_matching
from bula_check.protocol import normalize_processed_field
from bula_check.protocol import save_chunk
from bula_check.protocol import save_medicine

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
BASE_URL = "https://bula.gratis"
INDEX_URL = f"{BASE_URL}/todas"

DEFAULT_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

SLEEP_BETWEEN_PAGES = 0.5
SLEEP_BETWEEN_BULAS = 0.3
MAX_RETRIES = 3
RETRY_BACKOFF = 10

OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"
LOG_FILE = Path("bula_gratis_crawler.log")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _get(session: requests.Session, url: str) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            log.warning(
                "Tentativa %d/%d falhou para %s: %s", attempt, MAX_RETRIES, url, exc
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
    raise RuntimeError(f"Falhou após {MAX_RETRIES} tentativas: {url}")


# ---------------------------------------------------------------------------
# Coleta de links do índice
# ---------------------------------------------------------------------------
def _collect_links_for_letter(session: requests.Session, letter: str) -> list[dict]:
    """Retorna lista de dicts {url, drug_name, company_name, cnpj, empresa_slug, medicamento_slug}."""
    url = f"{INDEX_URL}/{letter}"
    resp = _get(session, url)
    soup = BeautifulSoup(resp.text, "html.parser")

    links: list[dict] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        label = a.get_text(strip=True)

        if not href.startswith("https://bula.gratis/"):
            continue
        parts = [p for p in href.replace("https://bula.gratis/", "").split("/") if p]
        if len(parts) < 3:
            continue
        if href in seen:
            continue
        seen.add(href)

        empresa_slug = parts[0]
        medicamento_slug = parts[2]
        drug_name, company_name, cnpj = _parse_label(label)

        links.append(
            {
                "url": href,
                "drug_name": drug_name,
                "company_name": company_name,
                "cnpj": cnpj,
                "empresa_slug": empresa_slug,
                "medicamento_slug": medicamento_slug,
            }
        )

    return links


def _search_links_by_name(session: requests.Session, name: str) -> list[dict]:
    """Busca links no índice da letra inicial de `name`."""
    first = name.strip()[:1].upper()
    if first not in DEFAULT_LETTERS:
        first = "A"
    links = _collect_links_for_letter(session, first)
    name_norm = normalize_for_matching(name)
    return [
        lk
        for lk in links
        if name_norm in normalize_for_matching(lk["drug_name"])
        or name_norm in normalize_for_matching(lk["medicamento_slug"])
    ]


def _parse_label(label: str) -> tuple[str, str | None, str | None]:
    """
    'Acebrofilina - Germed Farmaceutica Ltda - 45992062000181'
    → ('Acebrofilina', 'Germed Farmaceutica Ltda', '45992062000181')
    """
    parts = [p.strip() for p in label.split(" - ")]
    drug = parts[0] if parts else label
    company = parts[1] if len(parts) > 1 else None
    cnpj = parts[2] if len(parts) > 2 else None
    if cnpj and not re.fullmatch(r"\d{14}", re.sub(r"\D", "", cnpj)):
        company = f"{company} - {cnpj}" if company else cnpj
        cnpj = None
    elif cnpj:
        cnpj = re.sub(r"\D", "", cnpj)
    return drug, company, cnpj


# ---------------------------------------------------------------------------
# Parsing HTML da bula
# ---------------------------------------------------------------------------
def _build_patient_url(url: str) -> str:
    return url if url.endswith("/paciente") else url.rstrip("/") + "/paciente"


def _get_drug_name(soup: BeautifulSoup) -> str | None:
    tag = soup.select_one("#nome_medicamento")
    if tag:
        return re.sub(r"\s+", " ", tag.get_text()).strip()
    title = soup.title.get_text() if soup.title else ""
    m = re.match(r"^(.*?)\s*\(Bula", title, re.IGNORECASE)
    return re.sub(r"\s+", " ", m.group(1)).strip() if m else None


def _get_company_name(soup: BeautifulSoup) -> str | None:
    tag = soup.select_one("#empresa_medicamento")
    if tag:
        text = re.sub(r"\s+", " ", tag.get_text(" ", strip=True)).strip()
        text = re.sub(r"\s*-\s*\d{8,14}\b.*$", "", text).strip()
        return text or None
    return None


def _find_article(soup: BeautifulSoup) -> Tag | None:
    return soup.find("article")  # type: ignore[return-value]


def _collect_heading_blocks(root: Tag) -> list[dict[str, str]]:
    """Agrupa o conteúdo da bula em blocos {heading, content}."""
    blocks: list[dict[str, str]] = []
    current_heading: str | None = None
    current_parts: list[str] = []

    for node in root.find_all(["h3", "p"], recursive=True):
        text = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
        if not text:
            continue
        if node.name == "h3":
            if current_heading is not None:
                blocks.append(
                    {"heading": current_heading, "content": " ".join(current_parts)}
                )
            current_heading = text
            current_parts = []
        elif current_heading is not None:
            current_parts.append(text)

    if current_heading is not None:
        blocks.append(
            {"heading": current_heading, "content": " ".join(current_parts)}
        )

    return blocks


# ---------------------------------------------------------------------------
# Extração de seções
# ---------------------------------------------------------------------------
def _extract_sections(blocks: list[dict[str, str]]) -> dict[str, str | None]:
    """Mapeia blocos para as 9 seções canônicas. Retorna {section_name: text|None}."""
    sections: dict[str, str | None] = {s.value: None for s in Section}

    for block in blocks:
        heading_norm = normalize_for_matching(block["heading"])
        for section_name, patterns in SECTION_PATTERNS.items():
            if sections[section_name] is not None:
                continue
            if any(p in heading_norm for p in patterns):
                sections[section_name] = block["content"] or None

    return sections


# ---------------------------------------------------------------------------
# Chunking: seção → parágrafos → sentenças
# ---------------------------------------------------------------------------
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
_PARA_SPLIT = re.compile(r"\n{2,}|\.\s{2,}")


def _split_into_paragraphs(text: str) -> list[str]:
    paras = _PARA_SPLIT.split(text)
    return [p.strip() for p in paras if p.strip()]


def _split_into_sentences(paragraph: str) -> list[str]:
    sentences = _SENTENCE_SPLIT.split(paragraph)
    return [s.strip() for s in sentences if s.strip()]


def _count_words(text: str) -> int:
    return len(text.split())


def _clean_text_for_embedding(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _split_paragraph_into_chunks(
    paragraph: str,
    max_words: int = 250,
) -> list[str]:
    """Split a paragraph into chunks with at most `max_words`.

    The function first applies a light text cleanup for embedding. Then it
    tries to split by sentence. If a single sentence is longer than
    `max_words`, that sentence is split by words as a fallback.

    Parameters
    ----------
    paragraph : str
        Paragraph text to split.
    max_words : int, optional
        Maximum number of words per chunk, by default 250.

    Returns
    -------
    list[str]
        List of text chunks.
    """
    paragraph = _clean_text_for_embedding(paragraph)

    if not paragraph:
        return []

    sentences = _split_into_sentences(paragraph)

    chunks: list[str] = []
    current_chunk: list[str] = []
    current_word_count = 0

    for sentence in sentences:
        sentence = _clean_text_for_embedding(sentence)
        sentence_word_count = _count_words(sentence)

        if sentence_word_count == 0:
            continue

        if sentence_word_count > max_words:
            if current_chunk:
                chunks.append(_clean_text_for_embedding(" ".join(current_chunk)))
                current_chunk = []
                current_word_count = 0

            words = sentence.split()

            for start in range(0, len(words), max_words):
                chunk_words = words[start : start + max_words]
                chunk = _clean_text_for_embedding(" ".join(chunk_words))

                if chunk:
                    chunks.append(chunk)

            continue

        if current_word_count + sentence_word_count <= max_words:
            current_chunk.append(sentence)
            current_word_count += sentence_word_count
        else:
            if current_chunk:
                chunks.append(_clean_text_for_embedding(" ".join(current_chunk)))

            current_chunk = [sentence]
            current_word_count = sentence_word_count

    if current_chunk:
        chunks.append(_clean_text_for_embedding(" ".join(current_chunk)))

    return chunks


def _build_chunks_for_medicine(
    medicine_id: str,
    medicine_name: str,
    sections: dict[str, str | None],
) -> list[Chunks]:
    """
    Divide cada seção em parágrafos e cada parágrafo em sentenças (chunks).
    O embedding é preenchido depois por `_embed_chunks`.
    """
    chunks: list[Chunks] = []
    dummy_embedding: list[float] = [0.0] * OPENAI_EMBEDDING_DIM

    for section_name, text in sections.items():
        if not text:
            continue
        try:
            section_enum = Section(section_name)
        except ValueError:
            continue

        for para_idx, paragraph in enumerate(_split_into_paragraphs(text)):
            for chunk_idx, sentence in enumerate(
                _split_paragraph_into_chunks(paragraph)
            ):
                chunks.append(
                    Chunks(
                        id=str(uuid.uuid4()),
                        medicine_id=medicine_id,
                        medicine_name=medicine_name,
                        section=section_enum,
                        paragraph_idx=para_idx,
                        chunk_idx=chunk_idx,
                        text=sentence,
                        embedding=dummy_embedding,
                    )
                )

    return chunks


# ---------------------------------------------------------------------------
# Embeddings via OpenAI
# ---------------------------------------------------------------------------
def _embed_chunks(chunks: list[Chunks]) -> list[Chunks]:
    """
    Preenche os embeddings dos chunks usando OpenAI.
    Requer OPENAI_API_KEY no ambiente.
    Faz batches de 100 textos por chamada.
    """
    if not chunks:
        return chunks

    try:
        from openai import OpenAI  # type: ignore
    except ImportError:
        log.warning(
            "openai não instalado — embeddings não gerados. pip install openai"
        )
        return chunks

    from dotenv import load_dotenv

    load_dotenv()

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        log.warning("OPENAI_API_KEY não encontrada — embeddings não gerados.")
        return chunks

    client = OpenAI(api_key=api_key)
    batch_size = 100

    for i in range(0, len(chunks), batch_size):
        batch = chunks[i : i + batch_size]
        texts = [c.text for c in batch]
        try:
            response = client.embeddings.create(
                model=OPENAI_EMBEDDING_MODEL,
                input=texts,
            )
            for j, emb_data in enumerate(response.data):
                batch[j] = batch[j].model_copy(
                    update={"embedding": emb_data.embedding}
                )
                chunks[i + j] = batch[j]
        except Exception as exc:
            log.error(
                "Erro ao gerar embeddings para batch %d: %s", i // batch_size, exc
            )

    return chunks


# ---------------------------------------------------------------------------
# Scrape de uma bula completa → (Medicines, list[Chunks])
# ---------------------------------------------------------------------------
def _scrape_bula(
    session: requests.Session,
    link: dict,
) -> tuple[Medicines, list[Chunks]] | None:
    """Baixa e parseia uma bula. Retorna (Medicines, chunks) ou None se falhar."""
    patient_url = _build_patient_url(link["url"])

    try:
        resp = _get(session, patient_url)
    except RuntimeError as exc:
        log.error("Falhou ao baixar %s: %s", patient_url, exc)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    article = _find_article(soup)

    drug_name = _get_drug_name(soup) or link["drug_name"]
    company_name = (
        _get_company_name(soup) or link.get("company_name") or "Desconhecida"
    )
    cnpj = link.get("cnpj")

    blocks = _collect_heading_blocks(article) if article else []
    sections = _extract_sections(blocks)

    medicine_id = str(uuid.uuid4())

    ai_raw: list[str] | None = (
        None  # BulaGratis não expõe princípio ativo estruturado
    )

    medicine = Medicines(
        id=medicine_id,
        name=drug_name,
        processed_name=normalize_processed_field(drug_name),
        active_ingredient=ai_raw,
        processed_active_ingredient=None,
        source="bula_gratis",
        url=link["url"],
        registration_number=None,
        therapeutic_classes=None,
        company_name=company_name,
        processed_company_name=normalize_processed_field(company_name),
        cnpj=cnpj,
    )

    chunks = _build_chunks_for_medicine(medicine_id, drug_name, sections)

    return medicine, chunks


# ---------------------------------------------------------------------------
# Função pública: busca por nome
# ---------------------------------------------------------------------------
def get_by_name(
    name: str,
    save_jsons: bool = False,
    save_sqlite: bool = False,
    embed: bool = False,
    db_path: Path = DEFAULT_DB_PATH,
    json_output_dir: Path = Path("outputs/bula_gratis/json"),
) -> list[tuple[Medicines, list[Chunks]]]:
    """
    Busca bulas pelo nome do medicamento no BulaGratis.

    Parameters
    ----------
    name : str
        Nome (ou prefixo) do medicamento.
    save_jsons : bool
        Salva JSON de debug com medicines e chunks resultantes.
    save_sqlite : bool
        Upsert no banco SQLite (tabelas medicines e chunks).
    embed : bool
        Se True, gera embeddings via OpenAI para os chunks.
    db_path : Path
        Caminho do banco SQLite.
    json_output_dir : Path
        Diretório para JSON de debug.

    Returns
    -------
    list[tuple[Medicines, list[Chunks]]]
    """
    session = _make_session()
    links = _search_links_by_name(session, name)
    log.info("Encontrados %d links para %r", len(links), name)

    results: list[tuple[Medicines, list[Chunks]]] = []

    for link in links:
        log.info("  Scraping: %s", link["url"])
        result = _scrape_bula(session, link)
        if result is None:
            continue

        medicine, chunks = result

        if embed:
            chunks = _embed_chunks(chunks)

        results.append((medicine, chunks))
        time.sleep(SLEEP_BETWEEN_BULAS)

    _persist(
        results,
        save_jsons,
        save_sqlite,
        db_path,
        json_output_dir,
        prefix=f"name_{name}",
    )
    return results


# ---------------------------------------------------------------------------
# Função pública: crawler completo
# ---------------------------------------------------------------------------
def crawl(
    letters: str = DEFAULT_LETTERS,
    save_jsons: bool = False,
    save_sqlite: bool = False,
    embed: bool = False,
    db_path: Path = DEFAULT_DB_PATH,
    json_output_dir: Path = Path("outputs/bula_gratis/json"),
) -> list[tuple[Medicines, list[Chunks]]]:
    """
    Varre o índice do BulaGratis letra a letra.

    Parameters
    ----------
    letters : str
        String com as letras a varrer. Ex: "ABCDEFGHIJKLMNOPQRSTUVWXYZ" ou "AB".
    save_jsons : bool
        Salva JSON único de debug com todos os resultados.
    save_sqlite : bool
        Upsert incremental no banco SQLite.
    embed : bool
        Se True, gera embeddings para cada chunk via OpenAI.
    db_path : Path
        Caminho do banco SQLite.
    json_output_dir : Path
        Diretório para JSON de debug.

    Returns
    -------
    list[tuple[Medicines, list[Chunks]]]
    """
    session = _make_session()
    all_results: list[tuple[Medicines, list[Chunks]]] = []
    seen_urls: set[str] = set()

    conn: sqlite3.Connection | None = None
    if save_sqlite:
        conn = init_db(db_path)

    try:
        for letter in letters.upper():
            log.info("=== Letra %s ===", letter)
            try:
                links = _collect_links_for_letter(session, letter)
            except Exception as exc:
                log.error("Erro ao coletar índice para letra %s: %s", letter, exc)
                continue

            log.info("  %d bulas encontradas para letra %s", len(links), letter)

            for i, link in enumerate(links, 1):
                if link["url"] in seen_urls:
                    continue
                seen_urls.add(link["url"])

                log.info("  [%s %d/%d] %s", letter, i, len(links), link["url"])

                result = _scrape_bula(session, link)
                if result is None:
                    continue

                medicine, chunks = result

                if embed:
                    chunks = _embed_chunks(chunks)

                all_results.append((medicine, chunks))

                if conn is not None:
                    save_medicine(conn, medicine)
                    for chunk in chunks:
                        save_chunk(conn, chunk)
                    conn.commit()

                time.sleep(SLEEP_BETWEEN_BULAS)

            time.sleep(SLEEP_BETWEEN_PAGES)

    finally:
        if conn is not None:
            n_m = conn.execute(
                "SELECT COUNT(*) FROM medicines WHERE source='bula_gratis'"
            ).fetchone()[0]
            n_c = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
            log.info("SQLite: %d medicamentos | %d chunks em %s", n_m, n_c, db_path)
            conn.close()

    _persist(all_results, save_jsons, False, db_path, json_output_dir, prefix="all")
    log.info(
        "Crawler BulaGratis concluído — %d medicamentos | %d chunks",
        len(all_results),
        sum(len(c) for _, c in all_results),
    )
    return all_results


# ---------------------------------------------------------------------------
# Persistência auxiliar
# ---------------------------------------------------------------------------
def _persist(
    results: list[tuple[Medicines, list[Chunks]]],
    save_jsons: bool,
    save_sqlite: bool,
    db_path: Path,
    json_output_dir: Path,
    prefix: str,
) -> None:
    if save_jsons and results:
        json_output_dir.mkdir(parents=True, exist_ok=True)
        safe_prefix = re.sub(r"\W+", "_", prefix).strip("_").lower()
        out = json_output_dir / f"bula_gratis_{safe_prefix}.json"
        payload = [
            {
                "medicine": m.model_dump(),
                "chunks": [c.model_dump() for c in chunks],
            }
            for m, chunks in results
        ]
        out.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        log.info("JSON salvo: %s (%d itens)", out, len(results))

    if save_sqlite and results:
        conn = init_db(db_path)
        try:
            for medicine, chunks in results:
                save_medicine(conn, medicine)
                for chunk in chunks:
                    save_chunk(conn, chunk)
            conn.commit()
            log.info("SQLite: %d medicamentos salvos em %s", len(results), db_path)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Iniciando crawler BulaGratis — %d letras", len(DEFAULT_LETTERS))
    crawl(save_sqlite=True)
    log.info("Pronto! DB: %s  Log: %s", DEFAULT_DB_PATH, LOG_FILE)
