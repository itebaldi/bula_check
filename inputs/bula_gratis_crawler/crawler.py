"""
bula_gratis_crawler.py
-----------------------
Coleta todas as bulas do bula.gratis e salva em SQLite (+ CSV opcional).

Estratégia:
  - Varre /todas/A até /todas/Z
  - Cada página lista links no formato:
      https://bula.gratis/{empresa_slug}/0/{medicamento_slug}
      label: "Nome - Empresa - CNPJ"
  - Para cada link, acessa /{url}/paciente e extrai:
      nome, empresa, cnpj, seções (indicações, contraindicações, etc.)

Uso:
    python bula_gratis_crawler.py

Dependências:
    pip install requests beautifulsoup4

Saída:
    bula_gratis.db    — SQLite
    bula_gratis.csv   — CSV (opcional, SET SAVE_CSV = True)
    crawler.log
"""

from __future__ import annotations

import csv
import logging
import re
import sqlite3
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
BASE_URL = "https://bula.gratis"
INDEX_URL = f"{BASE_URL}/todas"
LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
# LETTERS = "A"

SLEEP_BETWEEN_PAGES = 0.5  # entre páginas do índice (A, B, C...)
SLEEP_BETWEEN_BULAS = 0.3  # entre cada bula
MAX_RETRIES = 3
RETRY_BACKOFF = 10  # segundos após erro

OUTPUT_DB = Path("bula_gratis.db")
OUTPUT_CSV = Path("bula_gratis.csv")
LOG_FILE = Path("bula_gratis_crawler.log")

SAVE_CSV = False  # mude para True se quiser o CSV também

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
# Modelo
# ---------------------------------------------------------------------------
@dataclass
class BulaRecord:
    drug_name: str
    company_name: str | None
    cnpj: str | None
    empresa_slug: str | None  # segmento da URL
    medicamento_slug: str | None  # segmento da URL
    source_url: str
    patient_url: str
    crawled_at: str = ""

    def __post_init__(self) -> None:
        if not self.crawled_at:
            self.crawled_at = datetime.now().isoformat()


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------
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
# Coleta links do índice
# ---------------------------------------------------------------------------
def _collect_links_for_letter(session: requests.Session, letter: str) -> list[dict]:
    """
    Retorna lista de dicts com:
      url, drug_name, company_name, cnpj, empresa_slug, medicamento_slug

    O label já vem formatado: "Nome - Empresa - CNPJ"
    A URL tem formato: https://bula.gratis/{empresa_slug}/0/{medicamento_slug}
    """
    url = f"{INDEX_URL}/{letter}"
    resp = _get(session, url)
    soup = BeautifulSoup(resp.text, "html.parser")

    links = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        label = a.get_text(strip=True)

        # Links de bula têm domínio completo e 3 segmentos: /empresa/0/medicamento
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

        # Parseia o label: "Nome - Empresa - CNPJ"
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


def _parse_label(label: str) -> tuple[str, str | None, str | None]:
    """
    'Acebrofilina - Germed Farmaceutica Ltda - 45992062000181'
    → ('Acebrofilina', 'Germed Farmaceutica Ltda', '45992062000181')
    """
    parts = [p.strip() for p in label.split(" - ")]
    drug = parts[0] if len(parts) > 0 else label
    company = parts[1] if len(parts) > 1 else None
    cnpj = parts[2] if len(parts) > 2 else None
    # CNPJ: só dígitos, 14 chars
    if cnpj and not re.fullmatch(r"\d{14}", re.sub(r"\D", "", cnpj)):
        # provavelmente não é CNPJ — pode ser parte do nome da empresa
        company = f"{company} - {cnpj}" if company else cnpj
        cnpj = None
    elif cnpj:
        cnpj = re.sub(r"\D", "", cnpj)
    return drug, company, cnpj


# ---------------------------------------------------------------------------
# Extrai conteúdo da página de bula
# ---------------------------------------------------------------------------
_SECTION_PATTERNS: dict[str, list[str]] = {
    "indications": ["indicac", "para que", "uso indicado"],
    "contraindications": ["contraindicac", "quando nao", "nao deve"],
    "warnings_and_precautions": ["advertencia", "precaucao", "cuidado", "atencao"],
    "adverse_reactions": ["reacao adversa", "efeito colateral", "efeito indesejado"],
}


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", text).strip().lower()


def _extract_sections(article) -> dict[str, str | None]:
    sections: dict[str, str | None] = {k: None for k in _SECTION_PATTERNS}
    if article is None:
        return sections

    current_heading = None
    current_parts: list[str] = []
    blocks: list[tuple[str, str]] = []

    for node in article.find_all(["h3", "p"], recursive=True):
        text = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
        if not text:
            continue
        if node.name == "h3":
            if current_heading is not None:
                blocks.append((current_heading, " ".join(current_parts)))
            current_heading = text
            current_parts = []
        else:
            if current_heading:
                current_parts.append(text)

    if current_heading:
        blocks.append((current_heading, " ".join(current_parts)))

    for heading, content in blocks:
        heading_norm = _normalize(heading)
        for field, patterns in _SECTION_PATTERNS.items():
            if sections[field] is not None:
                continue
            if any(p in heading_norm for p in patterns):
                sections[field] = content or None

    return sections


def _scrape_bula(session: requests.Session, link: dict) -> BulaRecord:
    """Acessa a página /paciente e extrai todos os campos."""
    patient_url = link["url"].rstrip("/") + "/paciente"
    resp = _get(session, patient_url)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Nome e empresa do HTML (mais limpo que o label)
    nome_tag = soup.select_one("#nome_medicamento")
    empresa_tag = soup.select_one("#empresa_medicamento")

    drug_name = (
        re.sub(r"\s+", " ", nome_tag.get_text(strip=True)).strip()
        if nome_tag
        else link["drug_name"]
    )
    company_raw = (
        re.sub(r"\s+", " ", empresa_tag.get_text(strip=True)).strip()
        if empresa_tag
        else None
    )
    # Remove "BULA DO PACIENTE" e CNPJ do campo empresa
    company_name = link["company_name"]
    cnpj = link["cnpj"]
    if company_raw:
        clean = re.sub(
            r"BULA DO (PACIENTE|PROFISSIONAL).*$", "", company_raw, flags=re.I
        ).strip()
        # separa CNPJ se vier junto
        m = re.match(r"^(.*?)\s*[-–]\s*(\d{14})\b", clean)
        if m:
            company_name = m.group(1).strip() or company_name
            cnpj = m.group(2) or cnpj
        else:
            company_name = clean or company_name

    # article = soup.find("article")
    # sections = _extract_sections(article)

    return BulaRecord(
        drug_name=drug_name,
        company_name=company_name,
        cnpj=cnpj,
        empresa_slug=link["empresa_slug"],
        medicamento_slug=link["medicamento_slug"],
        source_url=link["url"],
        patient_url=patient_url,
    )


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------
_DDL = """
CREATE TABLE IF NOT EXISTS bulas (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    drug_name                 TEXT NOT NULL,
    company_name              TEXT,
    cnpj                      TEXT,
    empresa_slug              TEXT,
    medicamento_slug          TEXT,
    source_url                TEXT NOT NULL UNIQUE,
    patient_url               TEXT NOT NULL,
    crawled_at                TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bulas_drug_name ON bulas(drug_name);
CREATE INDEX IF NOT EXISTS idx_bulas_cnpj      ON bulas(cnpj);
"""

_UPSERT = """
INSERT INTO bulas (
    drug_name, company_name, cnpj, empresa_slug, medicamento_slug,
    source_url, patient_url,
    crawled_at
) VALUES (
    :drug_name, :company_name, :cnpj, :empresa_slug, :medicamento_slug,
    :source_url, :patient_url,
    :crawled_at
)
ON CONFLICT(source_url) DO UPDATE SET
    drug_name                = excluded.drug_name,
    company_name             = excluded.company_name,
    cnpj                     = excluded.cnpj,
    crawled_at               = excluded.crawled_at;
"""

CSV_FIELDS = [
    "drug_name",
    "company_name",
    "cnpj",
    "empresa_slug",
    "medicamento_slug",
    "source_url",
    "patient_url",
    "crawled_at",
]


def _init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(_DDL)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    return conn


def _save(conn: sqlite3.Connection, record: BulaRecord) -> None:
    from dataclasses import asdict

    conn.execute(_UPSERT, asdict(record))


# ---------------------------------------------------------------------------
# Crawler principal
# ---------------------------------------------------------------------------
def crawl() -> list[BulaRecord]:
    session = requests.Session()
    session.headers.update(HEADERS)
    conn = _init_db(OUTPUT_DB)
    csv_file = None
    writer = None

    if SAVE_CSV:
        csv_file = OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig")
        writer = csv.DictWriter(
            csv_file, fieldnames=CSV_FIELDS, extrasaction="ignore"
        )
        writer.writeheader()

    total_saved = 0
    total_skipped = 0
    total_errors = 0

    try:
        for letter in LETTERS:
            log.info("=== Letra %s ===", letter)

            try:
                links = _collect_links_for_letter(session, letter)
            except Exception as exc:
                log.error("Erro ao coletar índice para letra %s: %s", letter, exc)
                continue

            log.info("  %d bulas encontradas para letra %s", len(links), letter)

            for i, link in enumerate(links, 1):
                # for i, link in enumerate(links[:3], 1):
                log.info("  [%s %d/%d] %s", letter, i, len(links), link["url"])

                try:
                    record = _scrape_bula(session, link)
                    _save(conn, record)
                    conn.commit()

                    if writer:
                        from dataclasses import asdict

                        writer.writerow(asdict(record))

                    total_saved += 1

                except Exception as exc:
                    log.error("    ERRO em %s: %s", link["url"], exc)
                    total_errors += 1

                time.sleep(SLEEP_BETWEEN_BULAS)

            time.sleep(SLEEP_BETWEEN_PAGES)

    finally:
        n = conn.execute("SELECT COUNT(*) FROM bulas").fetchone()[0]
        log.info("SQLite: %d registros em %s", n, OUTPUT_DB)
        conn.close()
        if csv_file:
            csv_file.close()

    log.info("Concluído — salvos: %d  erros: %d", total_saved, total_errors)
    return []


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Iniciando crawler bula.gratis — %d letras", len(LETTERS))
    crawl()
    log.info("Pronto! DB: %s  Log: %s", OUTPUT_DB, LOG_FILE)
