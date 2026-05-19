"""
sliding_window_crawler.py
--------------------------
Regenera chunks do banco BulaGratis usando janela deslizante com sobreposição,
SEM precisar percorrer o site novamente.

Estratégia de chunking:
    Dado o texto de cada seção, gera chunks de `chunk_size` palavras com
    sobreposição de `overlap` palavras entre chunks consecutivos.

    Exemplo (chunk_size=300, overlap=100):
        Chunk 0: palavras   0 – 300
        Chunk 1: palavras 200 – 500
        Chunk 2: palavras 400 – 700
        ...

Os chunks são indexados como paragraph_idx=0 (seção inteira tratada como um
único "parágrafo") e chunk_idx=0,1,2,… dentro da seção.

Uso:
    python -m bula_check.sliding_window_crawler \\
        --source bulas_gratis.db \\
        --dest   bulas_gratis_sliding.db \\
        --chunk-size 250 \\
        --overlap    100 \\
        --test-medicine "Dipirona"  # opcional: testa apenas 1 remédio

Dependências:
    pip install openai python-dotenv
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import uuid
from pathlib import Path

from dotenv import load_dotenv

from bula_check.protocol import DEFAULT_DB_PATH
from bula_check.protocol import OPENAI_EMBEDDING_DIM
from bula_check.protocol import Chunks
from bula_check.protocol import Medicines
from bula_check.protocol import Section
from bula_check.protocol import init_db
from bula_check.protocol import save_chunk
from bula_check.protocol import save_medicine

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"
EMBED_BATCH_SIZE = 100

LOG_FILE = Path("sliding_window_crawler.log")

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
# Leitura do banco fonte
# ---------------------------------------------------------------------------
def _load_medicines(conn: sqlite3.Connection) -> list[dict]:
    """Carrega todos os medicamentos do banco fonte."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, name, processed_name, active_ingredient,
               processed_active_ingredient, source, url,
               registration_number, therapeutic_classes,
               company_name, processed_company_name, cnpj
        FROM medicines
        ORDER BY name
        """
    ).fetchall()
    return [dict(r) for r in rows]


def _load_sections_for_medicine(
    conn: sqlite3.Connection,
    medicine_id: str,
) -> dict[str, str]:
    """
    Reconstrói o texto de cada seção a partir dos chunks existentes.

    Os chunks de um medicamento são ordenados por (section, paragraph_idx,
    chunk_idx) e seus textos são concatenados para recriar o texto original
    de cada seção.
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT section, paragraph_idx, chunk_idx, text
        FROM chunks
        WHERE medicine_id = ?
        ORDER BY section, paragraph_idx, chunk_idx
        """,
        (medicine_id,),
    ).fetchall()

    sections: dict[str, list[str]] = {}
    for row in rows:
        sec = row["section"]
        sections.setdefault(sec, []).append(row["text"])

    # Une os fragmentos de cada seção com espaço
    return {sec: " ".join(parts) for sec, parts in sections.items()}


# ---------------------------------------------------------------------------
# Sliding-window chunking
# ---------------------------------------------------------------------------
def _sliding_window_chunks(
    text: str,
    chunk_size: int = 300,
    overlap: int = 100,
) -> list[str]:
    """
    Divide `text` em chunks de `chunk_size` palavras com `overlap` palavras
    de sobreposição entre chunks consecutivos.

    Parâmetros
    ----------
    text : str
        Texto completo da seção.
    chunk_size : int
        Número máximo de palavras por chunk.
    overlap : int
        Número de palavras compartilhadas entre chunks consecutivos.
        Deve ser menor que chunk_size.

    Retorna
    -------
    list[str]
        Lista de chunks de texto.

    Exemplo (chunk_size=300, overlap=100, step=200):
        Chunk 0: palavras   0 – 299
        Chunk 1: palavras 200 – 499
        Chunk 2: palavras 400 – 699
    """
    if overlap >= chunk_size:
        raise ValueError(
            f"overlap ({overlap}) deve ser menor que chunk_size ({chunk_size})"
        )

    words = text.split()
    if not words:
        return []

    step = chunk_size - overlap
    chunks: list[str] = []

    start = 0
    while start < len(words):
        end = start + chunk_size
        chunk_words = words[start:end]
        chunks.append(" ".join(chunk_words))
        if end >= len(words):
            break
        start += step

    return chunks


def _build_sliding_chunks(
    medicine_id: str,
    medicine_name: str,
    sections: dict[str, str],
    chunk_size: int,
    overlap: int,
) -> list[Chunks]:
    """
    Para cada seção, gera chunks com janela deslizante.

    paragraph_idx é sempre 0 (a seção inteira é tratada como um único bloco).
    chunk_idx enumera os chunks dentro da seção (0, 1, 2, …).
    """
    dummy_embedding: list[float] = [0.0] * OPENAI_EMBEDDING_DIM
    result: list[Chunks] = []

    for section_name, text in sections.items():
        if not text or not text.strip():
            continue

        try:
            section_enum = Section(section_name)
        except ValueError:
            log.warning("Seção desconhecida ignorada: %r", section_name)
            continue

        window_chunks = _sliding_window_chunks(text, chunk_size, overlap)

        for chunk_idx, chunk_text in enumerate(window_chunks):
            result.append(
                Chunks(
                    id=str(uuid.uuid4()),
                    medicine_id=medicine_id,
                    medicine_name=medicine_name,
                    section=section_enum,
                    paragraph_idx=0,  # seção inteira = parágrafo único
                    chunk_idx=chunk_idx,
                    text=chunk_text,
                    embedding=dummy_embedding,
                )
            )

    return result


# ---------------------------------------------------------------------------
# Embeddings via OpenAI
# ---------------------------------------------------------------------------
def _embed_chunks(chunks: list[Chunks]) -> list[Chunks]:
    """Gera embeddings para os chunks via OpenAI (batches de EMBED_BATCH_SIZE)."""
    if not chunks:
        return chunks

    load_dotenv()
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        log.warning("OPENAI_API_KEY não encontrada — embeddings não gerados.")
        return chunks

    try:
        from openai import OpenAI  # type: ignore
    except ImportError:
        log.warning(
            "openai não instalado — embeddings não gerados. pip install openai"
        )
        return chunks

    client = OpenAI(api_key=api_key)

    for i in range(0, len(chunks), EMBED_BATCH_SIZE):
        batch = chunks[i : i + EMBED_BATCH_SIZE]
        texts = [c.text for c in batch]
        try:
            response = client.embeddings.create(
                model=OPENAI_EMBEDDING_MODEL,
                input=texts,
            )
            for j, emb_data in enumerate(response.data):
                chunks[i + j] = batch[j].model_copy(
                    update={"embedding": emb_data.embedding}
                )
        except Exception as exc:
            log.error(
                "Erro ao gerar embeddings (batch %d): %s", i // EMBED_BATCH_SIZE, exc
            )

    return chunks


# ---------------------------------------------------------------------------
# Gerador principal
# ---------------------------------------------------------------------------
def regenerate_chunks(
    source_db: Path,
    dest_db: Path,
    chunk_size: int = 300,
    overlap: int = 100,
    embed: bool = True,
    test_medicine: str | None = None,
) -> None:
    """
    Lê medicamentos do `source_db`, gera novos chunks com janela deslizante
    e salva no `dest_db`.

    Parâmetros
    ----------
    source_db : Path
        Banco SQLite de origem (gerado pelo bula_gratis_crawler).
    dest_db : Path
        Banco SQLite de destino (será criado ou sobrescrito).
    chunk_size : int
        Número de palavras por chunk.
    overlap : int
        Número de palavras de sobreposição entre chunks consecutivos.
    embed : bool
        Se True, gera embeddings via OpenAI para cada chunk.
    test_medicine : str | None
        Se fornecido, processa apenas o medicamento cujo nome contenha essa
        string (case-insensitive). Útil para teste rápido.
    """
    if not source_db.exists():
        raise FileNotFoundError(f"Banco fonte não encontrado: {source_db}")

    log.info(
        "Iniciando regeneração de chunks | source=%s | dest=%s | "
        "chunk_size=%d | overlap=%d | embed=%s",
        source_db,
        dest_db,
        chunk_size,
        overlap,
        embed,
    )

    src_conn = sqlite3.connect(str(source_db))
    dest_conn = init_db(dest_db)

    try:
        medicines = _load_medicines(src_conn)

        if test_medicine:
            test_norm = test_medicine.lower()
            medicines = [m for m in medicines if test_norm in m["name"].lower()]
            log.info(
                "Modo teste: %d medicamento(s) encontrado(s) para %r",
                len(medicines),
                test_medicine,
            )
            if not medicines:
                log.warning("Nenhum medicamento encontrado para %r", test_medicine)
                return

        total_chunks = 0

        for idx, med_row in enumerate(medicines, 1):
            medicine_id = med_row["id"]
            medicine_name = med_row["name"]

            log.info(
                "[%d/%d] Processando: %s",
                idx,
                len(medicines),
                medicine_name,
            )

            # --- Copia o medicamento para o banco de destino ---
            medicine = Medicines(
                id=medicine_id,
                name=medicine_name,
                processed_name=med_row["processed_name"],
                active_ingredient=(
                    [med_row["active_ingredient"]]
                    if med_row.get("active_ingredient")
                    else None
                ),
                processed_active_ingredient=(
                    [med_row["processed_active_ingredient"]]
                    if med_row.get("processed_active_ingredient")
                    else None
                ),
                source=med_row["source"],  # type: ignore[arg-type]
                url=med_row["url"],
                registration_number=med_row.get("registration_number"),
                therapeutic_classes=med_row.get("therapeutic_classes"),
                company_name=med_row["company_name"],
                processed_company_name=med_row["processed_company_name"],
                cnpj=med_row.get("cnpj"),
            )
            save_medicine(dest_conn, medicine)

            # --- Reconstrói o texto das seções ---
            sections = _load_sections_for_medicine(src_conn, medicine_id)

            if not sections:
                log.warning("  Sem seções para %s — pulando.", medicine_name)
                continue

            # --- Gera os novos chunks com sliding window ---
            chunks = _build_sliding_chunks(
                medicine_id=medicine_id,
                medicine_name=medicine_name,
                sections=sections,
                chunk_size=chunk_size,
                overlap=overlap,
            )

            log.info(
                "  %d seções → %d chunks (chunk_size=%d, overlap=%d)",
                len(sections),
                len(chunks),
                chunk_size,
                overlap,
            )

            # --- Embeddings ---
            if embed:
                chunks = _embed_chunks(chunks)

            # --- Persiste no banco de destino ---
            for chunk in chunks:
                save_chunk(dest_conn, chunk)
            dest_conn.commit()

            total_chunks += len(chunks)

        # --- Sumário final ---
        n_med = dest_conn.execute("SELECT COUNT(*) FROM medicines").fetchone()[0]
        n_chk = dest_conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        log.info(
            "Concluído! %d medicamentos | %d chunks → %s",
            n_med,
            n_chk,
            dest_db,
        )

    finally:
        src_conn.close()
        dest_conn.close()


# ---------------------------------------------------------------------------
# Entry point CLI
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Regenera chunks do BulaGratis com janela deslizante."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Banco SQLite de origem (default: %(default)s)",
    )
    parser.add_argument(
        "--dest",
        type=Path,
        default=Path("bulas_gratis_sliding.db"),
        help="Banco SQLite de destino (default: %(default)s)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=300,
        help="Número de palavras por chunk (default: %(default)s)",
    )
    parser.add_argument(
        "--overlap",
        type=int,
        default=100,
        help="Sobreposição em palavras entre chunks consecutivos (default: %(default)s)",
    )
    parser.add_argument(
        "--no-embed",
        action="store_true",
        help="Não gera embeddings (útil para testes rápidos de estrutura)",
    )
    parser.add_argument(
        "--test-medicine",
        type=str,
        default=None,
        metavar="NAME",
        help="Processa apenas o medicamento cujo nome contenha NAME (ex: 'Dipirona')",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    regenerate_chunks(
        source_db=args.source,
        dest_db=args.dest,
        chunk_size=args.chunk_size,
        overlap=args.overlap,
        embed=not args.no_embed,
        test_medicine=args.test_medicine,
    )

    """    
    python -m bula_check.sliding_window_crawler \
    --source bulas_gratis.db \
    --dest   bulas_gratis_sliding.db \
    --chunk-size 250 \
    --overlap    100 \
    --test-medicine "Dipirona"
    --no-embed

    python -m bula_check.sliding_window_crawler --source bulas_gratis.db --dest bulas_gratis_sliding.db --chunk-size 250 --overlap 100
    """
