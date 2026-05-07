"""
protocol.py
-----------
Protocolo central do projeto: modelos de dados, configuração do banco SQLite
e utilitários de normalização de texto.

Tabelas:
  - medicines : uma linha por medicamento (Medicines)
  - chunks    : fragmentos de texto com embeddings (Chunks)
"""

from __future__ import annotations

import re
import sqlite3
import unicodedata
from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuração global
# ---------------------------------------------------------------------------
DEFAULT_DB_PATH = Path("bulas.db")
OPENAI_EMBEDDING_DIM = 1536  # text-embedding-3-small


# ---------------------------------------------------------------------------
# Enumerações e modelos
# ---------------------------------------------------------------------------
class Section(str, Enum):
    indications = "indications"
    how_it_works = "how_it_works"
    contraindications = "contraindications"
    warnings_and_precautions = "warnings_and_precautions"
    storage = "storage"
    dosage_and_administration = "dosage_and_administration"
    missed_dose = "missed_dose"
    adverse_reactions = "adverse_reactions"
    overdose = "overdose"


# ---------------------------------------------------------------------------
# Padrões de seções das bulas (texto normalizado)
# ---------------------------------------------------------------------------
RAW_SECTION_PATTERNS: dict[str, list[str]] = {
    "indications": [
        "para que este medicamento e indicado",
        "PARA QUE ESTE MEDICAMENTO E INDICADO",
    ],
    "how_it_works": [
        "como este medicamento funciona",
        "COMO ESTE MEDICAMENTO FUNCIONA",
    ],
    "contraindications": [
        "quando nao devo usar este medicamento",
        "quem nao deve usar este medicamento",
        "QUANDO NAO DEVO USAR ESTE MEDICAMENTO",
    ],
    "warnings_and_precautions": [
        "o que devo saber antes de usar este medicamento",
        "precaucoes",
        "O QUE DEVO SABER ANTES DE USAR ESTE MEDICAMENTO",
    ],
    "storage": [
        "onde como e por quanto tempo posso guardar este medicamento",
        "ONDE, COMO E POR QUANTO TEMPO POSSO GUARDAR ESTE MEDICAMENTO",
    ],
    "dosage_and_administration": [
        "como devo usar este medicamento",
        "COMO DEVO USAR ESTE MEDICAMENTO",
    ],
    "missed_dose": [
        "o que devo fazer quando eu me esquecer de usar este medicamento",
        "O QUE DEVO FAZER QUANDO EU ME ESQUECER DE USAR ESTE MEDICAMENTO",
    ],
    "adverse_reactions": [
        "quais os males que este medicamento pode me causar",
        "QUAIS OS MALES QUE ESTE MEDICAMENTO PODE ME CAUSAR",
    ],
    "overdose": [
        "o que fazer se alguem usar uma quantidade maior do que a indicada deste medicamento",
        "O QUE FAZER SE ALGUEM USAR UMA QUANTIDADE MAIOR DO QUE A INDICADA DESTE MEDICAMENTO",
    ],
}


class Medicines(BaseModel):
    """Representa um medicamento no banco.

    Attributes
    ----------
    id : str
        UUID único do medicamento.
    name : str
        Nome original.
    processed_name : str
        Nome normalizado (uppercase, sem acentos, sem pontuação).
    active_ingredient : list[str] | None
        Princípios ativos.
    processed_active_ingredient : list[str] | None
        Princípios ativos normalizados.
    source : Literal["anvisa", "bula_gratis"]
        Fonte dos dados.
    url : str
        URL de origem.
    registration_number : int | None
        Número de registro ANVISA.
    therapeutic_classes : list[str] | None
        Classes terapêuticas.
    company_name : str
        Nome da empresa.
    processed_company_name : str
        Nome da empresa normalizado.
    cnpj : str | None
        CNPJ da empresa.
    extras : str
        Informações adicionais em JSON.
    """

    id: str
    name: str
    processed_name: str
    active_ingredient: list[str] | None = None
    processed_active_ingredient: list[str] | None = None
    source: Literal["anvisa", "bula_gratis"]
    url: str
    registration_number: int | None = None
    therapeutic_classes: list[str] | None = None
    company_name: str
    processed_company_name: str
    cnpj: str | None = None
    extras: str = "{}"


class Chunks(BaseModel):
    """Representa um fragmento de texto de uma bula com embedding.

    Attributes
    ----------
    id : str
        UUID único do chunk.
    medicine_id : str
        UUID do medicamento ao qual pertence.
    medicine_name : str
        Nome do medicamento.
    section : Section
        Seção da bula.
    paragraph_idx : int
        Índice do parágrafo dentro da seção.
    chunk_idx : int
        Índice do chunk dentro do parágrafo.
    text : str
        Texto do chunk.
    embedding : list[float]
        Vetor de embedding (OpenAI text-embedding-3-small, dim=1536).
    """

    id: str
    medicine_id: str
    medicine_name: str
    section: Section
    paragraph_idx: int
    chunk_idx: int
    text: str
    embedding: list[float]


# ---------------------------------------------------------------------------
# Normalização de texto
# ---------------------------------------------------------------------------
def normalize_for_matching(text: str) -> str:
    """Lowercase + remove acentos + remove pontuação + colapsa espaços."""
    text = text.upper()
    text = _remove_accents(text)
    text = _remove_punctuation(text)
    text = _normalize_whitespace(text)
    return text.lower()


def normalize_processed_field(text: str) -> str:
    """Normalização para campos processed_*: uppercase sem acentos/pontuação."""
    text = text.upper()
    text = _remove_accents(text)
    text = _remove_punctuation(text)
    text = _normalize_whitespace(text)
    return text


def _remove_accents(text: str) -> str:
    nfd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def _remove_punctuation(text: str) -> str:
    return re.sub(r"[^\w\s]", " ", text)


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


# Versões normalizadas dos padrões de seção (lowercase, sem acentos)
SECTION_PATTERNS: dict[str, list[str]] = {
    section: [normalize_for_matching(p) for p in patterns]
    for section, patterns in RAW_SECTION_PATTERNS.items()
}


# ---------------------------------------------------------------------------
# DDL do banco SQLite
# ---------------------------------------------------------------------------
_DDL_MEDICINES = """
CREATE TABLE IF NOT EXISTS medicines (
    id                          TEXT PRIMARY KEY,
    name                        TEXT NOT NULL,
    processed_name              TEXT NOT NULL,
    active_ingredient           TEXT,          -- JSON array ou NULL
    processed_active_ingredient TEXT,          -- JSON array ou NULL
    source                      TEXT NOT NULL, -- 'anvisa' | 'bula_gratis'
    url                         TEXT NOT NULL UNIQUE,
    registration_number         INTEGER,
    therapeutic_classes         TEXT,          -- JSON array ou NULL
    company_name                TEXT NOT NULL,
    processed_company_name      TEXT NOT NULL,
    cnpj                        TEXT,
    extras                      TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_medicines_processed_name  ON medicines(processed_name);
CREATE INDEX IF NOT EXISTS idx_medicines_cnpj            ON medicines(cnpj);
CREATE INDEX IF NOT EXISTS idx_medicines_source          ON medicines(source);
CREATE INDEX IF NOT EXISTS idx_medicines_reg_number      ON medicines(registration_number);
"""

_DDL_CHUNKS = """
CREATE TABLE IF NOT EXISTS chunks (
    id             TEXT PRIMARY KEY,
    medicine_id    TEXT NOT NULL REFERENCES medicines(id),
    medicine_name  TEXT NOT NULL,
    section        TEXT NOT NULL,
    paragraph_idx  INTEGER NOT NULL,
    chunk_idx      INTEGER NOT NULL,
    text           TEXT NOT NULL,
    embedding      BLOB NOT NULL   -- float32 array serializado com json
);
CREATE INDEX IF NOT EXISTS idx_chunks_medicine_id ON chunks(medicine_id);
CREATE INDEX IF NOT EXISTS idx_chunks_section     ON chunks(section);
"""

_UPSERT_MEDICINE = """
INSERT INTO medicines (
    id, name, processed_name,
    active_ingredient, processed_active_ingredient,
    source, url, registration_number,
    therapeutic_classes, company_name, processed_company_name,
    cnpj, extras
) VALUES (
    :id, :name, :processed_name,
    :active_ingredient, :processed_active_ingredient,
    :source, :url, :registration_number,
    :therapeutic_classes, :company_name, :processed_company_name,
    :cnpj, :extras
)
ON CONFLICT(url) DO UPDATE SET
    name                        = excluded.name,
    processed_name              = excluded.processed_name,
    active_ingredient           = excluded.active_ingredient,
    processed_active_ingredient = excluded.processed_active_ingredient,
    registration_number         = excluded.registration_number,
    therapeutic_classes         = excluded.therapeutic_classes,
    company_name                = excluded.company_name,
    processed_company_name      = excluded.processed_company_name,
    cnpj                        = excluded.cnpj,
    extras                      = excluded.extras;
"""

_UPSERT_CHUNK = """
INSERT INTO chunks (
    id, medicine_id, medicine_name,
    section, paragraph_idx, chunk_idx, text, embedding
) VALUES (
    :id, :medicine_id, :medicine_name,
    :section, :paragraph_idx, :chunk_idx, :text, :embedding
)
ON CONFLICT(id) DO UPDATE SET
    text      = excluded.text,
    embedding = excluded.embedding;
"""


# ---------------------------------------------------------------------------
# API do banco
# ---------------------------------------------------------------------------
def init_db(path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Cria (ou abre) o banco e garante que as tabelas existam."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(_DDL_MEDICINES)
    conn.executescript(_DDL_CHUNKS)
    conn.commit()
    return conn


def save_medicine(conn: sqlite3.Connection, medicine: Medicines) -> None:
    """Insere ou atualiza um medicamento (upsert por URL)."""
    import json

    conn.execute(
        _UPSERT_MEDICINE,
        {
            "id": medicine.id,
            "name": medicine.name,
            "processed_name": medicine.processed_name,
            "active_ingredient": json.dumps(
                medicine.active_ingredient, ensure_ascii=False
            )
            if medicine.active_ingredient is not None
            else None,
            "processed_active_ingredient": json.dumps(
                medicine.processed_active_ingredient, ensure_ascii=False
            )
            if medicine.processed_active_ingredient is not None
            else None,
            "source": medicine.source,
            "url": medicine.url,
            "registration_number": medicine.registration_number,
            "therapeutic_classes": json.dumps(
                medicine.therapeutic_classes, ensure_ascii=False
            )
            if medicine.therapeutic_classes is not None
            else None,
            "company_name": medicine.company_name,
            "processed_company_name": medicine.processed_company_name,
            "cnpj": medicine.cnpj,
            "extras": medicine.extras,
        },
    )


def save_chunk(conn: sqlite3.Connection, chunk: Chunks) -> None:
    """Insere ou atualiza um chunk."""
    import json

    conn.execute(
        _UPSERT_CHUNK,
        {
            "id": chunk.id,
            "medicine_id": chunk.medicine_id,
            "medicine_name": chunk.medicine_name,
            "section": chunk.section.value,
            "paragraph_idx": chunk.paragraph_idx,
            "chunk_idx": chunk.chunk_idx,
            "text": chunk.text,
            "embedding": json.dumps(chunk.embedding),
        },
    )
