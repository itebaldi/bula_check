import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

from bula_check.agents.nodes import _open_db
from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.agents.search import find_medicine_candidates
from bula_check.protocol import Chunks
from bula_check.protocol import Medicines

WRITE_PDF = True


# Metoprolol — 25 mg / 1 dia
# Olmesartana — 40 mg / 1 dia
# Plenance — 10 mg / dia
# Duomo HP — 1 comp / noite
# Naldecon
# Buscofem


def test_find_medicine_candidates():
    config = DEFAULT_CONFIG
    bulagratis_conn = _open_db(config["bulagratis_db_path"])
    anvisa_conn = _open_db(config["anvisa_db_path"])

    try:
        candidates = find_medicine_candidates(
            bulagratis_conn=bulagratis_conn,
            anvisa_conn=anvisa_conn,
            name="Kaloba",
            active_ingredient=None,  # "paracetamol",
            cfg=config,
        )
    finally:
        bulagratis_conn.close()
        anvisa_conn.close()

    if WRITE_PDF:
        for candidate in candidates:
            _gerar_pdf_json(
                candidate["medicine"]["id"],
                save_jsons=True,
                save_pdf_json=True,
                db_path=config["bulagratis_db_path"],
            )


def _gerar_pdf_json(
    medicine_id: str,
    save_jsons: bool = False,
    save_pdf_json: bool = False,
    db_path: Path = Path("bulas_gratis.db"),
    json_output_dir: Path = Path("outputs/bula_gratis/json"),
    pdf_json_output_dir: Path = Path("outputs/bula_gratis/pdf_json"),
) -> tuple[Medicines, list[Chunks]] | None:
    """
    Lê um medicamento por ID no DB do bula_gratis e exporta JSONs no mesmo
    formato que `get_by_name` produz — sem re-crawl.

    Útil para gerar fixtures de teste a partir do DB já populado.
    """
    conn = sqlite3.connect(db_path)
    try:
        medicine = _fetch_medicine_by_id(conn, medicine_id)
        if medicine is None:
            return None
        chunks = _fetch_chunks_for_medicine(conn, medicine.id)
        sections = _reconstruct_sections(chunks)
    finally:
        conn.close()

    safe_name = re.sub(r"\W+", "_", medicine.name).strip("_").lower()
    filename = f"bula_gratis_{safe_name}_{medicine.id[:8]}.json"

    if save_jsons:
        json_output_dir.mkdir(parents=True, exist_ok=True)
        out = json_output_dir / filename
        payload = [
            {
                "medicine": medicine.model_dump(),
                "chunks": [c.model_dump() for c in chunks],
            }
        ]
        out.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if save_pdf_json:
        pdf_json_output_dir.mkdir(parents=True, exist_ok=True)
        out = pdf_json_output_dir / filename
        payload_readable = [
            {
                "name": medicine.name,
                "active_ingredient": medicine.active_ingredient,
                "company_name": medicine.company_name,
                "cnpj": medicine.cnpj,
                "registration_number": medicine.registration_number,
                "therapeutic_classes": medicine.therapeutic_classes,
                "url": medicine.url,
                "sections": {k: v for k, v in sections.items() if v},
            }
        ]
        out.write_text(
            json.dumps(payload_readable, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return medicine, chunks


def _fetch_medicine_by_id(
    conn: sqlite3.Connection,
    medicine_id: str,
) -> Medicines | None:
    """Busca uma medicine pelo ID (PK). Retorna None se não existir."""
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, processed_name, active_ingredient,
               processed_active_ingredient, source, url, registration_number,
               therapeutic_classes, company_name, processed_company_name, cnpj
        FROM medicines
        WHERE id = ?
        """,
        (medicine_id,),
    )
    row = cursor.fetchone()
    return _row_to_medicine(row) if row else None


def _fetch_chunks_for_medicine(
    conn: sqlite3.Connection,
    medicine_id: str,
) -> list[Chunks]:
    """Busca todos os chunks de um medicamento, ordenados por seção/posição."""
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, medicine_id, medicine_name, section,
               paragraph_idx, chunk_idx, text, embedding
        FROM chunks
        WHERE medicine_id = ?
        ORDER BY section, paragraph_idx, chunk_idx
        """,
        (medicine_id,),
    )
    return [_row_to_chunk(row) for row in cursor.fetchall()]


def _row_to_medicine(row: sqlite3.Row) -> Medicines:
    data = dict(row)
    data["active_ingredient"] = _split_csv(data.get("active_ingredient"))
    data["processed_active_ingredient"] = _split_csv(
        data.get("processed_active_ingredient")
    )
    return Medicines(**data)


def _row_to_chunk(row: sqlite3.Row) -> Chunks:
    data = dict(row)
    emb = data["embedding"]
    if isinstance(emb, (str, bytes, bytearray)):
        data["embedding"] = json.loads(emb)
    return Chunks(**data)


def _split_csv(value: str | None) -> list[str] | None:
    """active_ingredient é salvo como ', '-joined string (ver protocol.save_medicine)."""
    if not value:
        return None
    return [token.strip() for token in value.split(",") if token.strip()]


def _reconstruct_sections(chunks: list[Chunks]) -> dict[str, str]:
    """
    Reagrupa chunks por (section, paragraph_idx, chunk_idx) e reconstrói o
    texto da seção. Aproxima o que `get_by_name` salva — pode diferir em
    whitespace por causa do clean feito no chunking.
    """
    grouped: dict[str, dict[int, list[tuple[int, str]]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for chunk in chunks:
        section_value = chunk.section.value
        grouped[section_value][chunk.paragraph_idx].append(
            (chunk.chunk_idx, chunk.text)
        )

    sections: dict[str, str] = {}
    for section_value, para_dict in grouped.items():
        paragraphs: list[str] = []
        for para_idx in sorted(para_dict.keys()):
            chunk_list = sorted(para_dict[para_idx], key=lambda x: x[0])
            paragraphs.append(" ".join(text for _, text in chunk_list))
        sections[section_value] = " ".join(paragraphs)

    return sections
