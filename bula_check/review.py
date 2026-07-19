"""Gera uma planilha CSV de revisão do dataset de avaliação para validação por
farmacêutico.

O dataset (inputs/evaluation/dataset.json) permanece a fonte da verdade e
enxuto: guarda apenas os rótulos e os `expected_chunk_ids`. Este módulo faz o
JOIN com o banco (chunks + medicines) e produz um CSV legível — com o texto de
cada chunk, a seção em português e o link da bula — mais colunas em branco para
o parecer do farmacêutico. O texto da bula NÃO é copiado para o dataset (evita
defasagem quando o banco é recrawleado); ele é regenerado a partir do banco a
cada execução.

Uso:
    # 1) gera o CSV para o farmacêutico revisar
    python -m bula_check.review build \\
        --dataset inputs/evaluation/dataset.json \\
        --db bulas_gratis.db \\
        --out outputs/review/dataset_review.csv

    # 2) importa o CSV revisado de volta para o bloco `validation` do dataset
    python -m bula_check.review ingest \\
        --csv outputs/review/dataset_review.csv \\
        --dataset inputs/evaluation/dataset.json
"""

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

from bula_check.agents.nodes import _open_db
from bula_check.protocol import pt_section_label

# Colunas derivadas do dataset/banco, preenchidas pelo gerador.
DATA_COLUMNS = [
    "id",
    "query",
    "medicine_brand",
    "expected_medicine",
    "medicine_name_db",
    "expected_sections_pt",
    "expected_verdict",
    "justification",
    "evidence",
    "bula_url",
    "stress_category",
]

# Colunas em branco para o farmacêutico preencher na revisão. `status`
# (aprovado/reprovado) NÃO fica aqui: é derivado dos *_ok no ingest.
VALIDATION_COLUMNS = [
    "verdict_ok",
    "medicine_ok",
    "evidence_ok",
    "validated_by",
    "comments",
]

REVIEW_COLUMNS = DATA_COLUMNS + VALIDATION_COLUMNS

# Rótulos em português exibidos no CSV (o farmacêutico revisa em PT). O código
# usa as chaves internas; a tradução acontece só na borda (build escreve com os
# rótulos PT, ingest lê e remapeia de volta para as chaves internas).
COLUMN_LABELS_PT = {
    "id": "id",
    "query": "Pergunta",
    "medicine_brand": "Marca",
    "expected_medicine": "Medicamento (esperado)",
    "medicine_name_db": "Medicamento (no banco)",
    "expected_sections_pt": "Seções esperadas",
    "expected_verdict": "Veredito esperado",
    "justification": "Justificativa",
    "evidence": "Evidência (texto da bula)",
    "bula_url": "Link da bula",
    "stress_category": "Tipo de desafio",
    "verdict_ok": "Veredito correto? (sim/não)",
    "medicine_ok": "Medicamento correto? (sim/não)",
    "evidence_ok": "Evidência correta? (sim/não)",
    "validated_by": "Validado por",
    "comments": "Comentários",
}
LABEL_TO_KEY = {label: key for key, label in COLUMN_LABELS_PT.items()}


def _fetch_chunk_rows(
    conn: sqlite3.Connection,
    ids: list[str],
) -> dict[str, dict]:
    """Busca chunks por id (sem o embedding, desnecessário aqui).

    Returns
    -------
    dict[str, dict]
        Mapeia chunk_id -> linha com id, medicine_id, medicine_name, section,
        text. IDs ausentes no banco simplesmente não aparecem no dicionário.
    """
    if not ids:
        return {}

    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in ids)
    cursor = conn.execute(
        f"""
        SELECT id, medicine_id, medicine_name, section, text
        FROM chunks
        WHERE id IN ({placeholders})
        """,
        ids,
    )
    return {row["id"]: dict(row) for row in cursor.fetchall()}


def _fetch_medicine_row(
    conn: sqlite3.Connection,
    medicine_id: str,
) -> dict:
    """Busca a url da bula de um medicamento por id."""
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        "SELECT url FROM medicines WHERE id = ?",
        (medicine_id,),
    )
    row = cursor.fetchone()
    return dict(row) if row else {}


def _format_evidence(chunks: list[dict]) -> str:
    """Monta a célula de evidência: cada chunk prefixado com [seção | id]."""
    blocks = []
    for chunk in chunks:
        header = f"[{pt_section_label(chunk['section'])} | {chunk['id']}]"
        blocks.append(f"{header}\n{chunk['text']}")
    return "\n\n".join(blocks)


def build_review_rows(
    dataset: list[dict],
    conn: sqlite3.Connection,
) -> tuple[list[dict], list[str]]:
    """Constrói as linhas do CSV (uma por questão) e coleta ids ausentes.

    Returns
    -------
    rows : list[dict]
        Linhas com as chaves de REVIEW_COLUMNS; colunas de validação vazias.
    missing : list[str]
        Ocorrências "questão: chunk_id" referenciadas mas ausentes no banco.
    """
    rows: list[dict] = []
    missing: list[str] = []

    for item in dataset:
        chunk_ids = item.get("expected_chunk_ids", [])
        found = _fetch_chunk_rows(conn, chunk_ids)

        # Preserva a ordem do dataset e registra os ids não encontrados.
        ordered = []
        for chunk_id in chunk_ids:
            if chunk_id in found:
                ordered.append(found[chunk_id])
            else:
                missing.append(f"{item.get('id', '?')}: {chunk_id}")

        medicine_name_db = ordered[0]["medicine_name"] if ordered else ""
        medicine_row = (
            _fetch_medicine_row(conn, ordered[0]["medicine_id"]) if ordered else {}
        )

        sections_pt = ", ".join(
            pt_section_label(s) for s in item.get("expected_sections", [])
        )

        row = {
            "id": item.get("id", ""),
            "query": item.get("query", ""),
            "medicine_brand": item.get("medicine_brand") or "",
            "expected_medicine": item.get("expected_medicine", ""),
            "medicine_name_db": medicine_name_db,
            "expected_sections_pt": sections_pt,
            "expected_verdict": item.get("expected_verdict", ""),
            "justification": item.get("justification") or "",
            "evidence": _format_evidence(ordered),
            "bula_url": medicine_row.get("url", ""),
            "stress_category": item.get("stress_category") or "",
        }
        for col in VALIDATION_COLUMNS:
            row[col] = ""
        rows.append(row)

    return rows, missing


def build_review_csv(
    dataset_path: Path,
    db_path: Path,
    out_path: Path,
) -> Path:
    """Lê o dataset, faz o JOIN com o banco e escreve o CSV de revisão."""
    dataset = json.loads(Path(dataset_path).read_text(encoding="utf-8"))

    conn = _open_db(Path(db_path))
    try:
        rows, missing = build_review_rows(dataset, conn)
    finally:
        conn.close()

    if missing:
        print(
            f"[review] AVISO: {len(missing)} chunk_id(s) ausentes no banco "
            "(gabarito vazio -> recall=0 nessas questões):",
            file=sys.stderr,
        )
        for entry in missing:
            print(f"  - {entry}", file=sys.stderr)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig: BOM para o Excel abrir os acentos corretamente. Cabeçalho em
    # português; corpo na ordem de REVIEW_COLUMNS (chaves internas).
    with out_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([COLUMN_LABELS_PT[k] for k in REVIEW_COLUMNS])
        for row in rows:
            writer.writerow([row.get(k, "") for k in REVIEW_COLUMNS])

    print(f"[review] {len(rows)} linha(s) escrita(s) em {out_path}")
    return out_path


# Tokens aceitos nas colunas *_ok preenchidas pelo farmacêutico.
_NEGATIVE_TOKENS = {
    "nao",
    "não",
    "n",
    "no",
    "false",
    "0",
    "incorreto",
    "errado",
    "reprovado",
    "rejected",
    "reject",
}
_POSITIVE_TOKENS = {
    "sim",
    "s",
    "yes",
    "y",
    "true",
    "1",
    "ok",
    "correto",
    "aprovado",
    "approved",
}
def _norm(value: str | None) -> str:
    return (value or "").strip().lower()


def _flag_state(value: str | None) -> bool | None:
    """Interpreta uma célula *_ok: True=ok, False=problema, None=vazio/ambíguo."""
    token = _norm(value)
    if token in _NEGATIVE_TOKENS:
        return False
    if token in _POSITIVE_TOKENS:
        return True
    return None


def ingest_review_csv(
    csv_path: Path,
    dataset_path: Path,
    out_path: Path | None = None,
) -> dict:
    """Importa o CSV revisado pelo farmacêutico de volta para o dataset.

    Grava o bloco `validation` em cada questão correspondente do dataset e
    reporta as divergências — questões cujo verdict_ok/medicine_ok/evidence_ok
    foi marcado como incorreto. O `status` (aprovado/reprovado) é DERIVADO desses
    campos, não preenchido à mão. Só grava o bloco nas questões em que o
    farmacêutico preencheu algo; o casamento é por `id`.

    Parameters
    ----------
    csv_path : Path
        CSV gerado por build_review_csv e preenchido pelo farmacêutico.
    dataset_path : Path
        Dataset JSON (fonte da verdade) a ser lido/atualizado.
    out_path : Path | None
        Destino do dataset atualizado. None sobrescreve dataset_path.

    Returns
    -------
    dict
        Resumo com total, validated, flagged (divergências) e unknown_ids
        (ids presentes no CSV mas ausentes do dataset).
    """
    with Path(csv_path).open(encoding="utf-8-sig", newline="") as fh:
        review_rows = {}
        for raw in csv.DictReader(fh):
            # Remapeia os rótulos PT do cabeçalho para as chaves internas.
            row = {LABEL_TO_KEY.get(k, k): v for k, v in raw.items()}
            review_rows[row["id"]] = row

    dataset = json.loads(Path(dataset_path).read_text(encoding="utf-8"))
    dataset_ids = {item.get("id") for item in dataset}

    validated = 0
    flagged: list[dict] = []

    for item in dataset:
        row = review_rows.get(item.get("id"))
        if row is None:
            continue

        block = {col: _norm_keep(row.get(col)) for col in VALIDATION_COLUMNS}
        if not any(block.values()):  # farmacêutico não preencheu nada
            continue

        problems = [
            field
            for field in ("verdict_ok", "medicine_ok", "evidence_ok")
            if _flag_state(block[field]) is False
        ]
        # status é DERIVADO dos *_ok (não preenchido à mão): reprovado se algum
        # aspecto foi marcado como incorreto, senão aprovado.
        block["status"] = "reprovado" if problems else "aprovado"

        item["validation"] = block
        validated += 1

        if problems:
            flagged.append(
                {
                    "id": item.get("id"),
                    "problems": problems,
                    "comments": block["comments"],
                }
            )

    unknown_ids = sorted(set(review_rows) - dataset_ids)

    target = Path(out_path) if out_path else Path(dataset_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(dataset, ensure_ascii=False, indent=4) + "\n",
        encoding="utf-8",
    )

    print(
        f"[ingest] {validated}/{len(dataset)} questão(ões) com parecer gravado "
        f"em {target}"
    )
    if unknown_ids:
        print(
            f"[ingest] AVISO: {len(unknown_ids)} id(s) do CSV fora do dataset: "
            f"{', '.join(unknown_ids)}",
            file=sys.stderr,
        )
    if flagged:
        print(f"[ingest] {len(flagged)} divergência(s) marcada(s):")
        for entry in flagged:
            fields = ", ".join(entry["problems"]) or "—"
            note = f" | {entry['comments']}" if entry["comments"] else ""
            print(f"  - {entry['id']}: {fields}{note}")
    else:
        print("[ingest] Nenhuma divergência marcada.")

    return {
        "total": len(dataset),
        "validated": validated,
        "flagged": flagged,
        "unknown_ids": unknown_ids,
    }


def _norm_keep(value: str | None) -> str:
    """Como _norm, mas preserva o texto original (só faz strip) para gravação."""
    return (value or "").strip()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera/importa a revisão do dataset de avaliação (farmacêutico)."
        )
    )
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser(
        "build", help="Gera o CSV de revisão a partir do dataset + banco."
    )
    build.add_argument(
        "--dataset",
        type=Path,
        default=Path("inputs/evaluation/dataset.json"),
        help="Caminho do dataset JSON (fonte da verdade).",
    )
    build.add_argument(
        "--db",
        type=Path,
        default=Path("bulas_gratis.db"),
        help="Banco de onde vêm os textos dos chunks e as urls das bulas.",
    )
    build.add_argument(
        "--out",
        type=Path,
        default=Path("outputs/review/dataset_review.csv"),
        help="Caminho de saída do CSV.",
    )

    ingest = sub.add_parser(
        "ingest", help="Importa o CSV revisado de volta para o dataset."
    )
    ingest.add_argument(
        "--csv",
        type=Path,
        required=True,
        help="CSV preenchido pelo farmacêutico.",
    )
    ingest.add_argument(
        "--dataset",
        type=Path,
        default=Path("inputs/evaluation/dataset.json"),
        help="Dataset JSON a ser atualizado.",
    )
    ingest.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Destino do dataset atualizado (default: sobrescreve --dataset).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.command == "build":
        build_review_csv(args.dataset, args.db, args.out)
    elif args.command == "ingest":
        ingest_review_csv(args.csv, args.dataset, args.out)


# python -m bula_check.review build \
#     --dataset inputs/evaluation/dataset.json \
#     --db bulas_gratis.db \
#     --out outputs/review/dataset_review.csv


# sliding
# python -m bula_check.review build \
#     --dataset inputs/evaluation/dataset_sliding.json \
#     --db bulas_gratis_sliding.db \
#     --out outputs/review/dataset_review_sliding.csv

# depois que validado
# venv/bin/python -m bula_check.review ingest \
#     --csv outputs/review/dataset_review.csv \
#     --dataset inputs/evaluation/dataset.json
