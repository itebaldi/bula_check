import json
import sqlite3
from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph
from pydantic import BaseModel

from bula_check.agents.nodes import _open_db
from bula_check.agents.pipeline import make_initial_state
from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import ChunksDict
from bula_check.agents.search import _cosine
from bula_check.agents.search import _parse_embedding
from bula_check.agents.search import normalize_text


class ExpectedResult(BaseModel):
    id: str
    query: str
    expected_medicine: str
    expected_sections: list[str]
    expected_verdict: str
    expected_chunk_ids: list[str]

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


def evaluate_results(
    config: BulaCheckConfig,
    graph: StateGraph,
    items: list[ExpectedResult],
    results_path: Path | None,
    semantic_threshold: float = 0.80,
):
    """
    Avalia o pipeline ponta-a-ponta.

    Métricas reportadas:
      - medicine_accuracy: medicamento previsto bate com o esperado (match normalizado)
      - section_accuracy: alguma seção esperada foi recuperada
      - verdict_accuracy: verdict do LLM bate com o esperado

      - semantic_recall: fração de chunks-gabarito cobertos por algum retrieved
        (cos >= threshold). Equivalente a Recall@k tradicional, mas robusto a IDs
        diferentes entre bulas equivalentes.
      - semantic_precision: fração de chunks retrieved que casam com algum gabarito.
      - semantic_f1: média harmônica de precision e recall.
      - semantic_mrr: 1/posição do primeiro retrieved que casa com algum gabarito.
      - semantic_hit_at_1: o top-1 retrieved casa com algum gabarito?
    """
    answer_rows: list[dict[str, Any]] = []
    retrieval_rows: list[dict[str, float]] = []

    bulagratis_conn = _open_db(config["bulagratis_db_path"])

    try:
        for item in items:
            state = make_initial_state(config)
            state["messages"].append(HumanMessage(content=item.query))

            final_state = graph.invoke(state)  # type: ignore

            selected_medicine = final_state.get("selected_medicine")
            retrieved_chunks = final_state.get("retrieved_chunks", [])
            verification_result = final_state.get("verification_result")
            parsed_query = final_state.get("parsed_query")

            # Fallback chain: retrieval (RAG) → parse (baseline). Mede
            # identificação de medicamento ponta-a-ponta, independente do modo.
            # active_ingredient cobre o caso brand → genérico (ex: Tylenol em
            # parse vs paracetamol esperado, ou Acetaminofeno no DB vs
            # paracetamol esperado).
            predicted_medicine = ""
            predicted_active_ingredient = ""
            if selected_medicine:
                predicted_medicine = selected_medicine["medicine"]["name"]
                ai_list = (
                    selected_medicine["medicine"].get("active_ingredient") or []
                )
                predicted_active_ingredient = " ".join(ai_list)
            elif parsed_query:
                predicted_medicine = parsed_query.get("medicine_name", "")
                predicted_active_ingredient = (
                    parsed_query.get("active_ingredient") or ""
                )

            predicted_verdict = ""
            if verification_result:
                predicted_verdict = verification_result["verdict"]

            # Para sections, no baseline cai no fallback do parse_query (que
            # mede entendimento da query, NÃO acerto do retrieval — semântica
            # ligeiramente diferente do RAG mode).
            if retrieved_chunks:
                predicted_sections = [
                    chunk["chunk"]["section"] for chunk in retrieved_chunks
                ]
            elif parsed_query:
                predicted_sections = parsed_query.get("sections", [])
            else:
                predicted_sections = []

            answer_rows.append(
                {
                    "medicine_correct": normalize_text(item.expected_medicine)
                    in normalize_text(predicted_medicine)
                    or normalize_text(item.expected_medicine)
                    in normalize_text(predicted_active_ingredient),
                    "section_correct": _has_expected_sections(
                        retrieved_sections=predicted_sections,
                        expected_sections=item.expected_sections,
                    ),
                    "verdict_correct": item.expected_verdict == predicted_verdict,
                }
            )

            gabarito_chunks = _fetch_chunks_by_ids(
                bulagratis_conn, list(item.expected_chunk_ids)
            )
            # Filtra chunks-vizinhos injetados pelo modo "with_prev_and_next"
            # (score=0.0 por convenção em search._expand_with_neighbours) e
            # recupera a ordem por score. Sem isso, o modo de retorno polui as
            # métricas de retrieval — que devem medir só o ranker, não a
            # estratégia de contexto. O efeito do contexto aparece em
            # verdict_accuracy.
            core_retrieved = sorted(
                (rc for rc in retrieved_chunks if rc["score"] > 0),
                key=lambda rc: -rc["score"],
            )
            retrieved_unwrapped = [rc["chunk"] for rc in core_retrieved]
            retrieval_rows.append(
                _semantic_ir_metrics(
                    retrieved=retrieved_unwrapped,
                    gabarito=gabarito_chunks,
                    threshold=semantic_threshold,
                )
            )
    finally:
        bulagratis_conn.close()

    total = len(answer_rows)

    summary: dict[str, float] = {
        "medicine_accuracy": sum(row["medicine_correct"] for row in answer_rows)
        / total,
        "section_accuracy": sum(row["section_correct"] for row in answer_rows)
        / total,
        "verdict_accuracy": sum(row["verdict_correct"] for row in answer_rows)
        / total,
    }

    if retrieval_rows:
        for key in retrieval_rows[0]:
            summary[key] = sum(row[key] for row in retrieval_rows) / total

    if results_path:
        _to_json(summary, results_path)

    return summary


def _semantic_ir_metrics(
    retrieved: list[ChunksDict],
    gabarito: list[ChunksDict],
    threshold: float,
) -> dict[str, float]:
    """
    Recall/Precision/MRR/Hit@1/R-Precision/AP com equivalência semântica + section gate.

    Hit = mesma seção AND cosine >= threshold. O section gate elimina falsos
    positivos comuns onde duas passagens da mesma bula compartilham vocabulário
    de domínio (cos alto) mas tratam de coisas diferentes (warnings vs overdose).

    R-Precision e AP são mode-agnostic com respeito a top_k: olham só os
    top-|gabarito| chunks (R-Precision) ou penalizam hits tardios sem fixar k
    (AP). Resolve o viés da precision@k quando top_k > |gabarito|. AP agregado
    via mean-over-queries vira MAP (padrão TREC).
    """
    if not retrieved or not gabarito:
        return {
            "semantic_recall": 0.0,
            "semantic_precision": 0.0,
            "semantic_r_precision": 0.0,
            "semantic_ap": 0.0,
            "semantic_f1": 0.0,
            "semantic_mrr": 0.0,
            "semantic_hit_at_1": 0.0,
        }

    gabarito_covered = sum(
        any(_is_match(g, r, threshold) for r in retrieved) for g in gabarito
    )
    recall = gabarito_covered / len(gabarito)

    retrieved_is_hit = [
        any(_is_match(r, g, threshold) for g in gabarito) for r in retrieved
    ]
    precision = sum(retrieved_is_hit) / len(retrieved)

    r = len(gabarito)
    top_r_hits = sum(retrieved_is_hit[:r])
    r_precision = top_r_hits / r

    hits, ap_sum = 0, 0.0
    for i, is_hit in enumerate(retrieved_is_hit, start=1):
        if is_hit:
            hits += 1
            ap_sum += hits / i
    ap = ap_sum / r

    first_hit_rank = next(
        (i + 1 for i, hit in enumerate(retrieved_is_hit) if hit), None
    )
    mrr = 1.0 / first_hit_rank if first_hit_rank else 0.0
    hit_at_1 = 1.0 if retrieved_is_hit and retrieved_is_hit[0] else 0.0

    return {
        "semantic_recall": recall,
        "semantic_precision": precision,
        "semantic_r_precision": r_precision,
        "semantic_ap": ap,
        "semantic_f1": _harmonic_mean(precision, recall),
        "semantic_mrr": mrr,
        "semantic_hit_at_1": hit_at_1,
    }


def _is_match(a: ChunksDict, b: ChunksDict, threshold: float) -> bool:
    """Chunks casam se estão na mesma seção E cos(embedding) >= threshold."""
    if a["section"] != b["section"]:
        return False
    return _cosine(a["embedding"], b["embedding"]) >= threshold


def _harmonic_mean(a: float, b: float) -> float:
    if a + b == 0:
        return 0.0
    return 2 * a * b / (a + b)


def _has_expected_sections(
    retrieved_sections: list[str],
    expected_sections: list[str],
) -> bool:
    # TODO tem que ser todas?
    return bool(set(retrieved_sections) & set(expected_sections))


def _to_json(
    data: dict[str, Any] | list[Any],
    file_path: str | Path | None = None,
    indent: int = 2,
) -> str | Path:

    json_content = json.dumps(
        data,
        ensure_ascii=False,
        indent=indent,
        default=str,
    )

    if file_path is None:
        return json_content

    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json_content, encoding="utf-8")

    return path


def _fetch_chunks_by_ids(
    conn: sqlite3.Connection,
    ids: list[str],
) -> list[ChunksDict]:
    """Busca chunks (text + embedding) por uma lista de IDs."""
    if not ids:
        return []

    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    placeholders = ",".join("?" for _ in ids)
    cursor.execute(
        f"""
        SELECT id, medicine_id, medicine_name, section,
               paragraph_idx, chunk_idx, text, embedding
        FROM chunks
        WHERE id IN ({placeholders})
        """,
        ids,
    )

    chunks: list[ChunksDict] = []
    for row in cursor.fetchall():
        data = dict(row)
        data["embedding"] = _parse_embedding(data["embedding"])
        chunks.append(ChunksDict(**data))
    return chunks
