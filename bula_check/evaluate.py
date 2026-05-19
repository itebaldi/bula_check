import json
from typing import Any

from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph
from nemo.vector_retrieval.metrics import Path
from nemo.vector_retrieval.metrics import Relevance
from nemo.vector_retrieval.metrics import compute_metrics
from nemo.vector_retrieval.query import RankedDocument
from nemo.vector_retrieval.search import SearchResults
from pydantic import BaseModel

from bula_check.agents.pipeline import make_initial_state
from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.search import normalize_text


class ExpectecResult(BaseModel):
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
    items: list[ExpectecResult],
    results_path: Path | None,
):
    all_chunk_ids: set[str] = set()
    per_query_results: dict[str, list[tuple[str, float]]] = {}
    per_query_relevance: dict[str, set[str]] = {}

    answer_rows: list[dict[str, Any]] = []

    for item in items:
        state = make_initial_state(config)
        state["messages"].append(HumanMessage(content=item.query))

        final_state = graph.invoke(state)  # type: ignore

        selected_medicine = final_state.get("selected_medicine")
        retrieved_chunks = final_state.get("retrieved_chunks", [])
        verification_result = final_state.get("verification_result")

        query_id = item.id

        expected_chunk_ids = set(item.get("expected_chunk_ids", []))
        retrieved_chunk_pairs: list[tuple[str, float]] = []

        for chunk in retrieved_chunks:
            chunk_id = chunk["chunk"]["id"]
            score = float(chunk["score"])  # ??

            all_chunk_ids.add(chunk_id)
            retrieved_chunk_pairs.append((chunk_id, score))

        all_chunk_ids.update(expected_chunk_ids)

        per_query_results[query_id] = retrieved_chunk_pairs
        per_query_relevance[query_id] = expected_chunk_ids

        predicted_medicine = ""
        if selected_medicine:
            predicted_medicine = selected_medicine["medicine"]["name"]

        predicted_verdict = ""
        if verification_result:
            predicted_verdict = verification_result["verdict"]

        predicted_sections = [
            chunk["chunk"]["section"] for chunk in retrieved_chunks
        ]

        medicine_correct = normalize_text(item.expected_medicine) in normalize_text(
            predicted_medicine
        )

        section_correct = _has_expected_sections(
            retrieved_sections=predicted_sections,
            expected_sections=item.expected_sections,
        )

        verdict_correct = item.expected_verdict == predicted_verdict

        answer_rows.append(
            {
                "medicine_correct": medicine_correct,
                "verdict_correct": verdict_correct,
                "section_correct": section_correct,
            }
        )

    chunk_id_to_int = {
        chunk_id: index
        for index, chunk_id in enumerate(sorted(all_chunk_ids), start=1)
    }

    relevance = Relevance(
        query_per_documents={
            query_id: {
                chunk_id_to_int[chunk_id]
                for chunk_id in expected_chunk_ids
                if chunk_id in chunk_id_to_int
            }
            for query_id, expected_chunk_ids in per_query_relevance.items()
        }
    )

    search_results = SearchResults(
        root={
            query_id: [
                RankedDocument(
                    document_id=chunk_id_to_int[chunk_id],
                    score=score,
                    rank=idx,
                )
                for idx, (chunk_id, score) in enumerate(retrieved_chunk_pairs)
                if chunk_id in chunk_id_to_int
            ]
            for query_id, retrieved_chunk_pairs in per_query_results.items()
        }
    )

    retrieval_metrics = compute_metrics(
        relevance=relevance,
        search_results=search_results,
    )

    retrieval_summary = retrieval_metrics.summary()

    total = len(answer_rows)

    summary = {
        "medicine_accuracy": sum(row["medicine_correct"] for row in answer_rows)
        / total,
        "verdict_accuracy": sum(row["verdict_correct"] for row in answer_rows)
        / total,
        # "retrieval_metrics": retrieval_metrics.model_dump(),
        # "results": answer_rows,
    }

    final_dict = dict(retrieval_summary)
    final_dict.update(summary)

    if results_path:
        _to_json(final_dict, results_path)

    return final_dict


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
