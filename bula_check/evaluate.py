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
from bula_check.agents.search import _fetch_chunks_for_medicine
from bula_check.agents.search import _parse_embedding
from bula_check.agents.search import normalize_text


class ExpectedResult(BaseModel):
    id: str
    query: str
    expected_medicine: str
    expected_sections: list[str]
    expected_verdict: str
    expected_chunk_ids: list[str]
    # Campos autorais (humano), ignorados pelas métricas. Servem para a
    # validação por farmacêutico via review.build_review_csv:
    #   medicine_brand — marca como aparece na query (ex: "Buscofem"), distinta
    #     do genérico expected_medicine ("ibuprofeno").
    #   justification  — por que este veredito, ancorado no chunk-gabarito.
    #   validation     — parecer do farmacêutico (status/validated_by/comments),
    #     preenchido após a revisão.
    medicine_brand: str | None = None
    justification: str | None = None
    validation: dict[str, Any] | None = None
    # Marca a questão como caso-desafio (stress test) e o modo de falha alvo
    # (ex: "marca", "erro_digitacao", "giria", "negacao", "fidelidade"). None
    # nas questões representativas normais.
    stress_category: str | None = None

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
                    "expected_verdict": item.expected_verdict,
                    "predicted_verdict": predicted_verdict,
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

            # universo de candidatos (N) para o VN da matriz de confusão de
            # chunks: chunks do medicamento nas seções consultadas — exatamente
            # o pool que o ranker pontuou em hybrid_chunk_search.
            n_candidates = 0
            if selected_medicine:
                n_candidates = len(
                    _fetch_chunks_for_medicine(
                        bulagratis_conn,
                        selected_medicine["medicine"]["id"],
                        parsed_query.get("sections") if parsed_query else None,
                    )
                )

            retrieval_rows.append(
                _semantic_ir_metrics(
                    retrieved=retrieved_unwrapped,
                    gabarito=gabarito_chunks,
                    threshold=semantic_threshold,
                    n_candidates=n_candidates,
                )
            )
    finally:
        bulagratis_conn.close()

    total = len(answer_rows)

    summary: dict[str, Any] = {
        "medicine_accuracy": sum(row["medicine_correct"] for row in answer_rows)
        / total,
        "section_accuracy": sum(row["section_correct"] for row in answer_rows)
        / total,
        "verdict_accuracy": sum(row["verdict_correct"] for row in answer_rows)
        / total,
    }

    # Métricas de IR são médias por questão; vp/fp/vn/fn são contagens somadas
    # (pooled/micro) entre as questões para formar a matriz de confusão de chunks.
    confusion_keys = {"vp", "fp", "vn", "fn"}
    chunk_totals = {"vp": 0, "fp": 0, "vn": 0, "fn": 0}
    if retrieval_rows:
        for key in retrieval_rows[0]:
            column_sum = sum(row[key] for row in retrieval_rows)
            if key in confusion_keys:
                chunk_totals[key] = int(column_sum)
            else:
                summary[key] = column_sum / total

        # Matriz de confusão de chunks — MICRO: soma das contagens de TODAS as
        # questões (cada decisão-por-chunk pesa igual). Linhas = real
        # (relevante/não); colunas = previsto (recuperado/não).
        summary["chunk_confusion_matrix_micro"] = {
            "relevante": {
                "recuperado": chunk_totals["vp"],  # VP
                "nao_recuperado": chunk_totals["fn"],  # FN
            },
            "nao_relevante": {
                "recuperado": chunk_totals["fp"],  # FP
                "nao_recuperado": chunk_totals["vn"],  # VN
            },
        }

        # Precision/recall/F1 MICRO, derivados da matriz micro (pooled), para a
        # matriz micro ser internamente reconciliável: quem recalcular P da
        # matriz acha exatamente este número. As métricas semantic_* SEM sufixo
        # são MACRO (média por questão, padrão TREC) e ponderam cada query 1/N;
        # as _micro ponderam por tamanho do conjunto relevante/recuperado. F1
        # micro = média harmônica de P e R POOLED (não a média dos f1_q, que
        # seria matematicamente sem sentido). MAP/MRR/hit@1 NÃO têm versão micro
        # — dependem do rank intra-query, que a matriz de contagens descarta.
        vp_t, fp_t, fn_t = chunk_totals["vp"], chunk_totals["fp"], chunk_totals["fn"]
        vn_t = chunk_totals["vn"]
        precision_micro = vp_t / (vp_t + fp_t) if (vp_t + fp_t) else 0.0
        recall_micro = vp_t / (vp_t + fn_t) if (vp_t + fn_t) else 0.0
        summary["semantic_precision_micro"] = precision_micro
        summary["semantic_recall_micro"] = recall_micro
        summary["semantic_f1_micro"] = _harmonic_mean(precision_micro, recall_micro)

        # Total de documentos (decisões chunk×query) analisados = soma das 4
        # células da matriz micro = Σ N_q (pool pontuado por questão, somado).
        chunk_total_docs = vp_t + fp_t + fn_t + vn_t
        summary["chunk_total_docs"] = chunk_total_docs
        # Acurácia de retrieval (chunk-level, micro) = (VP+VN)/total. CAVEAT: é
        # dominada pelo VN (tende a ≈1, pouco informativa) e NÃO é comparável
        # entre configs — o VN colapsa para 0 quando não há medicamento
        # selecionado (baseline). É só o fechamento da matriz; para qualidade de
        # busca use recall/precision/MRR.
        summary["chunk_accuracy"] = (
            (vp_t + vn_t) / chunk_total_docs if chunk_total_docs else 0.0
        )

    # Matriz de confusão DO SISTEMA: sobre o veredito final. Cada questão é 1
    # amostra → contagem direta sobre o dataset (sem ambiguidade micro/macro).
    # 3x3 (descritiva, com inconclusive) + 2x2 (binária V/F, foco do artigo).
    verdict_pairs = [
        (row["expected_verdict"], row["predicted_verdict"]) for row in answer_rows
    ]
    summary["verdict_confusion_matrix"] = _verdict_confusion_matrix(verdict_pairs)

    summary["config"] = {
        "llm_provider": str(config["llm_provider"]),
        "llm_model": config["llm_model"],
        "return_chunks": config["return_chunks"],
        "bulagratis_db": str(config["bulagratis_db_path"]),
        "with_rag": config["with_rag"],
        "lexical_weight": config["lexical_weight"],
        "semantic_weight": config["semantic_weight"],
    }

    if results_path:
        _to_json(summary, results_path)

    return summary


_VERDICT_LABELS = ("confirmed", "refuted", "inconclusive")


def _norm_verdict(verdict: str) -> str:
    """
    Mapeia o veredito para o espaço canônico (confirmed/refuted/inconclusive).
    Um veredito ausente ("" — o pipeline não chegou ao verify_claim, ex:
    medicamento não encontrado) é tratado como `inconclusive`, evitando uma
    classe "none" fora do espaço de rótulos.
    """
    return verdict if verdict in _VERDICT_LABELS else "inconclusive"


def _verdict_confusion_matrix(
    pairs: list[tuple[str, str]],
) -> dict[str, dict[str, int]]:
    """
    Matriz 3x3 do veredito do sistema (confirmed/refuted/inconclusive). Linhas =
    esperado, colunas = previsto; cada célula é a contagem de questões.

    Uma questão é exatamente uma amostra, então é a matriz direta sobre o dataset
    — sem ambiguidade micro/macro. Útil como tabela descritiva; note que, no
    dataset atual, `inconclusive` tem poucos exemplos.
    """
    matrix = {
        expected: {predicted: 0 for predicted in _VERDICT_LABELS}
        for expected in _VERDICT_LABELS
    }
    for expected, predicted in pairs:
        matrix[_norm_verdict(expected)][_norm_verdict(predicted)] += 1
    return matrix


def _semantic_ir_metrics(
    retrieved: list[ChunksDict],
    gabarito: list[ChunksDict],
    threshold: float,
    n_candidates: int,
) -> dict[str, float]:
    """
    Recall/Precision/MRR/Hit@1/R-Precision/AP + contagens da matriz de confusão
    de chunks (vp/fp/vn/fn), com equivalência semântica + section gate.

    Hit = mesma seção AND cosine >= threshold. O section gate elimina falsos
    positivos comuns onde duas passagens da mesma bula compartilham vocabulário
    de domínio (cos alto) mas tratam de coisas diferentes (warnings vs overdose).

    R-Precision e AP são mode-agnostic com respeito a top_k: olham só os
    top-|gabarito| chunks (R-Precision) ou penalizam hits tardios sem fixar k
    (AP). Resolve o viés da precision@k quando top_k > |gabarito|. AP agregado
    via mean-over-queries vira MAP (padrão TREC).

    Matriz de confusão (contagem de chunks, sobre o pool pontuado pelo ranker):
      - vp: recuperados que são relevantes (hits)
      - fp: recuperados que não são relevantes (= n_retrieved - vp)
      - fn: relevantes do gabarito não cobertos (= n_gabarito - gabarito_covered)
      - vn: candidatos restantes (= n_candidates - vp - fp - fn, clamped >= 0)
    `n_candidates` é o universo = chunks do medicamento nas seções consultadas.
    Estas contagens são somadas (pooled) entre questões para formar a matriz
    binária do summary.
    """
    n_retrieved = len(retrieved)
    n_gabarito = len(gabarito)

    if not retrieved or not gabarito:
        # vp=0; com um lado vazio, fp=n_retrieved (nenhum relevante existe) ou
        # fn=n_gabarito (nada foi recuperado) — o lado vazio zera sozinho.
        fp = n_retrieved
        fn = n_gabarito
        return {
            "semantic_recall": 0.0,
            "semantic_precision": 0.0,
            "semantic_r_precision": 0.0,
            "semantic_ap": 0.0,
            "semantic_f1": 0.0,
            "semantic_mrr": 0.0,
            "semantic_hit_at_1": 0.0,
            "vp": 0,
            "fp": fp,
            "vn": max(0, n_candidates - fp - fn),
            "fn": fn,
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

    vp = sum(retrieved_is_hit)
    fp = n_retrieved - vp
    fn = n_gabarito - gabarito_covered
    vn = max(0, n_candidates - vp - fp - fn)

    return {
        "semantic_recall": recall,
        "semantic_precision": precision,
        "semantic_r_precision": r_precision,
        "semantic_ap": ap,
        "semantic_f1": _harmonic_mean(precision, recall),
        "semantic_mrr": mrr,
        "semantic_hit_at_1": hit_at_1,
        "vp": vp,
        "fp": fp,
        "vn": vn,
        "fn": fn,
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
