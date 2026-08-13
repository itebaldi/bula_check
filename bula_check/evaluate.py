import json
import re
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
    slices: list[str] = []  # fatia de cada item: stress_category ou "representative"
    val_status: list[str] = []  # parecer da validação: aprovado/reprovado/sem_parecer
    ids: list[str] = []
    retrieved_ids: list[list[str]] = []  # chunk_ids recuperados (ordem do ranker)
    gabarito_ids: list[list[str]] = []  # chunk_ids do gabarito (expected)
    n_candidates_list: list[int] = []  # tamanho do pool pontuado (p/ recompute)
    failure_reasons: list[str | None] = []  # estágio em que o item falhou
    parsed_names: list[str] = []  # nome extraído pelo parse da query
    selected_names: list[str] = []  # nome resolvido no banco (vazio = não achou)

    bulagratis_conn = _open_db(config["bulagratis_db_path"])
    anvisa_conn = _open_db(config["anvisa_db_path"])

    try:
        for item in items:
            ids.append(item.id)
            slices.append(item.stress_category or "representative")
            val_status.append((item.validation or {}).get("status") or "sem_parecer")
            state = make_initial_state(config)
            state["messages"].append(HumanMessage(content=item.query))

            pipeline_error: str | None = None
            try:
                final_state = graph.invoke(state)  # type: ignore
            except Exception as error:
                # Um item que quebra o pipeline não deve abortar o benchmark
                # inteiro; registra como miss total e segue.
                print(f"[eval] falha no item {item.id}: {error}")
                final_state = {}
                pipeline_error = f"{type(error).__name__}: {error}"

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

            failure_reason = _failure_reason(
                pipeline_error=pipeline_error,
                parse_error=final_state.get("parse_error"),
                selected_medicine=selected_medicine,
                retrieved_chunks=retrieved_chunks,
                with_rag=config["with_rag"],
            )
            failure_reasons.append(failure_reason)
            parsed_names.append(
                parsed_query.get("medicine_name", "") if parsed_query else ""
            )
            selected_names.append(
                selected_medicine["medicine"]["name"] if selected_medicine else ""
            )

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
                    "medicine_correct": _medicine_correct(
                        anvisa_conn,
                        item.expected_medicine,
                        predicted_medicine,
                        predicted_active_ingredient,
                    ),
                    "section_correct": _has_expected_sections(
                        retrieved_sections=predicted_sections,
                        expected_sections=item.expected_sections,
                    ),
                    # Abstenção não é acerto: quando o medicamento não é
                    # resolvido, o `inconclusive` do verify_claim é um "não sei
                    # do que você está falando", não um julgamento da alegação.
                    "verdict_correct": (
                        failure_reason not in _ABSTENTION_REASONS
                        and item.expected_verdict == predicted_verdict
                    ),
                    "expected_verdict": item.expected_verdict,
                    "predicted_verdict": predicted_verdict,
                    "failure_reason": failure_reason,
                }
            )

            gabarito_chunks = _fetch_chunks_by_ids(
                bulagratis_conn, list(item.expected_chunk_ids)
            )
            # Filtra chunks-vizinhos injetados pelo modo "with_prev_and_next"
            # (score negativo = NEIGHBOUR_SCORE em search._expand_with_neighbours)
            # e recupera a ordem por score. Usa `>= 0` (não `> 0`): chunks
            # ranqueados podem ter score 0.0 legítimo pela normalização min-max
            # (ex.: o menor da seção, ou o único chunk de uma seção) e NÃO devem
            # ser descartados — só os vizinhos (score < 0) saem. Sem isso, o modo
            # de retorno poluiria as métricas; o efeito do contexto aparece em
            # verdict_accuracy.
            core_retrieved = sorted(
                (rc for rc in retrieved_chunks if rc["score"] >= 0),
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
            retrieved_ids.append([c["id"] for c in retrieved_unwrapped])
            gabarito_ids.append(list(item.expected_chunk_ids))
            n_candidates_list.append(n_candidates)
    finally:
        bulagratis_conn.close()
        anvisa_conn.close()

    summary = _aggregate(answer_rows, retrieval_rows)

    # Fatiamento para diagnóstico: mostra ONDE a falha se concentra (por modo de
    # falha adversarial e representativas vs desafio) sem re-rodar o pipeline.
    groups: dict[str, list[int]] = {}
    for i, slice_name in enumerate(slices):
        groups.setdefault(slice_name, []).append(i)

    def _slice(idxs: list[int]) -> dict[str, Any]:
        return _aggregate(
            [answer_rows[i] for i in idxs],
            [retrieval_rows[i] for i in idxs],
        )

    summary["by_stress_category"] = {
        name: _slice(idxs) for name, idxs in sorted(groups.items())
    }
    # baseline = questões plain (sem padrão-desafio). Aceita None (legado) e
    # "representativa" (rótulo explícito das representativas classificadas).
    baseline = {"representative", "representativa"}
    summary["stress_vs_representative"] = {
        "representative": _slice(
            [i for i, s in enumerate(slices) if s in baseline]
        ),
        "stress": _slice(
            [i for i, s in enumerate(slices) if s not in baseline]
        ),
    }

    # Fatiamento pelo parecer da validação (juiz/farmacêutico). Só popula se os
    # items carregarem o bloco `validation` (ex.: rodar sobre o dataset julgado).
    # Permite reportar o número cheio (todas) e o recorte "só-aprovadas" sem
    # descartar as questões contestadas — que são as mais difíceis (fidelidade).
    val_groups: dict[str, list[int]] = {}
    for i, status in enumerate(val_status):
        val_groups.setdefault(status, []).append(i)
    summary["by_validation"] = {
        name: _slice(idxs) for name, idxs in sorted(val_groups.items())
    }
    summary["validated_vs_flagged"] = {
        "aprovado": _slice([i for i, s in enumerate(val_status) if s == "aprovado"]),
        "flagged": _slice([i for i, s in enumerate(val_status) if s == "reprovado"]),
    }

    # Fatiamento pelo estágio que falhou: responde "de onde vêm os vereditos
    # ausentes/inconclusivos" sem abrir o arquivo de itens.
    fail_groups: dict[str, list[int]] = {}
    for i, reason in enumerate(failure_reasons):
        fail_groups.setdefault(reason or "ok", []).append(i)
    summary["by_failure_reason"] = {
        name: _slice(idxs) for name, idxs in sorted(fail_groups.items())
    }

    summary["config"] = {
        "llm_provider": str(config["llm_provider"]),
        "llm_model": config["llm_model"],
        "return_chunks": config["return_chunks"],
        "bulagratis_db": str(config["bulagratis_db_path"]),
        "with_rag": config["with_rag"],
        "lexical_weight": config["lexical_weight"],
        "semantic_weight": config["semantic_weight"],
    }

    # Detalhe por questão → permite re-fatiar a análise offline (por categoria,
    # veredito, medicamento) sem re-invocar o grafo (que custa OpenAI).
    item_details = [
        {
            "id": ids[i],
            "stress_category": (
                None if slices[i] == "representative" else slices[i]
            ),
            "validation_status": val_status[i],
            "medicine_correct": answer_rows[i]["medicine_correct"],
            "section_correct": answer_rows[i]["section_correct"],
            "verdict_correct": answer_rows[i]["verdict_correct"],
            "expected_verdict": answer_rows[i]["expected_verdict"],
            "predicted_verdict": answer_rows[i]["predicted_verdict"],
            "semantic_recall": retrieval_rows[i].get("semantic_recall"),
            "semantic_hit_at_1": retrieval_rows[i].get("semantic_hit_at_1"),
            # ids para recomputar as métricas de retrieval offline (sem LLM),
            # via recompute_metrics — ver docstring da função.
            "retrieved_chunk_ids": retrieved_ids[i],
            "gabarito_chunk_ids": gabarito_ids[i],
            "n_candidates": n_candidates_list[i],
            # Atribuição da falha: sem isto, um veredito ausente ou inconclusive
            # não distingue "o pipeline quebrou" de "o parse falhou" de "o
            # medicamento não está no banco".
            "failure_reason": failure_reasons[i],
            "parsed_medicine_name": parsed_names[i],
            "selected_medicine_name": selected_names[i],
        }
        for i in range(len(answer_rows))
    ]

    if results_path:
        _to_json(summary, results_path)
        _to_json(item_details, _items_path(results_path))

    return summary


def recompute_metrics(
    items_path: str | Path,
    db_path: str | Path,
    threshold: float = 0.80,
) -> dict[str, Any]:
    """Recalcula as métricas a partir de um `{name}_items.json`, sem LLM/grafo.

    Usa os `retrieved_chunk_ids` / `gabarito_chunk_ids` / `n_candidates`
    persistidos por `evaluate_results` e busca embeddings/seções por id no banco,
    reaplicando `_semantic_ir_metrics` e `_aggregate`. Permite reavaliar mudanças
    de métrica (threshold, regra de match) offline, sem re-rodar o pipeline.

    Parameters
    ----------
    items_path : str | Path
        Arquivo `{name}_items.json` gerado por `evaluate_results` (pós-fix, com os
        campos de ids persistidos).
    db_path : str | Path
        Banco de chunks (mesmo usado no run: bulas_gratis.db ou o _sliding).
    threshold : float
        Limiar de cosseno para `_is_match`.

    Returns
    -------
    dict[str, Any]
        Summary no mesmo formato de `evaluate_results` (métricas + fatiamentos).
    """
    items = json.loads(Path(items_path).read_text(encoding="utf-8"))
    # Arquivos antigos (ex.: rag_5.1_items.json) não persistiram os ids dos
    # chunks; sem o guard, _semantic_ir_metrics devolveria 0.0 em todas as
    # métricas de retrieval e o summary recomputado apagaria os números da
    # rodada. Nesses casos só as métricas de resposta são recalculadas.
    has_ids = all("retrieved_chunk_ids" in it for it in items)
    conn = _open_db(Path(db_path))
    try:
        def _ordered(ids: list[str]) -> list[ChunksDict]:
            by_id = {c["id"]: c for c in _fetch_chunks_by_ids(conn, list(ids))}
            return [by_id[i] for i in ids if i in by_id]

        answer_rows: list[dict[str, Any]] = []
        retrieval_rows: list[dict[str, float]] = []
        slices: list[str] = []
        val_status: list[str] = []
        failure_reasons: list[str] = []
        for it in items:
            answer_rows.append(
                {
                    "medicine_correct": it["medicine_correct"],
                    "section_correct": it["section_correct"],
                    "verdict_correct": it["verdict_correct"],
                    "expected_verdict": it["expected_verdict"],
                    "predicted_verdict": it["predicted_verdict"],
                    "failure_reason": it.get("failure_reason"),
                }
            )
            if has_ids:
                retrieval_rows.append(
                    _semantic_ir_metrics(
                        retrieved=_ordered(it["retrieved_chunk_ids"]),
                        gabarito=_ordered(it.get("gabarito_chunk_ids", [])),
                        threshold=threshold,
                        n_candidates=it.get("n_candidates", 0),
                    )
                )
            slices.append(it.get("stress_category") or "representative")
            val_status.append(it.get("validation_status") or "sem_parecer")
            # Itens de rodadas antigas não têm o motivo; o que dá para afirmar é
            # que ficaram sem veredito, não em qual estágio pararam.
            failure_reasons.append(
                it.get("failure_reason")
                or ("unattributed_no_verdict" if not it["predicted_verdict"] else "ok")
            )
    finally:
        conn.close()

    summary = _aggregate(answer_rows, retrieval_rows)
    summary["retrieval_metrics_available"] = has_ids

    def _slice(idxs: list[int]) -> dict[str, Any]:
        return _aggregate(
            [answer_rows[i] for i in idxs],
            [retrieval_rows[i] for i in idxs] if has_ids else [],
        )

    groups: dict[str, list[int]] = {}
    for i, name in enumerate(slices):
        groups.setdefault(name, []).append(i)
    summary["by_stress_category"] = {
        name: _slice(idxs) for name, idxs in sorted(groups.items())
    }
    val_groups: dict[str, list[int]] = {}
    for i, status in enumerate(val_status):
        val_groups.setdefault(status, []).append(i)
    summary["by_validation"] = {
        name: _slice(idxs) for name, idxs in sorted(val_groups.items())
    }
    fail_groups: dict[str, list[int]] = {}
    for i, reason in enumerate(failure_reasons):
        fail_groups.setdefault(reason, []).append(i)
    summary["by_failure_reason"] = {
        name: _slice(idxs) for name, idxs in sorted(fail_groups.items())
    }
    return summary


# Versão do formato de métricas gravado no summary:
#   1 — matriz 3x3, com o veredito ausente ("") dobrado em `inconclusive`
#   2 — matriz 3x4 com `sem_resposta`, mais answer_rate / accuracy_answered
METRICS_VERSION = 2


def rewrite_summaries(
    results_dir: str | Path = "outputs/evaluation/results",
    dry_run: bool = True,
) -> dict[str, dict[str, Any]]:
    """Reescreve os summaries já gravados no formato de métricas atual, offline.

    Para cada `{name}_items.json` recomputa as métricas com `recompute_metrics` e
    faz merge por cima do summary existente — o recompute não emite `config`,
    `stress_vs_representative` nem `validated_vs_flagged`, que são a proveniência
    da rodada e precisam sobreviver. O banco vem do `config.bulagratis_db` do
    próprio arquivo. Summaries sem arquivo de itens não podem ser recomputados e
    só recebem `metrics_version: 1`.

    Parameters
    ----------
    results_dir : str | Path
        Pasta com os `{name}.json` e `{name}_items.json`.
    dry_run : bool
        Se True (padrão) nada é gravado; devolve o que mudaria.

    Returns
    -------
    dict[str, dict[str, Any]]
        Por arquivo: `metrics_version`, `recomputed` e as diferenças de valor nas
        métricas escalares que já existiam.
    """
    directory = Path(results_dir)
    report: dict[str, dict[str, Any]] = {}

    for summary_path in sorted(directory.glob("*.json")):
        if summary_path.name.endswith("_items.json"):
            continue
        old = json.loads(summary_path.read_text(encoding="utf-8"))
        items_path = _items_path(summary_path)

        if not items_path.exists():
            report[summary_path.name] = {"metrics_version": 1, "recomputed": False}
            if not dry_run:
                _to_json({**old, "metrics_version": 1}, summary_path)
            continue

        db_path = (old.get("config") or {}).get("bulagratis_db") or "bulas_gratis.db"
        new = recompute_metrics(items_path, db_path)
        merged = {**old, **new, "metrics_version": METRICS_VERSION}

        changed = {
            key: (old[key], new[key])
            for key in old
            if key in new
            and isinstance(old[key], (int, float))
            and old[key] != new[key]
        }
        report[summary_path.name] = {
            "metrics_version": METRICS_VERSION,
            "recomputed": True,
            "retrieval_metrics_available": new["retrieval_metrics_available"],
            "changed": changed,
        }
        if not dry_run:
            _to_json(merged, summary_path)

    return report


# Sais / formas farmacêuticas — qualificadores que não identificam o fármaco.
_SALT_FORMS = {
    "cloridrato", "sulfato", "mesilato", "succinato", "maleato", "fosfato",
    "hemifumarato", "fumarato", "sodico", "sodica", "sodio", "calcio",
    "potassio", "dihidratado", "monoidratado", "acido", "besilato",
    "bromidrato", "nitrato", "tartarato", "valerato", "dipropionato",
    "medoxomila", "cilexetila", "dihidratada", "hidratado", "hidratada",
    "trihidratado", "propionato", "acetato", "citrato", "estearato",
}


def _ingredient_tokens(text: str) -> set[str]:
    """Tokens de princípio ativo (>=5 letras, sem sais/formas farmacêuticas)."""
    tokens = re.split(r"[,+;/\s]+", normalize_text(text).lower())
    return {t for t in tokens if len(t) >= 5 and t not in _SALT_FORMS}


def _active_ingredients(anvisa_conn: sqlite3.Connection, name: str) -> set[str]:
    """
    Princípios ativos de um medicamento, resolvidos via ANVISA
    (name -> active_ingredient, coluna preenchida em bulas_anvisa.db). Fallback:
    tokeniza o próprio nome quando ele já é o genérico. Usado para creditar
    equivalência marca <-> genérico em `medicine_correct`.
    """
    norm = normalize_text(name)
    if not norm:
        return set()
    row = anvisa_conn.execute(
        "SELECT active_ingredient FROM medicines "
        "WHERE processed_name = ? AND active_ingredient IS NOT NULL "
        "AND TRIM(active_ingredient) <> '' LIMIT 1",
        (norm,),
    ).fetchone()
    if row is None:
        row = anvisa_conn.execute(
            "SELECT active_ingredient FROM medicines "
            "WHERE processed_name LIKE ? AND active_ingredient IS NOT NULL "
            "AND TRIM(active_ingredient) <> '' LIMIT 1",
            (f"%{norm}%",),
        ).fetchone()
    if row and row[0]:
        return _ingredient_tokens(row[0])
    return _ingredient_tokens(name)


def _medicine_correct(
    anvisa_conn: sqlite3.Connection,
    expected: str,
    predicted_name: str,
    predicted_ai: str,
) -> bool:
    """
    Correto se o esperado casa por substring (comportamento antigo) OU se o
    previsto compartilha o mesmo princípio ativo do esperado — credita resolução
    marca <-> genérico (ex: expected "MENSYVA" vs previsto "HEMIFUMARATO DE
    QUETIAPINA", ambos quetiapina). A equivalência exige que TODO princípio ativo
    esperado esteja no previsto (subconjunto), evitando leniência entre fármacos
    distintos que só compartilham um sal.
    """
    exp_norm = normalize_text(expected)
    if exp_norm in normalize_text(predicted_name) or exp_norm in normalize_text(
        predicted_ai
    ):
        return True
    expected_ai = _active_ingredients(anvisa_conn, expected)
    if not expected_ai:
        return False
    return expected_ai <= _active_ingredients(anvisa_conn, predicted_name)


# Motivos em que o pipeline não chegou a julgar a alegação. O veredito
# registrado nesses casos é o "não encontrei" do verify_claim — abstenção, não
# resposta. Historicamente esses itens vinham como `""` (o grafo terminava sem
# verificação); manter a distinção é o que deixa as duas séries comparáveis.
_ABSTENTION_REASONS = frozenset(
    {"pipeline_error", "parse_failed", "medicine_not_found"}
)


def _failure_reason(
    pipeline_error: str | None,
    parse_error: str | None,
    selected_medicine: Any,
    retrieved_chunks: list[Any],
    with_rag: bool,
) -> str | None:
    """
    Estágio em que o item falhou, do mais a montante para o mais a jusante.
    None quando o fluxo completou (o veredito veio do LLM com evidência).

    Returns
    -------
    str | None
        "pipeline_error", "parse_failed", "medicine_not_found", "no_chunks" ou
        None.
    """
    if pipeline_error:
        return "pipeline_error"
    if parse_error:
        return "parse_failed"
    if not with_rag:
        return None
    if not selected_medicine:
        return "medicine_not_found"
    if not retrieved_chunks:
        return "no_chunks"
    return None


def _items_path(results_path: str | Path) -> Path:
    """Caminho do detalhe por item: {name}.json -> {name}_items.json."""
    path = Path(results_path)
    return path.with_name(f"{path.stem}_items.json")


def _is_abstention(row: dict[str, Any]) -> bool:
    """
    O item não recebeu um julgamento da alegação: ou o veredito nunca foi
    produzido (`""`, rodadas anteriores ao roteamento que sempre verifica), ou o
    pipeline parou antes de ter evidência (`failure_reason` de abstenção).
    """
    if row.get("failure_reason") in _ABSTENTION_REASONS:
        return True
    return row["predicted_verdict"] not in _VERDICT_LABELS


def _aggregate(
    answer_rows: list[dict[str, Any]],
    retrieval_rows: list[dict[str, float]],
) -> dict[str, Any]:
    """
    Agrega as métricas de um conjunto de questões (answer/retrieval rows
    index-alinhados). Usado tanto para o summary global quanto para cada fatia
    (stress_category / representativas vs desafio). Retorna {} com n=0 se vazio.

    - accuracies (medicine/section/verdict): média por questão.
    - semantic_* (sem sufixo): MACRO (média por questão, padrão TREC).
    - semantic_*_micro + matriz de chunks: MICRO (contagens vp/fp/vn/fn somadas).
    """
    total = len(answer_rows)
    if total == 0:
        return {"n": 0}

    # Abstenções (veredito "" das rodadas antigas, ou o "não encontrei" do
    # verify_claim nas novas) contam como erro em verdict_accuracy; answer_rate e
    # verdict_accuracy_answered separam "errou o veredito" de "não respondeu".
    answered = [r for r in answer_rows if not _is_abstention(r)]
    out: dict[str, Any] = {
        "n": total,
        "medicine_accuracy": sum(r["medicine_correct"] for r in answer_rows) / total,
        "section_accuracy": sum(r["section_correct"] for r in answer_rows) / total,
        "verdict_accuracy": sum(r["verdict_correct"] for r in answer_rows) / total,
        "verdict_answer_rate": len(answered) / total,
        "verdict_accuracy_answered": (
            sum(r["verdict_correct"] for r in answered) / len(answered)
            if answered
            else 0.0
        ),
    }

    confusion_keys = {"vp", "fp", "vn", "fn"}
    chunk_totals = {"vp": 0, "fp": 0, "vn": 0, "fn": 0}
    if retrieval_rows:
        for key in retrieval_rows[0]:
            column_sum = sum(row[key] for row in retrieval_rows)
            if key in confusion_keys:
                chunk_totals[key] = int(column_sum)
            else:
                out[key] = column_sum / total

        out["chunk_confusion_matrix_micro"] = {
            "relevante": {
                "recuperado": chunk_totals["vp"],  # VP
                "nao_recuperado": chunk_totals["fn"],  # FN
            },
            "nao_relevante": {
                "recuperado": chunk_totals["fp"],  # FP
                "nao_recuperado": chunk_totals["vn"],  # VN
            },
        }

        vp_t, fp_t, fn_t = chunk_totals["vp"], chunk_totals["fp"], chunk_totals["fn"]
        vn_t = chunk_totals["vn"]
        precision_micro = vp_t / (vp_t + fp_t) if (vp_t + fp_t) else 0.0
        recall_micro = vp_t / (vp_t + fn_t) if (vp_t + fn_t) else 0.0
        out["semantic_precision_micro"] = precision_micro
        out["semantic_recall_micro"] = recall_micro
        out["semantic_f1_micro"] = _harmonic_mean(precision_micro, recall_micro)

        chunk_total_docs = vp_t + fp_t + fn_t + vn_t
        out["chunk_total_docs"] = chunk_total_docs
        out["chunk_accuracy"] = (
            (vp_t + vn_t) / chunk_total_docs if chunk_total_docs else 0.0
        )

    verdict_pairs = [
        (
            r["expected_verdict"],
            "" if _is_abstention(r) else r["predicted_verdict"],
        )
        for r in answer_rows
    ]
    out["verdict_confusion_matrix"] = _verdict_confusion_matrix(verdict_pairs)
    return out


def print_stress_breakdown(summary: dict[str, Any]) -> None:
    """Imprime uma tabela das métricas por fatia (para inspeção rápida)."""
    by_cat = summary.get("by_stress_category", {})
    cols = [
        "n",
        "medicine_accuracy",
        "section_accuracy",
        "verdict_accuracy",
        "semantic_recall",
        "semantic_hit_at_1",
    ]
    header = f"{'fatia':<18}" + "".join(f"{c[:10]:>12}" for c in cols)
    print(header)
    print("-" * len(header))
    for name, metrics in by_cat.items():
        cells = []
        for col in cols:
            value = metrics.get(col)
            if value is None:
                cells.append(f"{'-':>12}")
            elif col == "n":
                cells.append(f"{value:>12}")
            else:
                cells.append(f"{value:>12.3f}")
        print(f"{name:<18}" + "".join(cells))


def slice_from_items(items_path: str | Path) -> dict[str, dict[str, float]]:
    """
    Recomputa accuracies e recall/hit@1 médios por stress_category a partir do
    arquivo {name}_items.json, sem re-rodar o grafo. Para iterar a análise de
    erro offline.
    """
    items = json.loads(Path(items_path).read_text(encoding="utf-8"))
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        key = item.get("stress_category") or "representative"
        groups.setdefault(key, []).append(item)

    out: dict[str, dict[str, float]] = {}
    for key, rows in sorted(groups.items()):
        n = len(rows)
        out[key] = {
            "n": n,
            "medicine_accuracy": sum(r["medicine_correct"] for r in rows) / n,
            "section_accuracy": sum(r["section_correct"] for r in rows) / n,
            "verdict_accuracy": sum(r["verdict_correct"] for r in rows) / n,
            "semantic_recall": sum((r["semantic_recall"] or 0) for r in rows) / n,
            "semantic_hit_at_1": sum((r["semantic_hit_at_1"] or 0) for r in rows) / n,
        }
    return out


_VERDICT_LABELS = ("confirmed", "refuted", "inconclusive")
_NO_ANSWER = "sem_resposta"


def _norm_verdict(verdict: str) -> str:
    """
    Mapeia o veredito previsto para o espaço de colunas da matriz. Um veredito
    ausente ("" — o pipeline não chegou ao verify_claim) vira a classe própria
    `sem_resposta`: dobrá-lo em `inconclusive` creditava como acerto uma falha de
    pipeline e fazia a diagonal da matriz divergir de `verdict_accuracy`.
    """
    return verdict if verdict in _VERDICT_LABELS else _NO_ANSWER


def _verdict_confusion_matrix(
    pairs: list[tuple[str, str]],
) -> dict[str, dict[str, int]]:
    """
    Matriz 3x4 do veredito do sistema: linhas = esperado
    (confirmed/refuted/inconclusive), colunas = previsto, mais a coluna
    `sem_resposta` para os itens em que o veredito nunca foi produzido.

    Uma questão é exatamente uma amostra, então é a matriz direta sobre o dataset
    — sem ambiguidade micro/macro. Com a coluna extra a diagonal volta a
    reproduzir `verdict_accuracy`.
    """
    matrix = {
        expected: {predicted: 0 for predicted in (*_VERDICT_LABELS, _NO_ANSWER)}
        for expected in _VERDICT_LABELS
    }
    for expected, predicted in pairs:
        row = expected if expected in _VERDICT_LABELS else "inconclusive"
        matrix[row][_norm_verdict(predicted)] += 1
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
