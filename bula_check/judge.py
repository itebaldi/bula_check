"""Validação do dataset de avaliação por LLM-as-judge (painel de juízes).

Cada pergunta gold é julgada por um painel de N modelos independentes. Cada juiz
recebe a query, o medicamento esperado e o texto COMPLETO das seções esperadas
(do banco — nunca truncado) e julga, de forma independente (sem ver a
justificativa nem o veredito gold), três dimensões:

  - medicine_ok  — a query se refere ao medicamento esperado?
  - judge_verdict — confirmed/refuted/inconclusive a partir do texto da seção.
  - evidence_ok  — o chunk citado é a evidência certa para esse veredito?

O script deriva verdict_ok (= judge_verdict == expected_verdict), tira o voto
majoritário por dimensão e grava o bloco `validation` no MESMO schema do
review.py (o farmacêutico preenche o mesmo slot depois). Reporta concordância
inter-juízes (acordo par-a-par + Fleiss' kappa).

Uso:
    python -m bula_check.judge judge \\
        --dataset inputs/evaluation/dataset.json \\
        --sample 50 --stratified \\
        --models openai:gpt-4o,google:gemini-1.5-pro,groq:llama-3.3-70b-versatile \\
        --out-report outputs/validation/pilot.json \\
        --out-dataset outputs/validation/dataset_judged.json

    # (futuro) compara o parecer do juiz com o do farmacêutico
    python -m bula_check.judge compare \\
        --judged outputs/validation/dataset_judged.json \\
        --human inputs/evaluation/dataset.json
"""

import argparse
import json
import random
import sqlite3
import threading
import time
from collections import Counter
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from itertools import combinations
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage
from langchain_core.messages import SystemMessage

from bula_check.agents.nodes import _open_db
from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.agents.protocol import LLMProvider
from bula_check.protocol import pt_section_label
from bula_check.review import _flag_state

# Painel padrão: 3 modelos OpenAI distintos (pagos, escalam para as 790 sem cota
# diária). Limitação assumida: mesmo laboratório → menor independência. Para
# independência entre laboratórios, trocar um por google:gemini-2.5-flash (tier
# pago) — os free-tiers de Google/Groq estouram a cota diária em poucas dezenas
# de chamadas com prompts longos.
DEFAULT_PANEL = "openai:gpt-4o,openai:gpt-4.1,openai:gpt-4.1-mini"
VERDICTS = ("confirmed", "refuted", "inconclusive")
OK_DIMENSIONS = ("verdict_ok", "medicine_ok", "evidence_ok")

_SIM = {"sim", "s", "yes", "y", "true", "1", "ok", "correto"}
_NAO = {"nao", "não", "n", "no", "false", "0", "incorreto", "errado"}


_JUDGE_SYSTEM = """Você é um farmacêutico avaliador validando uma pergunta \
gold-standard de um benchmark que verifica alegações de pacientes contra a bula \
de um medicamento. Julgue de forma INDEPENDENTE e rigorosa, usando SOMENTE o \
texto da bula fornecido. Não invente informação que não esteja no texto.

Você recebe: a PERGUNTA do paciente, o MEDICAMENTO esperado e o TEXTO COMPLETO \
das seções relevantes da bula (cada trecho marcado com [seção | id]). Um dos \
trechos é apontado como a EVIDÊNCIA citada pelo gabarito.

Avalie três dimensões e responda APENAS com um JSON válido:
{
  "medicine_ok": "sim" | "não" | "incerto",
  "judge_verdict": "confirmed" | "refuted" | "inconclusive",
  "evidence_ok": "sim" | "não" | "incerto",
  "comments": "uma frase curta citando o trecho decisivo"
}

Critérios:
- medicine_ok: a pergunta se refere ao medicamento esperado (pela marca OU pelo \
princípio ativo)? "não" se for claramente outro fármaco; "incerto" se ambígua.
- judge_verdict: interprete a pergunta como uma alegação e classifique usando \
só o texto: "confirmed" se o texto AFIRMA a alegação; "refuted" se o texto \
afirma o OPOSTO; "inconclusive" se o texto NÃO trata do tópico (silêncio). \
Mencionar o tema sem afirmar/negar = inconclusive. Nunca refute por mera ausência.
- evidence_ok: o trecho citado (id apontado) é a evidência correta para o seu \
veredito? Para confirmed/refuted ele deve conter a frase que afirma/contradiz. \
Para inconclusive, ele deve pertencer a uma seção que de fato não trata o tópico. \
"não" se a evidência real está em outro trecho ou o citado é irrelevante."""


def build_judge(spec: str) -> tuple[str, BaseChatModel]:
    """Instancia um juiz (temperatura 0) a partir de "provider:model".

    Diferente de build_llm, força temperatura 0 (o build_llm descarta
    temperatura quando falsy, e 0 é falsy).

    Returns
    -------
    tuple[str, BaseChatModel]
        Rótulo legível e o modelo LangChain.
    """
    provider_name, _, model = spec.partition(":")
    provider = LLMProvider(provider_name)
    label = f"{provider_name}:{model}"

    if provider == LLMProvider.openai:
        from langchain_openai import ChatOpenAI

        return label, ChatOpenAI(model=model, temperature=0)
    if provider == LLMProvider.google:
        from langchain_google_genai import ChatGoogleGenerativeAI

        return label, ChatGoogleGenerativeAI(model=model, temperature=0)
    if provider == LLMProvider.groq:
        from langchain_groq import ChatGroq

        return label, ChatGroq(model=model, temperature=0)  # type: ignore
    if provider == LLMProvider.anthropic:
        from langchain_anthropic import ChatAnthropic

        return label, ChatAnthropic(model=model, temperature=0)  # type: ignore
    if provider == LLMProvider.ollama:
        from langchain_ollama import ChatOllama

        return label, ChatOllama(model=model, temperature=0)
    raise ValueError(f"Provedor desconhecido: {provider_name}")


def fetch_section_context(
    conn: sqlite3.Connection,
    chunk_ids: list[str],
    sections: list[str],
) -> tuple[str, str | None]:
    """Monta o texto COMPLETO das seções esperadas do medicamento do chunk citado.

    Pega o medicine_id a partir do primeiro chunk citado e busca todos os chunks
    daquele medicamento nas seções esperadas (texto integral, sem truncar). Isso
    dá ao juiz o contexto necessário — em especial para o veredito inconclusive,
    que exige confirmar a AUSÊNCIA do tópico na seção inteira.

    Returns
    -------
    tuple[str, str | None]
        (texto formatado com cabeçalhos [seção | id], id do chunk citado). O id
        citado é None se o chunk não existir no banco.
    """
    conn.row_factory = sqlite3.Row
    if not chunk_ids:
        return "", None
    cited_id = chunk_ids[0]
    row = conn.execute(
        "SELECT medicine_id FROM chunks WHERE id = ?", (cited_id,)
    ).fetchone()
    if row is None:
        return "", None
    medicine_id = row["medicine_id"]

    placeholders = ",".join("?" for _ in sections)
    rows = conn.execute(
        f"""
        SELECT id, section, text FROM chunks
        WHERE medicine_id = ? AND section IN ({placeholders})
        ORDER BY section, paragraph_idx, chunk_idx
        """,
        [medicine_id, *sections],
    ).fetchall()

    blocks = []
    for r in rows:
        cited = "  <<< TRECHO CITADO PELO GABARITO" if r["id"] == cited_id else ""
        header = f"[{pt_section_label(r['section'])} | {r['id']}]{cited}"
        blocks.append(f"{header}\n{r['text']}")
    return "\n\n".join(blocks), cited_id


def _build_user_prompt(item: dict, context: str) -> str:
    brand = f" (marca citada: {item['medicine_brand']})" if item.get("medicine_brand") else ""
    sections_pt = ", ".join(pt_section_label(s) for s in item["expected_sections"])
    return (
        f"PERGUNTA DO PACIENTE:\n{item['query']}\n\n"
        f"MEDICAMENTO ESPERADO: {item['expected_medicine']}{brand}\n"
        f"SEÇÃO(ÕES) ESPERADA(S): {sections_pt}\n\n"
        f"TEXTO DA BULA (seções relevantes, completo):\n{context}"
    )


def _parse_json(raw: str) -> dict:
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def _norm_flag(value: Any) -> str:
    token = str(value or "").strip().lower()
    if token in _NAO:
        return "não"
    if token in _SIM:
        return "sim"
    return "incerto"


def _norm_verdict(value: Any) -> str:
    token = str(value or "").strip().lower()
    for verdict in VERDICTS:
        if verdict in token:
            return verdict
    return "inconclusive"


_TRANSIENT = ("429", "resource_exhausted", "rate", "timeout", "overloaded", "503")

# Espaçamento mínimo entre chamadas POR PROVEDOR (respeita o RPM da conta;
# free-tiers de Groq/Google limitam agressivamente). O lock é por provedor, então
# múltiplos modelos do mesmo provedor compartilham o mesmo ritmo.
_PROVIDER_INTERVAL = {"google": 4.0, "groq": 2.0, "openai": 0.2}
_LOCKS: dict[str, threading.Lock] = defaultdict(threading.Lock)
_LAST_CALL: dict[str, float] = {}


def _throttle(label: str) -> None:
    provider = label.split(":", 1)[0]
    interval = _PROVIDER_INTERVAL.get(provider, 0.0)
    if interval <= 0:
        return
    with _LOCKS[provider]:
        wait = interval - (time.monotonic() - _LAST_CALL.get(provider, 0.0))
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL[provider] = time.monotonic()


def _invoke_with_retry(
    model: BaseChatModel,
    messages: list,
    label: str,
    attempts: int = 5,
) -> str:
    """Invoca o modelo com backoff exponencial em erros transitórios (rate limit)."""
    for i in range(attempts):
        _throttle(label)
        try:
            return str(model.invoke(messages).content)
        except Exception as error:  # noqa: PERF203
            transient = any(t in str(error).lower() for t in _TRANSIENT)
            if not transient or i == attempts - 1:
                raise
            time.sleep(2**i)
    raise RuntimeError("unreachable")


def run_judge(
    label: str,
    model: BaseChatModel,
    item: dict,
    context: str,
) -> dict:
    """Roda um juiz numa pergunta e devolve as dimensões normalizadas.

    Em caso de erro (modelo/chave/parse), devolve todas as dimensões como
    "incerto" e registra o erro em `_error`.
    """
    user_prompt = _build_user_prompt(item, context)
    try:
        content = _invoke_with_retry(
            model,
            [SystemMessage(content=_JUDGE_SYSTEM), HumanMessage(content=user_prompt)],
            label,
        )
        parsed = _parse_json(content)
        judge_verdict = _norm_verdict(parsed.get("judge_verdict"))
        return {
            "_model": label,
            "medicine_ok": _norm_flag(parsed.get("medicine_ok")),
            "judge_verdict": judge_verdict,
            "verdict_ok": "sim" if judge_verdict == item["expected_verdict"] else "não",
            "evidence_ok": _norm_flag(parsed.get("evidence_ok")),
            "comments": str(parsed.get("comments", "")).strip(),
        }
    except Exception as error:  # modelo indisponível, JSON inválido etc.
        return {
            "_model": label,
            "medicine_ok": "incerto",
            "judge_verdict": "inconclusive",
            "verdict_ok": "incerto",
            "evidence_ok": "incerto",
            "comments": "",
            "_error": f"{type(error).__name__}: {error}",
        }


def _majority(values: list[str]) -> str:
    counts = Counter(values)
    top, n = counts.most_common(1)[0]
    return top if n * 2 > len(values) else "incerto"


def majority_vote(judge_outputs: list[dict], panel_label: str) -> dict:
    """Agrega os pareceres dos juízes num bloco `validation` (schema do review.py).

    Voto majoritário por dimensão; status derivado como no review.py (reprovado
    se qualquer dimensão majoritária for "não").
    """
    block = {
        dim: _majority([o[dim] for o in judge_outputs]) for dim in OK_DIMENSIONS
    }
    problems = [d for d in OK_DIMENSIONS if _flag_state(block[d]) is False]
    verdicts = [o["judge_verdict"] for o in judge_outputs]
    diverge = "" if len(set(verdicts)) == 1 else f" [vereditos: {', '.join(verdicts)}]"
    comments = " | ".join(
        f"{o['_model']}: {o['comments']}" for o in judge_outputs if o.get("comments")
    )
    block["validated_by"] = f"llm-panel:{panel_label}"
    block["comments"] = (comments + diverge).strip()
    block["status"] = "reprovado" if problems else "aprovado"
    return block


def _pairwise_agreement(rows: list[list[str]]) -> float:
    """Acordo médio par-a-par: cada linha são os rótulos dos juízes de um item."""
    scores = []
    for labels in rows:
        pairs = list(combinations(labels, 2))
        if pairs:
            scores.append(sum(a == b for a, b in pairs) / len(pairs))
    return round(sum(scores) / len(scores), 4) if scores else 0.0


def _fleiss_kappa(rows: list[list[str]], categories: list[str]) -> float:
    """Fleiss' kappa para N itens avaliados por n juízes em k categorias."""
    rows = [r for r in rows if len(r) == len(rows[0])] if rows else []
    if not rows:
        return 0.0
    n = len(rows[0])
    if n < 2:
        return 0.0
    matrix = [[row.count(c) for c in categories] for row in rows]
    big_n = len(matrix)
    p_i = [
        (sum(x * x for x in row) - n) / (n * (n - 1)) for row in matrix
    ]
    p_bar = sum(p_i) / big_n
    totals = [sum(matrix[i][j] for i in range(big_n)) for j in range(len(categories))]
    p_j = [t / (big_n * n) for t in totals]
    p_e = sum(x * x for x in p_j)
    if p_e >= 1.0:
        return 1.0
    return round((p_bar - p_e) / (1 - p_e), 4)


def agreement(
    per_item_outputs: list[list[dict]],
) -> dict:
    """Concordância inter-juízes por dimensão (só itens sem erro em nenhum juiz)."""
    clean = [
        outs for outs in per_item_outputs if not any("_error" in o for o in outs)
    ]
    result: dict[str, Any] = {"n_items_clean": len(clean)}
    dims = {
        "judge_verdict": list(VERDICTS),
        "medicine_ok": ["sim", "não", "incerto"],
        "evidence_ok": ["sim", "não", "incerto"],
    }
    for dim, cats in dims.items():
        rows = [[o[dim] for o in outs] for outs in clean]
        result[dim] = {
            "pairwise_agreement": _pairwise_agreement(rows),
            "fleiss_kappa": _fleiss_kappa(rows, cats),
        }
    return result


def stratified_sample(items: list[dict], n: int, seed: int) -> list[dict]:
    """Amostra ~n itens proporcionalmente por stress_category (determinístico)."""
    if n >= len(items):
        return items
    rng = random.Random(seed)
    groups: dict[str, list[dict]] = defaultdict(list)
    for it in items:
        groups[it.get("stress_category") or "representativa"].append(it)
    picked: list[dict] = []
    for cat, group in sorted(groups.items()):
        k = max(1, round(n * len(group) / len(items)))
        picked.extend(rng.sample(group, min(k, len(group))))
    rng.shuffle(picked)
    return picked[:n]


def judge_dataset(
    dataset_path: Path,
    models: list[str],
    out_report: Path,
    out_dataset: Path,
    sample: int | None,
    stratified: bool,
    seed: int,
    max_workers: int,
) -> dict:
    """Roda o painel sobre o dataset (ou amostra) e grava relatório + dataset julgado."""
    dataset = json.loads(dataset_path.read_text(encoding="utf-8"))
    if sample:
        items = (
            stratified_sample(dataset, sample, seed)
            if stratified
            else random.Random(seed).sample(dataset, min(sample, len(dataset)))
        )
    else:
        items = dataset

    conn = _open_db(DEFAULT_CONFIG["bulagratis_db_path"])
    contexts = {}
    for it in items:
        contexts[it["id"]] = fetch_section_context(
            conn, it["expected_chunk_ids"], it["expected_sections"]
        )
    conn.close()

    panel = [build_judge(spec) for spec in models]
    panel_label = "+".join(spec.split(":")[-1] for spec in models)

    tasks = [(it, label, model) for it in items for label, model in panel]
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(
            pool.map(
                lambda t: (t[0]["id"], run_judge(t[1], t[2], t[0], contexts[t[0]["id"]][0])),
                tasks,
            )
        )

    by_item: dict[str, list[dict]] = defaultdict(list)
    for item_id, out in results:
        by_item[item_id].append(out)

    judged = {}
    for it in items:
        block = majority_vote(by_item[it["id"]], panel_label)
        judged[it["id"]] = block

    report = _build_report(items, by_item, judged, models)
    _write_outputs(items, judged, report, out_report, out_dataset)
    return report


def _build_report(
    items: list[dict],
    by_item: dict[str, list[dict]],
    judged: dict[str, dict],
    models: list[str],
) -> dict:
    status_counts = Counter(judged[it["id"]]["status"] for it in items)
    dim_counts = {d: Counter(judged[it["id"]][d] for it in items) for d in OK_DIMENSIONS}
    by_cat: dict[str, Counter] = defaultdict(Counter)
    by_verdict: dict[str, Counter] = defaultdict(Counter)
    for it in items:
        by_cat[it.get("stress_category") or "representativa"][judged[it["id"]]["status"]] += 1
        by_verdict[it["expected_verdict"]][judged[it["id"]]["status"]] += 1

    flagged = []
    for it in items:
        block = judged[it["id"]]
        if block["status"] == "reprovado" or "incerto" in (
            block["verdict_ok"],
            block["medicine_ok"],
            block["evidence_ok"],
        ):
            flagged.append(
                {
                    "id": it["id"],
                    "query": it["query"],
                    "expected_medicine": it["expected_medicine"],
                    "expected_verdict": it["expected_verdict"],
                    "stress_category": it.get("stress_category"),
                    "verdict_ok": block["verdict_ok"],
                    "medicine_ok": block["medicine_ok"],
                    "evidence_ok": block["evidence_ok"],
                    "judge_verdicts": [o["judge_verdict"] for o in by_item[it["id"]]],
                    "comments": block["comments"],
                }
            )
    errors = [
        {"id": item_id, "model": o["_model"], "error": o["_error"]}
        for item_id, outs in by_item.items()
        for o in outs
        if "_error" in o
    ]
    return {
        "panel": models,
        "n_items": len(items),
        "overall": dict(status_counts),
        "by_dimension": {d: dict(c) for d, c in dim_counts.items()},
        "by_stress_category": {k: dict(v) for k, v in sorted(by_cat.items())},
        "by_verdict": {k: dict(v) for k, v in by_verdict.items()},
        "agreement": agreement([by_item[it["id"]] for it in items]),
        "n_flagged": len(flagged),
        "flagged": flagged,
        "n_errors": len(errors),
        "errors": errors[:50],
    }


def _write_outputs(
    items: list[dict],
    judged: dict[str, dict],
    report: dict,
    out_report: Path,
    out_dataset: Path,
) -> None:
    out_report.parent.mkdir(parents=True, exist_ok=True)
    out_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    judged_items = []
    for it in items:
        copy = dict(it)
        copy["validation"] = judged[it["id"]]
        judged_items.append(copy)
    out_dataset.write_text(
        json.dumps(judged_items, ensure_ascii=False, indent=4) + "\n", encoding="utf-8"
    )
    (out_report.with_suffix(".md")).write_text(_render_markdown(report), encoding="utf-8")

    ov = report["overall"]
    print(f"[judge] {report['n_items']} perguntas | painel {'+'.join(report['panel'])}")
    print(f"[judge] status: {ov}")
    print(f"[judge] flags (reprovado/incerto): {report['n_flagged']} | erros: {report['n_errors']}")
    print(f"[judge] relatório: {out_report} | dataset julgado: {out_dataset}")


def _render_markdown(report: dict) -> str:
    lines = ["# Validação por LLM-as-judge", ""]
    lines.append(f"- Painel: `{', '.join(report['panel'])}`")
    lines.append(f"- Perguntas: {report['n_items']}")
    lines.append(f"- Status geral: {report['overall']}")
    lines.append(f"- Flags p/ revisão humana: {report['n_flagged']}  |  erros: {report['n_errors']}")
    lines.append("")
    lines.append("## Concordância inter-juízes")
    for dim, m in report["agreement"].items():
        if isinstance(m, dict):
            lines.append(f"- {dim}: acordo par-a-par {m['pairwise_agreement']} | Fleiss κ {m['fleiss_kappa']}")
    lines.append("")
    lines.append("## Por dimensão (voto majoritário)")
    for dim, c in report["by_dimension"].items():
        lines.append(f"- {dim}: {c}")
    lines.append("")
    lines.append("## Itens sinalizados (fila do farmacêutico)")
    for f in report["flagged"][:60]:
        flags = [d for d in OK_DIMENSIONS if f[d] != "sim"]
        lines.append(f"- **{f['id']}** ({f['stress_category']}, gold={f['expected_verdict']}) "
                     f"{', '.join(f'{d}={f[d]}' for d in flags)} — {f['query']}")
        if f["comments"]:
            lines.append(f"  - {f['comments']}")
    return "\n".join(lines) + "\n"


def compare_human(judged_path: Path, human_path: Path) -> dict:
    """Concordância juiz×humano (gancho previsto p/ a validação do farmacêutico).

    Casa por id os itens que têm bloco `validation` em ambos os arquivos e
    compara status e as três dimensões *_ok.
    """
    judged = {i["id"]: i.get("validation") for i in json.loads(judged_path.read_text())}
    human = {i["id"]: i.get("validation") for i in json.loads(human_path.read_text())}
    common = [i for i in judged if judged[i] and human.get(i)]
    dims = {}
    for dim in (*OK_DIMENSIONS, "status"):
        agree = sum(
            1 for i in common if _flag_state(judged[i].get(dim)) == _flag_state(human[i].get(dim))
        ) if dim != "status" else sum(
            1 for i in common if judged[i].get("status") == human[i].get("status")
        )
        dims[dim] = round(agree / len(common), 4) if common else 0.0
    summary = {"n_common": len(common), "agreement": dims}
    print(f"[compare] {len(common)} itens em comum | acordo: {dims}")
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validação do dataset por LLM-as-judge.")
    sub = parser.add_subparsers(dest="command", required=True)

    j = sub.add_parser("judge", help="Roda o painel de juízes sobre o dataset.")
    j.add_argument("--dataset", type=Path, default=Path("inputs/evaluation/dataset.json"))
    j.add_argument("--models", type=str, default=DEFAULT_PANEL)
    j.add_argument("--sample", type=int, default=None)
    j.add_argument("--stratified", action="store_true")
    j.add_argument("--seed", type=int, default=7)
    j.add_argument("--max-workers", type=int, default=8)
    j.add_argument("--out-report", type=Path, default=Path("outputs/validation/report.json"))
    j.add_argument(
        "--out-dataset", type=Path, default=Path("outputs/validation/dataset_judged.json")
    )

    c = sub.add_parser("compare", help="Compara parecer do juiz com o do farmacêutico.")
    c.add_argument("--judged", type=Path, required=True)
    c.add_argument("--human", type=Path, required=True)

    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = _parse_args()
    if args.command == "judge":
        judge_dataset(
            dataset_path=args.dataset,
            models=[s.strip() for s in args.models.split(",") if s.strip()],
            out_report=args.out_report,
            out_dataset=args.out_dataset,
            sample=args.sample,
            stratified=args.stratified,
            seed=args.seed,
            max_workers=args.max_workers,
        )
    elif args.command == "compare":
        compare_human(args.judged, args.human)


if __name__ == "__main__":
    main()
