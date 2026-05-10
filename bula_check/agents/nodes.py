import sqlite3
from pathlib import Path
from typing import cast

from langchain_core.messages import AIMessage
from langchain_core.messages import HumanMessage
from langchain_core.messages import HumanMessage as HM
from langchain_core.messages import SystemMessage

from bula_check.agents.llm import build_llm
from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import BulaCheckState
from bula_check.agents.protocol import MedicineCandidate
from bula_check.agents.protocol import ParsedQuery
from bula_check.agents.protocol import RetrievedChunk
from bula_check.agents.protocol import VerificationResult
from bula_check.agents.search import find_medicine_candidates
from bula_check.agents.search import find_similar_medicines
from bula_check.agents.search import hybrid_chunk_search
from bula_check.agents.tools import expand_keywords_decs
from bula_check.agents.tools import get_query_embedding
from bula_check.agents.tools import parse_medicine_query
from bula_check.protocol import Section
from bula_check.protocol import pt_section_label


def node_parse_query(state: BulaCheckState) -> dict:
    """extrai medicamento, seções e key words"""
    config: BulaCheckConfig = state["config"]
    messages = state["messages"]

    last_human = next(
        (m for m in reversed(messages) if isinstance(m, HumanMessage)), None
    )
    if last_human is None:
        return {}

    user_text = last_human.content

    # aguardando confirmação de sugestão, trata resposta do usuário
    if state.get("awaiting_user_confirmation"):
        text_lower = str(user_text).lower()
        confirmed = any(
            w in text_lower for w in ["sim", "yes", "s", "isso", "exato", "correto"]
        )
        if confirmed and state.get("similar_medicines"):
            best = state["similar_medicines"][0]
            return {
                "selected_medicine": best,
                "awaiting_user_confirmation": False,
                "parsed_query": state["parsed_query"],
            }
        else:
            return {
                "awaiting_user_confirmation": False,
                "messages": [
                    AIMessage(
                        content="Ok! Por favor, tente descrever o medicamento de outra forma ou verifique o nome."
                    )
                ],
            }

    try:
        parsed: ParsedQuery = parse_medicine_query.invoke(
            {
                "user_message": str(user_text),
                "llm_provider": config["llm_provider"].value,
                "llm_model": config["llm_model"],
            }
        )

    except Exception as error:
        # Fallback manual. #TODO revisar isso
        print(error)
        parsed = ParsedQuery(
            medicine_name=str(user_text),
            active_ingredient=None,
            sections=list({s.value for s in Section}),
            expanded_keywords=[str(user_text)],
            claim_type="question",
            original_query=str(user_text),
        )

    return {
        "parsed_query": parsed,
        "search_attempted_bulagratis": False,
        "search_attempted_anvisa": False,
        "awaiting_user_confirmation": False,
        "medicine_candidates": [],
        "selected_medicine": None,
        "retrieved_chunks": [],
        "decs_keywords": [],
        "verification_result": None,
    }


def node_expand_decs(state: BulaCheckState) -> dict:
    """Enriquece palavras-chave via DeCS."""
    config: BulaCheckConfig = state["config"]
    parsed: ParsedQuery | None = state.get("parsed_query")

    if not parsed:
        return {}

    base_keywords = parsed["expanded_keywords"] or [parsed["medicine_name"]]

    try:
        expanded = expand_keywords_decs.invoke(
            {"keywords": base_keywords, "language": config["decs_lang"]}
        )
    except Exception:
        expanded = base_keywords

    return {"decs_keywords": expanded}


def node_find_medicine(state: BulaCheckState) -> dict:
    """Busca o medicamento nos bancos SQLite."""
    config: BulaCheckConfig = state["config"]
    parsed: ParsedQuery | None = state.get("parsed_query")
    if not parsed:
        return {}

    # se já foi selecionado (por ex., confirmação de sugestão), pula
    if state.get("selected_medicine"):
        return {}

    bg_conn = _open_db(config["bulagratis_db_path"])

    if bg_conn is None:
        return {
            "messages": [
                AIMessage(
                    content="Banco de dados não encontrado. Verifique o caminho configurado."
                )
            ],
        }

    anvisa_conn = (
        _open_db(config["anvisa_db_path"])
        if not state.get("search_attempted_anvisa")
        else None
    )

    try:
        candidates = find_medicine_candidates(
            bulagratis_conn=bg_conn,
            anvisa_conn=anvisa_conn,
            name=parsed["medicine_name"],
            active_ingredient=parsed.get("active_ingredient"),
            cfg=config,
        )
    finally:
        bg_conn.close()
        if anvisa_conn:
            anvisa_conn.close()

    if not candidates:
        # tenta buscar similares para sugestão
        bg_conn2 = _open_db(config["bulagratis_db_path"])
        similars = []
        if bg_conn2:
            try:
                similars = find_similar_medicines(
                    bg_conn2,
                    parsed["medicine_name"],
                    limit=config["similarity_candidates"],
                )
            finally:
                bg_conn2.close()

        return {
            "medicine_candidates": [],
            "similar_medicines": similars,
            "search_attempted_bulagratis": True,
            "search_attempted_anvisa": anvisa_conn is not None,
        }
    # TODO revisar output
    return {
        "medicine_candidates": candidates,
        "selected_medicine": candidates[0],
        "search_attempted_bulagratis": True,
        "search_attempted_anvisa": anvisa_conn is not None,
    }


def node_fetch_chunks(state: BulaCheckState) -> dict:
    """Recupera chunks relevantes do medicamento selecionado."""
    config: BulaCheckConfig = state["config"]
    medicine: MedicineCandidate | None = state.get("selected_medicine")
    parsed: ParsedQuery | None = state.get("parsed_query")

    if not medicine or not parsed:
        return {}

    # keywords, parsed + decs
    keywords = list(
        set(
            (parsed.get("expanded_keywords") or [])
            + state.get("decs_keywords", [])
            + [parsed["medicine_name"]]
        )
    )

    # Embedding da query
    query_embedding: list[float] | None = None

    try:
        query_text = f"{parsed['original_query']} {' '.join(keywords[:5])}"
        query_embedding = get_query_embedding.invoke({"text": query_text})
    except Exception:
        pass

    bg_conn = _open_db(config["bulagratis_db_path"])
    if bg_conn is None:
        return {}

    try:
        chunks = hybrid_chunk_search(
            conn=bg_conn,
            medicine_id=medicine["medicine"]["id"],
            keywords=keywords,
            sections=parsed.get("sections"),
            query_embedding=query_embedding,
            cfg=config,
        )
    finally:
        bg_conn.close()

    return {"retrieved_chunks": chunks}


def _open_db(path: Path) -> sqlite3.Connection:
    if path.exists():
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row
        return conn
    raise Exception(f"Path does not exist {str(path)}")


_VERIFY_PROMPT_SYSTEM = """
Você é um especialista em bulas de medicamentos. Com base nos trechos fornecidos da bula,
verifique a alegação ou responda a pergunta do usuário.

REGRAS:
1. Use APENAS as informações dos trechos fornecidos da bula
2. Seja preciso e cite os trechos relevantes entre aspas duplas
3. Indique a seção de origem de cada trecho citado
4. Classifique o resultado como: CONFIRMADA, REFUTADA ou INCONCLUSIVA
5. Responda em português, de forma clara e acessível ao paciente
6. Máximo de {max_words} palavras na resposta final
7. Formato da resposta:

**Veredicto:** [CONFIRMADA / REFUTADA / INCONCLUSIVA]

**Análise:**
[Sua análise citando trechos da bula]

**Trechos relevantes da bula de {medicine_name}:**
- [Seção]: "trecho citado"
"""


def node_verify_claim(state: BulaCheckState) -> dict:
    """Verifica a alegação/pergunta com base nos chunks recuperados."""
    config: BulaCheckConfig = state["config"]
    parsed: ParsedQuery | None = state.get("parsed_query")
    medicine: MedicineCandidate | None = state.get("selected_medicine")
    chunks: list[RetrievedChunk] = state.get("retrieved_chunks", [])

    if not parsed or not medicine:
        return {}

    if not chunks:
        no_info_msg = (
            f"Não encontrei informações suficientes na bula de **{medicine['medicine']['name']}** "
            f"para verificar sua {'pergunta' if parsed['claim_type'] == 'question' else 'alegação'}. "
            "Consulte um profissional de saúde."
        )
        result = VerificationResult(
            verdict="inconclusive",
            confidence=0.0,
            explanation=no_info_msg,
            supporting_chunks=[],
            response_text=no_info_msg,
        )
        return {
            "verification_result": result,
            "messages": [AIMessage(content=no_info_msg)],
        }

    context_parts = []
    for chunk in chunks[: config["top_k_chunks"]]:
        label = pt_section_label(chunk["chunk"]["section"])
        context_parts.append(f"[{label}]\n{chunk['chunk']['text']}")
    context = "\n\n---\n\n".join(context_parts)

    system_prompt = _VERIFY_PROMPT_SYSTEM.format(
        max_words=config["max_response_words"],
        medicine_name=medicine["medicine"]["name"],
    )

    user_prompt = (
        f"Medicamento: {medicine['medicine']['name']}\n"
        f"{'Pergunta' if parsed['claim_type'] == 'question' else 'Alegação'}: {parsed['original_query']}\n\n"
        f"Trechos da bula:\n{context}"
    )

    llm = build_llm(config)

    response = llm.invoke(
        [
            SystemMessage(content=system_prompt),
            HM(content=user_prompt),
        ]
    )

    response_text = cast(str, response.content)

    verdict = "inconclusive"
    if "CONFIRMADA" in response_text.upper():
        verdict = "confirmed"
    elif "REFUTADA" in response_text.upper():
        verdict = "refuted"

    result = VerificationResult(
        verdict=verdict,
        confidence=0.8 if chunks else 0.2,
        explanation=response_text,
        supporting_chunks=chunks,
        response_text=response_text,
    )

    return {
        "verification_result": result,
        "messages": [AIMessage(content=response_text)],
    }


def node_suggest_similar(state: BulaCheckState) -> dict:
    """Sugere medicamentos similares quando o medicamento não é encontrado."""
    parsed: ParsedQuery | None = state.get("parsed_query")
    similars: list[MedicineCandidate] = state.get("similar_medicines", [])

    if not similars:
        name = parsed["medicine_name"] if parsed else "esse medicamento"
        msg = (
            f"Não encontrei **{name}** na base de dados. "
            "Verifique o nome do medicamento e tente novamente."
        )
        return {"messages": [AIMessage(content=msg)]}

    names = ", ".join(f"**{m['medicine']['name']}**" for m in similars)
    name = parsed["medicine_name"] if parsed else "esse medicamento"
    msg = (
        f"Não encontrei **{name}** diretamente. "
        f"Você quis dizer: {names}? (responda 'sim' para confirmar o primeiro resultado)"
    )

    return {
        "awaiting_user_confirmation": True,
        "messages": [AIMessage(content=msg)],
    }
