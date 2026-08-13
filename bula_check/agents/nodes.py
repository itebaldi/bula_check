import re
import sqlite3
from pathlib import Path
from typing import Literal
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
from bula_check.protocol import pt_section_label


def _parse_query_with_retry(
    user_text: str,
    config: BulaCheckConfig,
    attempts: int = 2,
) -> tuple[ParsedQuery, str | None]:
    """
    Extrai a query estruturada, com uma retentativa antes de desistir.

    Em caso de falha devolve um ParsedQuery vazio junto do motivo. O fallback
    antigo usava a mensagem inteira como nome do medicamento: além de nunca
    resolver na busca (mais da metade dos tokens teria que casar em
    _filter_low_match), ele inflava as métricas — `_medicine_correct` casa por
    substring, então a query inteira "continha" o nome esperado, e a lista cheia
    de seções creditava `section_correct` de graça.
    """
    error: Exception | None = None
    for _ in range(attempts):
        try:
            parsed: ParsedQuery = parse_medicine_query.invoke(
                {
                    "user_message": user_text,
                    "llm_provider": config["llm_provider"].value,
                    "llm_model": config["llm_model"],
                }
            )
            return parsed, None
        except Exception as retry_error:  # noqa: PERF203
            error = retry_error

    fallback = ParsedQuery(
        medicine_name="",
        active_ingredient=None,
        sections=[],
        expanded_keywords=[user_text],
        claim_type="question",
        original_query=user_text,
    )
    return fallback, f"{type(error).__name__}: {error}"


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

    parsed, parse_error = _parse_query_with_retry(str(user_text), config)

    return {
        "parsed_query": parsed,
        "parse_error": parse_error,
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
        # Fuzzy: busca similares. Se a similaridade do melhor candidato for alta
        # o bastante (ex: erro de digitação no nome), AUTO-SELECIONA; senão,
        # devolve como sugestão para o usuário confirmar.
        bg_conn2 = _open_db(config["bulagratis_db_path"])
        try:
            similars = find_similar_medicines(
                bg_conn2,
                parsed["medicine_name"],
                limit=config["similarity_candidates"],
            )
        finally:
            bg_conn2.close()

        threshold = config.get("fuzzy_autoselect_threshold", 0.75)
        if similars and similars[0]["score"] >= threshold:
            return {
                "medicine_candidates": similars,
                "selected_medicine": similars[0],
                "search_attempted_bulagratis": True,
                "search_attempted_anvisa": anvisa_conn is not None,
            }

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
        # Embeda só a pergunta/alegação: a cauda de keywords (de um set, ordem
        # aleatória, + nome do remédio + DeCS) diluía o sinal semântico. As
        # keywords seguem alimentando o lado lexical/tfidf em hybrid_chunk_search.
        query_text = parsed["original_query"]
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
2. ANTES de classificar, decida: os trechos AFIRMAM a alegação, AFIRMAM o
   CONTRÁRIO dela, ou não a sustentam? Mencionar o mesmo tema NÃO é confirmar a
   alegação específica.
3. Classifique com base nessa decisão:
   - CONFIRMADA: algum trecho AFIRMA a alegação.
   - REFUTADA: algum trecho AFIRMA o CONTRÁRIO (contradiz explicitamente).
   - INCONCLUSIVA: os trechos tocam no tema mas não sustentam nem contradizem a
     alegação, OU não tratam do assunto. NUNCA use REFUTADA por simples AUSÊNCIA
     de informação nos trechos.
4. Cite os trechos relevantes entre aspas duplas e indique a seção de cada um
5. Responda em português, de forma clara e acessível ao paciente
6. Máximo de {max_words} palavras na resposta final
7. Formato da resposta:

**Veredicto:** [CONFIRMADA / REFUTADA / INCONCLUSIVA]

**Análise:**
[Sua análise citando trechos da bula]

**Trechos relevantes da bula de {medicine_name}:**
- [Seção]: "trecho citado"
"""


_VERIFY_PROMPT_SYSTEM_CLOSED_BOOK = """
Você é um farmacêutico clínico experiente com conhecimento amplo sobre medicamentos,
princípios ativos, indicações, contraindicações, reações adversas e interações.
Você NÃO tem acesso à bula específica deste medicamento — responda baseado no seu
conhecimento farmacológico geral.

REGRAS:
1. Use seu conhecimento médico/farmacológico geral sobre o medicamento {medicine_name}
2. Posicione-se com confiança quando o conhecimento for sólido sobre a classe terapêutica
3. Classifique o resultado como: CONFIRMADA, REFUTADA ou INCONCLUSIVA
4. INCONCLUSIVA apenas quando realmente não houver informação confiável — não use
   por excesso de cautela. Se você sabe o suficiente para ter uma opinião informada,
   classifique como CONFIRMADA ou REFUTADA
5. Responda em português, de forma clara e acessível ao paciente
6. Máximo de {max_words} palavras na resposta final
7. Indique sempre que se trata de orientação geral, não substitui consulta médica
8. Formato da resposta:

**Veredicto:** [CONFIRMADA / REFUTADA / INCONCLUSIVA]

**Análise:**
[Sua análise com base em conhecimento farmacológico geral sobre {medicine_name}]
"""


_VERDICT_LINE_RE = re.compile(
    r"vered\w*[^A-Za-z]*(CONFIRMADA|REFUTADA|INCONCLUSIVA)",
    re.IGNORECASE,
)


def _parse_verdict(
    response_text: str,
) -> Literal["confirmed", "refuted", "inconclusive"]:
    """
    Extrai o veredito preferindo a linha '**Veredicto:**'. Só cai na varredura do
    texto todo (comportamento antigo, menos robusto) se a linha não existir.
    """
    match = _VERDICT_LINE_RE.search(response_text)
    label = match.group(1).upper() if match else response_text.upper()
    if "CONFIRMADA" in label:
        return "confirmed"
    if "REFUTADA" in label:
        return "refuted"
    return "inconclusive"


def node_verify_claim(state: BulaCheckState) -> dict:
    """Verifica a alegação/pergunta com base nos chunks recuperados."""
    config: BulaCheckConfig = state["config"]
    parsed: ParsedQuery | None = state.get("parsed_query")
    medicine: MedicineCandidate | None = state.get("selected_medicine")
    chunks: list[RetrievedChunk] = state.get("retrieved_chunks", [])

    if not parsed:
        return {}

    is_closed_book = not config.get("with_rag", True)
    # o nome fica vazio quando o parse falhou (ver _parse_query_with_retry)
    medicine_name = (
        medicine["medicine"]["name"] if medicine else parsed["medicine_name"]
    ) or "esse medicamento"

    # Sem medicamento resolvido (só acontece em modo RAG) o veredito é um
    # inconclusive explícito: o grafo não pode terminar sem verificação, senão o
    # item vira `predicted_verdict: ""` no eval.
    if not is_closed_book and not medicine:
        not_found_msg = (
            f"Não encontrei **{medicine_name}** na base de bulas, então não "
            "consigo verificar sua pergunta. Consulte um profissional de saúde."
        )
        result = VerificationResult(
            verdict="inconclusive",
            confidence=0.0,
            explanation=not_found_msg,
            supporting_chunks=[],
            response_text=not_found_msg,
        )
        # Se o suggest_similar já perguntou "você quis dizer X?", o veredito fica
        # só no estado (para o eval) e a conversa segue esperando a confirmação.
        if state.get("awaiting_user_confirmation"):
            return {"verification_result": result}
        return {
            "verification_result": result,
            "messages": [AIMessage(content=not_found_msg)],
        }

    # Mensagem "não encontrei" só em modo RAG com retrieval vazio
    if not is_closed_book and not chunks:
        no_info_msg = (
            f"Não encontrei informações suficientes na bula de **{medicine_name}** "
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

    system_template = (
        _VERIFY_PROMPT_SYSTEM_CLOSED_BOOK if is_closed_book else _VERIFY_PROMPT_SYSTEM
    )
    system_prompt = system_template.format(
        max_words=config["max_response_words"],
        medicine_name=medicine_name,
    )

    claim_label = "Pergunta" if parsed["claim_type"] == "question" else "Alegação"

    if is_closed_book:
        user_prompt = (
            f"Medicamento: {medicine_name}\n"
            f"{claim_label}: {parsed['original_query']}"
        )
    else:
        context_parts = []
        for chunk in chunks[: config["top_k_chunks"]]:
            label = pt_section_label(chunk["chunk"]["section"])
            context_parts.append(f"[{label}]\n{chunk['chunk']['text']}")
        context = "\n\n---\n\n".join(context_parts)

        user_prompt = (
            f"Medicamento: {medicine_name}\n"
            f"{claim_label}: {parsed['original_query']}\n\n"
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
    verdict = _parse_verdict(response_text)

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
        name = (parsed["medicine_name"] if parsed else "") or "esse medicamento"
        msg = (
            f"Não encontrei **{name}** na base de dados. "
            "Verifique o nome do medicamento e tente novamente."
        )
        return {"messages": [AIMessage(content=msg)]}

    names = ", ".join(f"**{m['medicine']['name']}**" for m in similars)
    name = (parsed["medicine_name"] if parsed else "") or "esse medicamento"
    msg = (
        f"Não encontrei **{name}** diretamente. "
        f"Você quis dizer: {names}? (responda 'sim' para confirmar o primeiro resultado)"
    )

    return {
        "awaiting_user_confirmation": True,
        "messages": [AIMessage(content=msg)],
    }
