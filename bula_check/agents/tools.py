import json
import os
import re
import unicodedata

from langchain_core.messages import HumanMessage
from langchain_core.messages import SystemMessage
from langchain_core.tools import tool
from openai import OpenAI
from typing_extensions import TypedDict

from bula_check.agents.llm import build_llm
from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import LLMProvider
from bula_check.agents.protocol import ParsedQuery
from bula_check.constants import LANGUAGES
from bula_check.decs import DeCSC
from bula_check.protocol import Section


class ParseQueryInput(TypedDict):
    """
    Parameters
    ----------
    user_message : str
        Mensagem original do usuário
    llm_provider
        Provedor LLM a usar
    llm_model
        Modelo LLM
    """

    user_message: str
    llm_provider: str
    llm_model: str


_PROMPT_PARSE_SYSTEM = """
Você é um especialista em farmacologia. Sua tarefa é analisar a mensagem do usuário
e extrair informações estruturadas sobre medicamentos mencionados.

Retorne APENAS um JSON válido com esta estrutura:
{
  "medicine_name": "<nome do medicamento mencionado>",
  "active_ingredient": "<princípio ativo se mencionado, ou null>",
  "sections": ["<seções relevantes da bula>"],
  "expanded_keywords": ["<palavras-chave relevantes para busca>"],
  "claim_type": "<'question' ou 'claim'>",
  "original_query": "<query original>"
}

Seções disponíveis da bula:
- indications
- how_it_works
- contraindications
- warnings_and_precautions
- storage
- dosage_and_administration
- missed_dose
- adverse_reactions
- overdose

Regras:
1. Selecione APENAS as seções mais relevantes para a pergunta/alegação (máximo 3)
2. expanded_keywords deve conter variações e sinônimos em português (ex: "náusea", "enjoo", "vômito")
3. Se não houver princípio ativo explícito, retorne null
4. claim_type é 'question' se for pergunta, 'claim' se for afirmação
"""
_VALID_SECTIONS = {s.value for s in Section}

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _strip_reasoning(raw: str) -> str:
    """
    Remove o raciocínio dos modelos pensantes que o devolvem como <think>...
    </think> dentro do content (varia por provedor e versão do cliente). Se a
    resposta foi cortada no meio do raciocínio, descarta a cauda — não há JSON
    ali. Se só o fechamento sobrou, fica com o que vem depois dele.
    """
    text = _THINK_RE.sub(" ", raw)
    lowered = text.lower()
    if "</think>" in lowered:
        text = text[lowered.rindex("</think>") + len("</think>") :]
        lowered = text.lower()
    if "<think>" in lowered:
        text = text[: lowered.index("<think>")]
    return text


def _extract_json(raw: str) -> dict:
    """
    Extrai o primeiro objeto JSON de uma resposta de LLM, tolerando raciocínio,
    cerca de crase e prosa em volta. Levanta ValueError explícito quando não há
    JSON ou quando o objeto ficou incompleto (resposta truncada), em vez do
    JSONDecodeError opaco.
    """
    text = _strip_reasoning(raw).replace("```json", " ").replace("```", " ")

    start = text.find("{")
    if start < 0:
        raise ValueError("resposta sem JSON")

    depth = 0
    in_string = False
    escaped = False
    for i, char in enumerate(text[start:], start):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return json.loads(text[start : i + 1])

    raise ValueError("JSON incompleto (resposta truncada?)")


@tool("parse_medicine_query")
def parse_medicine_query(
    user_message: str,
    llm_provider: LLMProvider = LLMProvider.openai,
    llm_model: str = "gpt-4o-mini",
) -> ParsedQuery:
    """
    Analisa a mensagem do usuário e extrai: nome do medicamento, princípio ativo,
    seções relevantes da bula e palavras-chave expandidas.
    """

    provider = (
        LLMProvider(llm_provider) if isinstance(llm_provider, str) else llm_provider
    )
    cfg: BulaCheckConfig = {
        **DEFAULT_CONFIG,
        "llm_provider": provider,
        "llm_model": llm_model,
    }
    # reasoning=False: extração estruturada não se beneficia do raciocínio, e
    # nos modelos pensantes ele consumia o orçamento de tokens antes do JSON.
    llm = build_llm(cfg, reasoning=False)

    response = llm.invoke(
        [
            SystemMessage(content=_PROMPT_PARSE_SYSTEM),
            HumanMessage(content=user_message),
        ]
    )

    parsed = _extract_json(str(response.content))

    # Sanitiza seções
    sections = [s for s in parsed.get("sections", []) if s in _VALID_SECTIONS]
    if not sections:
        # TODO é isso?
        sections = list(_VALID_SECTIONS)  # fallback: todas as seções

    # TODO precisa de claim_type?
    return ParsedQuery(
        medicine_name=parsed.get("medicine_name", ""),
        active_ingredient=parsed.get("active_ingredient"),
        sections=sections,
        expanded_keywords=parsed.get("expanded_keywords", [user_message]),
        claim_type=parsed.get("claim_type", "question"),
        original_query=user_message,
    )


class DeCSExpandInput(TypedDict):
    """
    keywords : list[str]
        Lista de palavras-chave para expandir
    language : str
        Idioma para busca DeCS
    """

    keywords: list[str]
    language: LANGUAGES


@tool("expand_keywords_decs")
def expand_keywords_decs(
    keywords: list[str],
    language: str = "portuguese",
) -> list[str]:
    """
    Expande uma lista de palavras-chave usando o vocabulário controlado DeCS
    (Descritores em Ciências da Saúde). Retorna os termos originais enriquecidos
    com sinônimos e termos relacionados da ontologia médica.
    """

    api_key = os.environ.get("DECS_API_KEY")
    if not api_key:
        return keywords
    client = DeCSC(api_key=api_key)
    expanded = list(keywords)

    def _norm(term: str) -> str:
        # lowercase + sem acento, para comparar/deduplicar de forma robusta.
        stripped = unicodedata.normalize("NFKD", term).encode("ascii", "ignore")
        return stripped.decode().lower().strip()

    def _append_term(term: str) -> None:
        if term and _norm(term) not in [_norm(e) for e in expanded]:
            expanded.append(term)

    def _pt_terms(record: dict) -> list[str]:
        # descritores em PT + sinônimos (já em PT), para checar relevância.
        descriptors = [
            d.get("descriptor", "")
            for d in record.get("descriptor_list", [])
            if d.get("attr", {}).get("lang", "").startswith("pt")
        ]
        synonyms = [s.get("synonym", "") for s in record.get("synonym_list", [])]
        return descriptors + synonyms

    for kw in keywords:
        try:
            result = client.search_by_words(kw, lang=language)
            nkw = _norm(kw)
            # A resposta vem como {"objects": [{"decsws_response": {...}}, ...]},
            # cada objeto com seu próprio record_list.record (dict ou lista).
            for obj in result.get("objects", []):
                record_list = obj.get("decsws_response", {}).get("record_list", {})
                records = record_list.get("record", [])
                if isinstance(records, dict):
                    records = [records]
                for record in records:
                    # search_by_words faz match em qualquer descritor que contenha
                    # a palavra em algum sinônimo (mesmo dentro de termos compostos
                    # como "Enjoo de Viagem"), trazendo conceitos não relacionados.
                    # Só expandimos a partir do descritor onde a keyword bate como
                    # termo INTEIRO — esse é o conceito realmente sobre ela.
                    if nkw not in [_norm(t) for t in _pt_terms(record)]:
                        continue
                    # Descritores: só os em português, os demais idiomas só
                    # adicionam ruído à busca lexical em PT.
                    # for desc in record.get("descriptor_list", []):
                    #     if desc.get("attr", {}).get("lang", "").startswith("pt"):
                    #         _append_term(desc.get("descriptor", ""))
                    # Sinônimos (já em português).
                    for syn in record.get("synonym_list", []):
                        _append_term(syn.get("synonym", ""))
        except Exception:
            continue

    return expanded


class EmbeddingInput(TypedDict):
    """
    Parameters
    ----------
    text : str
        "Texto para gerar embedding"
    """

    text: str


@tool("get_query_embedding")
def get_query_embedding(text: str) -> list[float] | None:
    """
    Gera o embedding vetorial de um texto usando OpenAI text-embedding-3-small.
    Retorna None se a API key não estiver configurada.
    """

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None

    try:
        client = OpenAI(api_key=api_key)
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=text,
        )
        return response.data[0].embedding
    except Exception:
        return None
