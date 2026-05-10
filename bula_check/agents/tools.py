import json
import os

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
    llm = build_llm(cfg)

    response = llm.invoke(
        [
            SystemMessage(content=_PROMPT_PARSE_SYSTEM),
            HumanMessage(content=user_message),
        ]
    )

    raw = response.content
    # TODO melhorar tipagem e tirar esses ignores
    if "```" in raw:
        raw = raw.split("```")[1]  # type: ignore
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()  # type: ignore

    parsed = json.loads(raw)

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

    for kw in keywords:
        try:
            result = client.search_by_words(kw, lang=language)
            records = (
                result.get("record_list", {}).get("records", {}).get("record", [])
            )
            # TODO checar essa logica
            if isinstance(records, dict):
                records = [records]
            for record in records[:2]:
                for desc in record.get("descriptor_list", []):
                    term = desc.get("descriptor", "")
                    if term and term.lower() not in [e.lower() for e in expanded]:
                        expanded.append(term)
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
