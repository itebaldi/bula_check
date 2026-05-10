from enum import Enum
from pathlib import Path
from typing import Annotated
from typing import Literal

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from bula_check.constants import LANGUAGES
from bula_check.protocol import Section


class LLMProvider(str, Enum):
    openai = "openai"
    anthropic = "anthropic"
    ollama = "ollama"


class BulaCheckConfig(TypedDict):
    """Configuração principal do BulaCheck."""

    # Banco de dados
    bulagratis_db_path: Path
    anvisa_db_path: Path

    # LLM
    llm_provider: LLMProvider
    llm_model: str
    llm_temperature: float

    # Retrieval
    top_k_chunks: int
    top_k_medicines: int
    similarity_candidates: int  # sugestões quando medicamento não é encontrado
    lexical_weight: float  # peso busca lexical no score híbrido
    semantic_weight: float  # peso busca semântica no score híbrido

    # Geração
    max_response_words: int

    # DeCS
    decs_lang: LANGUAGES


DEFAULT_CONFIG: BulaCheckConfig = {
    "bulagratis_db_path": Path("bulas_gratis.db"),
    "anvisa_db_path": Path("bulas_anvisa.db"),
    "llm_provider": LLMProvider.openai,
    "llm_model": "gpt-4o-mini",
    "llm_temperature": 0.0,
    "top_k_chunks": 8,
    "top_k_medicines": 5,
    "similarity_candidates": 3,
    "lexical_weight": 0.4,
    "semantic_weight": 0.6,
    "max_response_words": 400,
    "decs_lang": "portuguese",
}


class ParsedQuery(TypedDict):
    """Resultado do parsing da mensagem do usuário."""

    medicine_name: str
    active_ingredient: str | None
    sections: list[str]
    expanded_keywords: list[str]
    claim_type: Literal["question", "claim"]
    original_query: str


class ChunksDict(TypedDict):
    id: str
    medicine_id: str
    medicine_name: str
    section: Section
    paragraph_idx: int
    chunk_idx: int
    text: str
    embedding: list[float]


class MedicinesDict(TypedDict):
    id: str
    name: str
    processed_name: str
    active_ingredient: list[str] | None
    processed_active_ingredient: list[str] | None
    source: Literal["anvisa", "bula_gratis"]
    url: str
    registration_number: int | None
    therapeutic_classes: str | None
    company_name: str
    processed_company_name: str
    cnpj: str | None


class MedicineCandidate(TypedDict):
    """Medicamento candidato encontrado na busca."""

    medicine: MedicinesDict
    score: float


class RetrievedChunk(TypedDict):
    """Medicamento candidato encontrado na busca."""

    chunk: ChunksDict
    score: float


class VerificationResult(TypedDict):
    """Resultado da verificação da alegação."""

    verdict: Literal["confirmed", "refuted", "inconclusive"]
    confidence: float
    explanation: str
    supporting_chunks: list[RetrievedChunk]
    response_text: str


class BulaCheckState(TypedDict):
    """Estado completo do grafo LangGraph."""

    # mensagens do usuário
    messages: Annotated[list[BaseMessage], add_messages]

    parsed_query: ParsedQuery | None

    medicine_candidates: list[MedicineCandidate]
    selected_medicine: MedicineCandidate | None
    similar_medicines: list[MedicineCandidate]  # fallback

    decs_keywords: list[str]

    retrieved_chunks: list[RetrievedChunk]

    verification_result: VerificationResult | None

    # para controle de fluxo
    search_attempted_bulagratis: bool
    search_attempted_anvisa: bool
    awaiting_user_confirmation: bool  # aguardando usuário confirmar sugestão
    suggested_medicine_name: str | None

    config: BulaCheckConfig
