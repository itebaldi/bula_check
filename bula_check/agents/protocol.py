from enum import Enum
from pathlib import Path
from typing import Annotated
from typing import Literal

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel
from typing_extensions import TypedDict

from bula_check.constants import LANGUAGES
from bula_check.protocol import Chunks
from bula_check.protocol import Medicines


class LLMProvider(str, Enum):
    openai = "openai"
    anthropic = "anthropic"
    ollama = "ollama"


class BulaCheckConfig(BaseModel):
    """Configuração principal do BulaCheck."""

    # Banco de dados
    bulagratis_db_path: Path = Path("bulas_gratis.db")
    anvisa_db_path: Path = Path("bulas_anvisa.db")

    # LLM
    llm_provider: LLMProvider = LLMProvider.openai
    llm_model: str = "gpt-4o-mini"
    llm_temperature: float = 0.0

    # Retrieval
    top_k_chunks: int = 8
    top_k_medicines: int = 5
    similarity_candidates: int = 3  # sugestões quando medicamento não é encontrado
    lexical_weight: float = 0.4  # peso busca lexical no score híbrido
    semantic_weight: float = 0.6  # peso busca semântica no score híbrido

    # Geração
    max_response_words: int = 400

    # TODO como imputar essas chaves dinamicamente?

    # DeCS
    decs_api_key: str | None = None
    decs_lang: LANGUAGES = "portuguese"

    # OBM
    obm_token: str | None = None

    class Config:
        arbitrary_types_allowed = True


class ParsedQuery(TypedDict):
    """Resultado do parsing da mensagem do usuário."""

    medicine_name: str
    active_ingredient: str | None
    sections: list[str]
    expanded_keywords: list[str]
    claim_type: Literal["question", "claim"]
    original_query: str


class MedicineCandidate(BaseModel):
    """Medicamento candidato encontrado na busca."""

    medicine: Medicines
    score: float


class RetrievedChunk(BaseModel):
    """Medicamento candidato encontrado na busca."""

    chunk: Chunks
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
