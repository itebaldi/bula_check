from langchain_core.language_models import BaseChatModel

from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import LLMProvider


def build_llm(config: BulaCheckConfig) -> BaseChatModel:
    """
    Instancia o modelo de linguagem conforme o provedor configurado.
    """
    provider = config.llm_provider
    model = config.llm_model
    temperature = config.llm_temperature

    if provider == LLMProvider.openai:
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=model, temperature=temperature)

    if provider == LLMProvider.anthropic:
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=model, temperature=temperature)  # type: ignore[call-arg]

    if provider == LLMProvider.ollama:
        from langchain_ollama import ChatOllama

        return ChatOllama(model=model, temperature=temperature)

    raise ValueError(f"Provedor LLM desconhecido: {provider}")
