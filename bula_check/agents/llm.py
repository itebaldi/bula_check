from langchain_core.language_models import BaseChatModel

from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import LLMProvider


def build_llm(config: BulaCheckConfig) -> BaseChatModel:
    """
    Instantiate the language model according to the configured provider.

    Parameters
    ----------
    config : BulaCheckConfig
        BulaCheck configuration containing provider, model and temperature.

    Returns
    -------
    BaseChatModel
        LangChain chat model instance.

    Raises
    ------
    ValueError
        If the configured LLM provider is unknown.
    """
    provider = config["llm_provider"]
    model = config["llm_model"]
    temperature = config["llm_temperature"]

    if provider == LLMProvider.openai:
        from langchain_openai import ChatOpenAI

        if temperature:
            return ChatOpenAI(model=model, temperature=temperature)

        return ChatOpenAI(model=model)

    if provider == LLMProvider.anthropic:
        from langchain_anthropic import ChatAnthropic

        if temperature:
            return ChatAnthropic(model=model, temperature=temperature)  # type: ignore

        return ChatAnthropic(model=model)  # type: ignore

    if provider == LLMProvider.ollama:
        from langchain_ollama import ChatOllama

        # num_ctx=8192: o prompt de verificação (até 20 chunks) tem ~5-6k tokens
        # e era silenciosamente truncado no default 4096 do Ollama. num_predict
        # limita o tamanho da resposta (o max_response_words é só instrução textual).
        ollama_opts = {
            "base_url": "http://localhost:11434",
            "num_ctx": 8192,
            "num_predict": 512,
        }
        if temperature:
            return ChatOllama(model=model, temperature=temperature, **ollama_opts)

        return ChatOllama(model=model, **ollama_opts)

    if provider == LLMProvider.groq:
        from langchain_groq import ChatGroq

        if temperature:
            return ChatGroq(model=model, temperature=temperature)  # type: ignore

        return ChatGroq(model=model)

    if provider == LLMProvider.google:
        from langchain_google_genai import ChatGoogleGenerativeAI

        if temperature:
            return ChatGoogleGenerativeAI(model=model, temperature=temperature)

        return ChatGoogleGenerativeAI(model=model)

    # if provider == LLMProvider.openrouter:
    #     from langchain_openai import ChatOpenAI

    #     openrouter_api_key = os.getenv("OPENROUTER_API_KEY")

    #     if not openrouter_api_key:
    #         raise ValueError("OPENROUTER_API_KEY is not set.")

    #     return ChatOpenAI(
    #         model=model,
    #         temperature=temperature,
    #         api_key=openrouter_api_key,
    #         base_url="https://openrouter.ai/api/v1",
    #     )  # type: ignore[call-arg, return-value]

    raise ValueError(f"Unknown LLM provider: {provider}")
