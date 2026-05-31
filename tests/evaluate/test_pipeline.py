from pathlib import Path

from dotenv import load_dotenv
from langchain.messages import HumanMessage

from bula_check.agents.pipeline import build_graph
from bula_check.agents.pipeline import make_initial_state
from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import LLMProvider

load_dotenv()


def test_pipeline():

    config: BulaCheckConfig = {
        **DEFAULT_CONFIG,
        "llm_provider": LLMProvider.openai,
        "llm_model": "gpt-4o-mini",
        "return_chunks": "only_desired",
        "bulagratis_db_path": Path("bulas_gratis.db"),
        "with_rag": True,
        # "lexical_weight": None,
        # "semantic_weight": None,
    }

    graph = build_graph(config)

    state = make_initial_state(config)
    query = "tylenol pode causar enjoo?"
    state["messages"].append(HumanMessage(content=query))

    final_state = graph.invoke(state)  # type: ignore

    assert final_state
