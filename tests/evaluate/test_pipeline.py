import json
from pathlib import Path

import pytest
from dotenv import load_dotenv

from bula_check.agents.pipeline import build_graph
from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import LLMProvider
from bula_check.evaluate import ExpectedResult
from bula_check.evaluate import evaluate_results

load_dotenv()

DATASET_PATH = Path("inputs/evaluation/dataset.json")


@pytest.mark.parametrize(
    "with_rag",
    [
        # True,
        False,
    ],
)
def test_evaluate_results(with_rag: bool):
    dataset = json.loads(DATASET_PATH.read_text(encoding="utf-8"))

    cfg: BulaCheckConfig = {
        **DEFAULT_CONFIG,
        "llm_provider": LLMProvider.openai,
        "llm_model": "gpt-4o-mini",
        "return_chunks": "only_desired",  # "with_prev_and_next"
        "bulagratis_db_path": Path(
            "bulas_gratis.db"
        ),  # Path("bulas_gratis_sliding.db")
        "with_rag": with_rag,
        # "lexical_weight": None,
        # "semantic_weight": None,
    }
    # chuck hierarquico

    graph = build_graph(cfg)

    items = [ExpectedResult(**i) for i in dataset]

    suffix = "rag" if with_rag else "baseline"
    results_path = Path(f"inputs/evaluation/results_{suffix}.json")
    evaluate_results(cfg, graph, items, results_path)
