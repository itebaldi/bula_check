import json
from pathlib import Path
from typing import Literal

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


# fmt: off
@pytest.mark.parametrize(
    "idx, with_rag, lexical_weight, semantic_weight, sliding_db, return_chunks, llm_provider, llm_model",
    [(0, True, True, True, False, "only_desired", LLMProvider.openai, "gpt-4o-mini")],
)
# fmt: on
def test_evaluate_results(
    idx: int,
    with_rag: bool,
    lexical_weight: bool,
    semantic_weight: bool,
    sliding_db: bool,
    return_chunks: Literal["only_desired", "with_prev_and_next"],
    llm_provider: LLMProvider,
    llm_model: str,
):
    dataset = json.loads(DATASET_PATH.read_text(encoding="utf-8"))

    cfg: BulaCheckConfig = {
        **DEFAULT_CONFIG,
        "llm_provider": llm_provider,
        "llm_model": llm_model,
        "return_chunks": return_chunks,
        "bulagratis_db_path": Path(
            "bulas_gratis_sliding.db" if sliding_db else "bulas_gratis.db"
        ),
        "with_rag": with_rag,
        "lexical_weight": 0.4 if lexical_weight else None,
        "semantic_weight": 0.6 if semantic_weight else None,
    }

    graph = build_graph(cfg)

    items = [ExpectedResult(**i) for i in dataset]

    rag = "rag" if with_rag else "withoutRag"
    name = f"{rag}_{idx}"
    results_path = Path(f"inputs/evaluation/results/{name}.json")
    evaluate_results(cfg, graph, items, results_path)
