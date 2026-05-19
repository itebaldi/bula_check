import json
from pathlib import Path

from dotenv import load_dotenv

from bula_check.agents.pipeline import build_graph
from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import LLMProvider
from bula_check.evaluate import ExpectecResult
from bula_check.evaluate import evaluate_results

load_dotenv()

DATASET_PATH = Path("inputs/evaluation/dataset.json")
RESULTS_PATH = Path("inputs/evaluation/results.json")


def test_evaluate_results():
    dataset = json.loads(DATASET_PATH.read_text(encoding="utf-8"))

    cfg: BulaCheckConfig = {
        **DEFAULT_CONFIG,
        "llm_provider": LLMProvider.openai,
        "llm_model": "gpt-4o-mini",
    }

    graph = build_graph(cfg)

    items = [ExpectecResult(**i) for i in dataset]

    evaluated = evaluate_results(cfg, graph, items, RESULTS_PATH)
