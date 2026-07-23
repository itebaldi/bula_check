import json
import random
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
from bula_check.evaluate import print_stress_breakdown

load_dotenv()

DATASET_PATH = Path("inputs/evaluation/dataset.json")
DATASET_SLIDING_PATH = Path("inputs/evaluation/dataset_sliding.json")
DATASET_JUDGED_PATH = Path("outputs/validation/dataset_judged.json")
DATASET_SLIDING_JUDGED_PATH = Path("outputs/validation/dataset_sliding_judged.json")

# Quantas perguntas avaliar. None = todas (790). Um inteiro faz uma amostra
# estratificada por categoria (determinística) para acelerar rodadas de teste.
MAX_QUERIES: int | None = 1
SAMPLE_SEED = 7


def _load_dataset(sliding_db: bool) -> list:
    """Carrega o dataset (fonte da verdade) e sobrepõe o parecer da validação.

    As perguntas e expected_chunk_ids vêm sempre do dataset limpo (inputs/); o
    bloco `validation` do painel (outputs/validation/*_judged.json) é sobreposto
    por id quando existe, habilitando o fatiamento por status no eval sem
    descartar nenhuma questão.
    """
    clean = DATASET_SLIDING_PATH if sliding_db else DATASET_PATH
    judged = DATASET_SLIDING_JUDGED_PATH if sliding_db else DATASET_JUDGED_PATH
    data = json.loads(clean.read_text(encoding="utf-8"))
    if judged.exists():
        val = {
            j["id"]: j.get("validation")
            for j in json.loads(judged.read_text(encoding="utf-8"))
        }
        for item in data:
            if item.get("validation") is None:
                item["validation"] = val.get(item["id"])
    return data


def _sample_dataset(data: list, n: int | None, seed: int = SAMPLE_SEED) -> list:
    """Amostra n perguntas estratificando por stress_category (determinística).

    Como o dataset é agrupado por medicamento, um corte simples data[:n] cobriria
    poucos medicamentos; a estratificação garante que mesmo um n pequeno
    represente todas as categorias. n None (ou >= total) avalia todas.
    """
    if n is None or n >= len(data):
        return data
    rng = random.Random(seed)
    groups: dict[str, list] = {}
    for item in data:
        key = item.get("stress_category") or "representativa"
        groups.setdefault(key, []).append(item)
    picked: list = []
    for cat in sorted(groups):
        group = groups[cat]
        k = max(1, round(n * len(group) / len(data)))
        picked.extend(rng.sample(group, min(k, len(group))))
    rng.shuffle(picked)
    return sorted(picked[:n], key=lambda x: x["id"])


# LLMProvider.openai, "gpt-4o-mini"
# LLMProvider.google,  "gemini-2.5-flash"
# LLMProvider.ollama, "llama3.1:8b"
# LLMProvider.groq, "llama-3.3-70b-versatile"


# fmt: off
@pytest.mark.parametrize(
    "idx, with_rag, lexical_weight, semantic_weight, sliding_db, return_chunks, llm_provider, llm_model",
    [
        # ("0.1", True, True, True, False, "only_desired", LLMProvider.openai, "gpt-4o-mini"),
        # ("0.2", False, True, True, False, "only_desired", LLMProvider.openai, "gpt-4o-mini"),
        # ("0.3", True, False, True, False, "only_desired", LLMProvider.openai, "gpt-4o-mini"), # FOI
        # ("0.4", True, True, False, False, "only_desired", LLMProvider.openai, "gpt-4o-mini"),
        # ("0.5", True, True, True, False, "with_prev_and_next", LLMProvider.openai, "gpt-4o-mini"),
        #### 
        # ("1.1", True, True, True, False, "only_desired", LLMProvider.ollama, "qwen3:8b"),
        # ("1.2", False, True, True, False, "only_desired", LLMProvider.ollama, "qwen3:8b"),
        # ("1.3", True, False, True, False, "only_desired", LLMProvider.ollama, "qwen3:8b"), # new 1
        # ("1.4", True, True, False, False, "only_desired", LLMProvider.ollama, "qwen3:8b"),
        # ("1.5", True, True, True, False, "with_prev_and_next", LLMProvider.ollama, "qwen3:8b"), # 1
        ####
        # ("2.1", True, True, True, False, "only_desired", LLMProvider.ollama, "llama3.1:8b"),
        # ("2.2", False, True, True, False, "only_desired", LLMProvider.ollama, "llama3.1:8b"),
        # ("2.3", True, False, True, False, "only_desired", LLMProvider.ollama, "llama3.1:8b"),
        # ("2.4", True, True, False, False, "only_desired", LLMProvider.ollama, "llama3.1:8b"),
        # ("2.5", True, True, True, False, "with_prev_and_next", LLMProvider.ollama, "llama3.1:8b"),
        ###  
        # ("3.1", True, True, True, False, "only_desired", LLMProvider.ollama, "gemma3:4b"),
        # ("3.2", False, True, True, False, "only_desired", LLMProvider.ollama, "gemma3:4b"),
        # ("3.3", True, False, True, False, "only_desired", LLMProvider.ollama, "gemma3:4b"),
        # ("3.4", True, True, False, False, "only_desired", LLMProvider.ollama, "gemma3:4b"),
        # ("3.5", True, True, True, False, "with_prev_and_next", LLMProvider.ollama, "gemma3:4b"),
        ####
        # ("4.1", True, True, True, False, "only_desired", LLMProvider.ollama, "llama3.2:3b"),
        # ("4.2", False, True, True, False, "only_desired", LLMProvider.ollama, "llama3.2:3b"),
        # ("4.3", True, False, True, False, "only_desired", LLMProvider.ollama, "llama3.2:3b"),
        # ("4.4", True, True, False, False, "only_desired", LLMProvider.ollama, "llama3.2:3b"),
        # ("4.5", True, True, True, False, "with_prev_and_next", LLMProvider.ollama, "llama3.2:3b"),
        ####
        ("5.1", True, True, True, False, "only_desired", LLMProvider.openai, "gpt-5-mini"), # 2 FOI
        # ("5.2", False, True, True, False, "only_desired", LLMProvider.openai, "gpt-5-mini"),
        # ("5.3", True, False, True, False, "only_desired", LLMProvider.openai, "gpt-5-mini"),
        # ("5.4", True, True, False, False, "only_desired", LLMProvider.openai, "gpt-5-mini"),
        # ("5.5", True, True, True, False, "with_prev_and_next", LLMProvider.openai, "gpt-5-mini"),
        ####
        # ("s0.1", True, True, True, True, "only_desired", LLMProvider.openai, "gpt-4o-mini"),
        # ("s0.2", False, True, True, True, "only_desired", LLMProvider.openai, "gpt-4o-mini"),
        # ("s0.3", True, False, True, True, "only_desired", LLMProvider.openai, "gpt-4o-mini"),
        # ("s0.4", True, True, False, True, "only_desired", LLMProvider.openai, "gpt-4o-mini"),
        # ("s0.5", True, True, True, True, "with_prev_and_next", LLMProvider.openai, "gpt-4o-mini"),
        #### 
        # ("s1.1", True, True, True, True, "only_desired", LLMProvider.ollama, "qwen3:8b"),
        # ("s1.2", False, True, True, True, "only_desired", LLMProvider.ollama, "qwen3:8b"),
        # ("s1.3", True, False, True, True, "only_desired", LLMProvider.ollama, "qwen3:8b"),
        # ("s1.4", True, True, False, True, "only_desired", LLMProvider.ollama, "qwen3:8b"),
        # ("s1.5", True, True, True, True, "with_prev_and_next", LLMProvider.ollama, "qwen3:8b"),
        # ####
        # ("s2.1", True, True, True, True, "only_desired", LLMProvider.ollama, "llama3.1:8b"),
        # ("s2.2", False, True, True, True, "only_desired", LLMProvider.ollama, "llama3.1:8b"),
        # ("s2.3", True, False, True, True, "only_desired", LLMProvider.ollama, "llama3.1:8b"),
        # ("s2.4", True, True, False, True, "only_desired", LLMProvider.ollama, "llama3.1:8b"),
        # ("s2.5", True, True, True, True, "with_prev_and_next", LLMProvider.ollama, "llama3.1:8b"),
        # ###  
        # ("s3.1", True, True, True, True, "only_desired", LLMProvider.ollama, "gemma3:4b"),
        # ("s3.2", False, True, True, True, "only_desired", LLMProvider.ollama, "gemma3:4b"),
        # ("s3.3", True, False, True, True, "only_desired", LLMProvider.ollama, "gemma3:4b"),
        # ("s3.4", True, True, False, True, "only_desired", LLMProvider.ollama, "gemma3:4b"),
        # ("s3.5", True, True, True, True, "with_prev_and_next", LLMProvider.ollama, "gemma3:4b"),
        # # ####
        # ("s4.1", True, True, True, True, "only_desired", LLMProvider.ollama, "llama3.2:3b"),
        # ("s4.2", False, True, True, True, "only_desired", LLMProvider.ollama, "llama3.2:3b"),
        # ("s4.3", True, False, True, True, "only_desired", LLMProvider.ollama, "llama3.2:3b"),
        # ("s4.4", True, True, False, True, "only_desired", LLMProvider.ollama, "llama3.2:3b"),
        # ("s4.5", True, True, True, True, "with_prev_and_next", LLMProvider.ollama, "llama3.2:3b"),
        ####
        # ("s5.1", True, True, True, True, "only_desired", LLMProvider.openai, "gpt-5-mini"),
        # ("s5.2", False, True, True, True, "only_desired", LLMProvider.openai, "gpt-5-mini"),
        # ("s5.3", True, False, True, True, "only_desired", LLMProvider.openai, "gpt-5-mini"),
        # ("s5.4", True, True, False, True, "only_desired", LLMProvider.openai, "gpt-5-mini"),
        # ("s5.5", True, True, True, True, "with_prev_and_next", LLMProvider.openai, "gpt-5-mini"),
    ],
)
# fmt: on
def test_evaluate_results(
    idx: str,
    with_rag: bool,
    lexical_weight: bool,
    semantic_weight: bool,
    sliding_db: bool,
    return_chunks: Literal["only_desired", "with_prev_and_next"],
    llm_provider: LLMProvider,
    llm_model: str,
):
    dataset = _sample_dataset(_load_dataset(sliding_db), MAX_QUERIES)

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
    results_path = Path(f"outputs/evaluation/results/{name}.json")
    summary = evaluate_results(cfg, graph, items, results_path)
    print_stress_breakdown(summary)
