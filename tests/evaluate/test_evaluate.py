import json
import random
from pathlib import Path

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
MAX_QUERIES: int | None = 300
SAMPLE_SEED = 7

# Vocabulário das configurações — mesmas strings da tabela do artigo.
PROVIDER_BY_MODEL = {
    "gpt-4o-mini": LLMProvider.openai,
    "gpt-5-mini": LLMProvider.openai,
    "qwen3:8b": LLMProvider.ollama,
    "llama3.1:8b": LLMProvider.ollama,
    "llama3.2:3b": LLMProvider.ollama,
    "gemma3:4b": LLMProvider.ollama,
}
# fragmentação da bula
BASE = {"sentenças": False, "janela desl.": True}
# estratégia de busca -> (with_rag, lexical_weight, semantic_weight)
BUSCA = {
    "híbrida": (True, 0.4, 0.6),
    "semântica": (True, None, 0.6),
    "lexical": (True, 0.4, None),
    "sem RAG": (False, None, None),
}
# contexto enviado ao modelo
CHUNKS = {
    "apenas recuperado": "only_desired",
    "recuperado+vizinhos": "with_prev_and_next",
    "–": "only_desired",
}


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
    "idx, model, base, busca, chunks",
    [
        # ("0.1",  "gpt-4o-mini", "sentenças",    "híbrida",   "apenas recuperado"), # FOI
        # ("0.2",  "gpt-4o-mini", "sentenças",    "sem RAG",   "–"),
        # ("0.3",  "gpt-4o-mini", "sentenças",    "semântica", "apenas recuperado"), # FOI
        # ("0.4",  "gpt-4o-mini", "sentenças",    "lexical",   "apenas recuperado"),
        # ("0.5",  "gpt-4o-mini", "sentenças",    "híbrida",   "recuperado+vizinhos"),
        #### 
        # ("1.1",  "qwen3:8b",    "sentenças",    "híbrida",   "apenas recuperado"), # FOI
        # ("1.2",  "qwen3:8b",    "sentenças",    "sem RAG",   "–"), # FOI
        # ("1.3",  "qwen3:8b",    "sentenças",    "semântica", "apenas recuperado"), # new 1 # FOI
        ("1.4",  "qwen3:8b",    "sentenças",    "lexical",   "apenas recuperado"),
        ("1.5",  "qwen3:8b",    "sentenças",    "híbrida",   "recuperado+vizinhos"), # 1
        ####
        # ("2.1",  "llama3.1:8b", "sentenças",    "híbrida",   "apenas recuperado"),
        # ("2.2",  "llama3.1:8b", "sentenças",    "sem RAG",   "–"),
        # ("2.3",  "llama3.1:8b", "sentenças",    "semântica", "apenas recuperado"),
        # ("2.4",  "llama3.1:8b", "sentenças",    "lexical",   "apenas recuperado"),
        # ("2.5",  "llama3.1:8b", "sentenças",    "híbrida",   "recuperado+vizinhos"),
        ###  
        # ("3.1",  "gemma3:4b",   "sentenças",    "híbrida",   "apenas recuperado"),
        # ("3.2",  "gemma3:4b",   "sentenças",    "sem RAG",   "–"),
        # ("3.3",  "gemma3:4b",   "sentenças",    "semântica", "apenas recuperado"), # FOI
        # ("3.4",  "gemma3:4b",   "sentenças",    "lexical",   "apenas recuperado"),
        # ("3.5",  "gemma3:4b",   "sentenças",    "híbrida",   "recuperado+vizinhos"),
        ####
        # ("4.1",  "llama3.2:3b", "sentenças",    "híbrida",   "apenas recuperado"),
        # ("4.2",  "llama3.2:3b", "sentenças",    "sem RAG",   "–"),
        ("4.3",  "llama3.2:3b", "sentenças",    "semântica", "apenas recuperado"),
        # ("4.4",  "llama3.2:3b", "sentenças",    "lexical",   "apenas recuperado"),
        # ("4.5",  "llama3.2:3b", "sentenças",    "híbrida",   "recuperado+vizinhos"),
        ####
        ("5.1",  "gpt-5-mini",  "sentenças",    "híbrida",   "apenas recuperado"), # 2 FOI
        ("5.2",  "gpt-5-mini",  "sentenças",    "sem RAG",   "–"),
        ("5.3",  "gpt-5-mini",  "sentenças",    "semântica", "apenas recuperado"),
        ("5.4",  "gpt-5-mini",  "sentenças",    "lexical",   "apenas recuperado"),
        ("5.5",  "gpt-5-mini",  "sentenças",    "híbrida",   "recuperado+vizinhos"),
        ####
        # ("s0.1", "gpt-4o-mini", "janela desl.", "híbrida",   "apenas recuperado"),
        # ("s0.2", "gpt-4o-mini", "janela desl.", "sem RAG",   "–"),
        # ("s0.3", "gpt-4o-mini", "janela desl.", "semântica", "apenas recuperado"),
        # ("s0.4", "gpt-4o-mini", "janela desl.", "lexical",   "apenas recuperado"),
        # ("s0.5", "gpt-4o-mini", "janela desl.", "híbrida",   "recuperado+vizinhos"),
        #### 
        ("s1.1", "qwen3:8b",    "janela desl.", "híbrida",   "apenas recuperado"),
        ("s1.2", "qwen3:8b",    "janela desl.", "sem RAG",   "–"),
        ("s1.3", "qwen3:8b",    "janela desl.", "semântica", "apenas recuperado"),
        ("s1.4", "qwen3:8b",    "janela desl.", "lexical",   "apenas recuperado"),
        ("s1.5", "qwen3:8b",    "janela desl.", "híbrida",   "recuperado+vizinhos"),
        # ####
        # ("s2.1", "llama3.1:8b", "janela desl.", "híbrida",   "apenas recuperado"),
        # ("s2.2", "llama3.1:8b", "janela desl.", "sem RAG",   "–"),
        # ("s2.3", "llama3.1:8b", "janela desl.", "semântica", "apenas recuperado"),
        # ("s2.4", "llama3.1:8b", "janela desl.", "lexical",   "apenas recuperado"),
        ("s2.5", "llama3.1:8b", "janela desl.", "híbrida",   "recuperado+vizinhos"),
        # ###  
        # ("s3.1", "gemma3:4b",   "janela desl.", "híbrida",   "apenas recuperado"),
        # ("s3.2", "gemma3:4b",   "janela desl.", "sem RAG",   "–"),
        # ("s3.3", "gemma3:4b",   "janela desl.", "semântica", "apenas recuperado"),
        # ("s3.4", "gemma3:4b",   "janela desl.", "lexical",   "apenas recuperado"),
        # ("s3.5", "gemma3:4b",   "janela desl.", "híbrida",   "recuperado+vizinhos"),
        # # ####
        # ("s4.1", "llama3.2:3b", "janela desl.", "híbrida",   "apenas recuperado"),
        # ("s4.2", "llama3.2:3b", "janela desl.", "sem RAG",   "–"),
        # ("s4.3", "llama3.2:3b", "janela desl.", "semântica", "apenas recuperado"),
        # ("s4.4", "llama3.2:3b", "janela desl.", "lexical",   "apenas recuperado"),
        # ("s4.5", "llama3.2:3b", "janela desl.", "híbrida",   "recuperado+vizinhos"),
        ####
        ("s5.1", "gpt-5-mini",  "janela desl.", "híbrida",   "apenas recuperado"),
        ("s5.2", "gpt-5-mini",  "janela desl.", "sem RAG",   "–"),
        ("s5.3", "gpt-5-mini",  "janela desl.", "semântica", "apenas recuperado"),
        ("s5.4", "gpt-5-mini",  "janela desl.", "lexical",   "apenas recuperado"),
        ("s5.5", "gpt-5-mini",  "janela desl.", "híbrida",   "recuperado+vizinhos"),
    ],
)
# fmt: on
def test_evaluate_results(
    idx: str,
    model: str,
    base: str,
    busca: str,
    chunks: str,
):
    with_rag, lexical_weight, semantic_weight = BUSCA[busca]
    sliding_db = BASE[base]

    dataset = _sample_dataset(_load_dataset(sliding_db), MAX_QUERIES)

    cfg: BulaCheckConfig = {
        **DEFAULT_CONFIG,
        "llm_provider": PROVIDER_BY_MODEL[model],
        "llm_model": model,
        "return_chunks": CHUNKS[chunks], # type: ignore
        "bulagratis_db_path": Path(
            "bulas_gratis_sliding.db" if sliding_db else "bulas_gratis.db"
        ),
        "with_rag": with_rag,
        "lexical_weight": lexical_weight,
        "semantic_weight": semantic_weight,
    }

    graph = build_graph(cfg)

    items = [ExpectedResult(**i) for i in dataset]

    rag = "rag" if with_rag else "withoutRag"
    name = f"{rag}_{idx}"
    results_path = Path(f"outputs/evaluation/results/{name}.json")
    summary = evaluate_results(cfg, graph, items, results_path)
    print_stress_breakdown(summary)
