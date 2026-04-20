from pathlib import Path

from bula_check.bula_gratis import Bula
from bula_check.llm import precheck_llm
from bula_check.llm import validate_llm


def test_ollama__precheck():
    response = precheck_llm("Tylenol faz mal para o coração")

    assert response


def test_ollama__validate():
    bula = Bula.read_from_json(
        Path("inputs/bulas/json/paracetamol__prati_donaduzzi__cia_ltda.json")
    )

    response = validate_llm(
        "paracetamol faz mal para o coração", "paracetamol", bula.raw_text
    )

    assert response
