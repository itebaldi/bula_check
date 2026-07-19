import pytest

from bula_check.agents.nodes import node_find_medicine
from bula_check.agents.pipeline import make_initial_state
from bula_check.agents.protocol import DEFAULT_CONFIG

pytestmark = pytest.mark.skipif(
    not DEFAULT_CONFIG["bulagratis_db_path"].exists(),
    reason="requer bulas_gratis.db (rodar da raiz do repo)",
)


def _state_with_medicine(name):
    state = make_initial_state(DEFAULT_CONFIG)
    state["parsed_query"] = {
        "medicine_name": name,
        "active_ingredient": None,
        "sections": ["indications"],
        "expanded_keywords": [name],
        "claim_type": "question",
        "original_query": name,
    }
    return state


@pytest.mark.parametrize(
    "typo, expected_name",
    [
        ("Mensiva", "MENSYVA"),
        ("aceclofenato", "ACECLOFENACO"),
    ],
)
def test_node_find_medicine__autoselects_on_typo(typo, expected_name):
    out = node_find_medicine(_state_with_medicine(typo))
    selected = out["selected_medicine"]
    assert selected is not None
    assert selected["medicine"]["name"] == expected_name


def test_node_find_medicine__exact_name_still_resolves():
    out = node_find_medicine(_state_with_medicine("MENSYVA"))
    selected = out["selected_medicine"]
    assert selected is not None
    assert selected["medicine"]["name"] == "MENSYVA"


def test_node_find_medicine__no_autoselect_on_garbage():
    out = node_find_medicine(_state_with_medicine("asdkjhqwe zzz"))
    assert out.get("selected_medicine") is None
    assert out["similar_medicines"] == []
