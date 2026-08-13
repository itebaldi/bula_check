from bula_check.agents.pipeline import _route_after_suggest
from bula_check.agents.pipeline import make_initial_state
from bula_check.agents.protocol import DEFAULT_CONFIG


def test_route_after_suggest__confirmed_medicine_goes_to_retrieval():
    state = make_initial_state(DEFAULT_CONFIG)
    state["selected_medicine"] = {"medicine": {"id": "m1"}, "score": 1.0}
    assert _route_after_suggest(state) == "fetch_chunks"


def test_route_after_suggest__without_medicine_still_verifies():
    assert _route_after_suggest(make_initial_state(DEFAULT_CONFIG)) == "verify_claim"


def test_route_after_suggest__awaiting_confirmation_still_verifies():
    state = make_initial_state(DEFAULT_CONFIG)
    state["awaiting_user_confirmation"] = True
    assert _route_after_suggest(state) == "verify_claim"
