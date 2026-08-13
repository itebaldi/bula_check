from bula_check.agents.nodes import node_verify_claim
from bula_check.agents.pipeline import make_initial_state
from bula_check.agents.protocol import DEFAULT_CONFIG


def _state(**overrides):
    state = make_initial_state({**DEFAULT_CONFIG, **overrides.pop("config", {})})
    state["parsed_query"] = {
        "medicine_name": "Duomo HP",
        "active_ingredient": None,
        "sections": ["indications"],
        "expanded_keywords": ["próstata"],
        "claim_type": "question",
        "original_query": "Duomo HP serve pra próstata aumentada?",
    }
    state.update(overrides)
    return state


def _medicine(name="DUOMO HP"):
    return {
        "medicine": {
            "id": "m1",
            "name": name,
            "processed_name": name.lower(),
            "active_ingredient": ["doxazosina"],
            "processed_active_ingredient": ["doxazosina"],
            "source": "bula_gratis",
            "url": "http://x",
            "registration_number": None,
            "therapeutic_classes": None,
            "company_name": "lab",
            "processed_company_name": "lab",
            "cnpj": None,
        },
        "score": 1.0,
    }


def test_node_verify_claim__without_parsed_query_returns_empty():
    state = _state()
    state["parsed_query"] = None
    assert node_verify_claim(state) == {}


def test_node_verify_claim__medicine_not_found_returns_inconclusive_verdict():
    out = node_verify_claim(_state())
    result = out["verification_result"]
    assert result["verdict"] == "inconclusive"
    assert result["confidence"] == 0.0
    assert result["supporting_chunks"] == []
    assert "Duomo HP" in result["response_text"]


def test_node_verify_claim__medicine_not_found_answers_the_user():
    out = node_verify_claim(_state())
    assert [m.content for m in out["messages"]] == [
        out["verification_result"]["response_text"]
    ]


def test_node_verify_claim__awaiting_confirmation_keeps_verdict_without_message():
    out = node_verify_claim(_state(awaiting_user_confirmation=True))
    assert out["verification_result"]["verdict"] == "inconclusive"
    assert "messages" not in out


def test_node_verify_claim__medicine_without_chunks_returns_inconclusive_verdict():
    out = node_verify_claim(_state(selected_medicine=_medicine()))
    result = out["verification_result"]
    assert result["verdict"] == "inconclusive"
    assert result["confidence"] == 0.0
    assert "DUOMO HP" in result["response_text"]
