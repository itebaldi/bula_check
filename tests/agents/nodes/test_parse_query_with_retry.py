from types import SimpleNamespace

from bula_check.agents import nodes
from bula_check.agents.nodes import _parse_query_with_retry
from bula_check.agents.protocol import DEFAULT_CONFIG

PARSED = {
    "medicine_name": "dipirona",
    "active_ingredient": None,
    "sections": ["adverse_reactions"],
    "expanded_keywords": ["enjoo"],
    "claim_type": "question",
    "original_query": "dipirona da enjoo?",
}


def test_parse_query_with_retry__successful_parse_has_no_error(monkeypatch):
    monkeypatch.setattr(
        nodes, "parse_medicine_query", SimpleNamespace(invoke=lambda _: PARSED)
    )
    parsed, error = _parse_query_with_retry("dipirona da enjoo?", DEFAULT_CONFIG)
    assert parsed == PARSED
    assert error is None


def test_parse_query_with_retry__recovers_on_the_second_attempt(monkeypatch):
    calls = []

    def flaky(_):
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("JSON incompleto (resposta truncada?)")
        return PARSED

    monkeypatch.setattr(nodes, "parse_medicine_query", SimpleNamespace(invoke=flaky))
    parsed, error = _parse_query_with_retry("dipirona da enjoo?", DEFAULT_CONFIG)
    assert len(calls) == 2
    assert parsed == PARSED
    assert error is None


def test_parse_query_with_retry__exhausted_attempts_report_the_error(monkeypatch):
    calls = []

    def always_fails(_):
        calls.append(1)
        raise ValueError("resposta sem JSON")

    monkeypatch.setattr(
        nodes, "parse_medicine_query", SimpleNamespace(invoke=always_fails)
    )
    parsed, error = _parse_query_with_retry("dipirona da enjoo?", DEFAULT_CONFIG)
    assert len(calls) == 2
    assert error == "ValueError: resposta sem JSON"
    assert parsed["medicine_name"] == ""
    assert parsed["sections"] == []
    assert parsed["original_query"] == "dipirona da enjoo?"
