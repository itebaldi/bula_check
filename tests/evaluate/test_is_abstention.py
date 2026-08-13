import pytest

from bula_check.evaluate import _is_abstention


@pytest.mark.parametrize("reason", ["pipeline_error", "parse_failed", "medicine_not_found"])
def test_is_abstention__pipeline_stopped_before_judging(reason):
    assert _is_abstention(
        {"predicted_verdict": "inconclusive", "failure_reason": reason}
    )


def test_is_abstention__no_chunks_still_counts_as_answer():
    assert not _is_abstention(
        {"predicted_verdict": "inconclusive", "failure_reason": "no_chunks"}
    )


def test_is_abstention__complete_run_is_an_answer():
    assert not _is_abstention(
        {"predicted_verdict": "confirmed", "failure_reason": None}
    )


def test_is_abstention__legacy_empty_verdict_without_reason():
    assert _is_abstention({"predicted_verdict": ""})


def test_is_abstention__legacy_verdict_without_reason_is_an_answer():
    assert not _is_abstention({"predicted_verdict": "refuted"})
