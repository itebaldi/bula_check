import math

import pytest

from bula_check.evaluate import _aggregate


def _row(expected, predicted, correct, failure_reason=None):
    return {
        "medicine_correct": True,
        "section_correct": True,
        "verdict_correct": correct,
        "expected_verdict": expected,
        "predicted_verdict": predicted,
        "failure_reason": failure_reason,
    }


def test_aggregate__empty_input():
    assert _aggregate([], []) == {"n": 0}


def test_aggregate__answer_rate_counts_only_judged_items():
    rows = [
        _row("confirmed", "confirmed", True),
        _row("refuted", "confirmed", False),
        _row("inconclusive", "inconclusive", False, "medicine_not_found"),
        _row("confirmed", "", False),
    ]
    summary = _aggregate(rows, [])
    assert summary["verdict_answer_rate"] == pytest.approx(0.5)
    assert summary["verdict_accuracy"] == pytest.approx(0.25)
    assert summary["verdict_accuracy_answered"] == pytest.approx(0.5)


def test_aggregate__abstention_goes_to_the_no_answer_column():
    rows = [_row("inconclusive", "inconclusive", False, "medicine_not_found")]
    matrix = _aggregate(rows, [])["verdict_confusion_matrix"]
    assert matrix["inconclusive"]["sem_resposta"] == 1
    assert matrix["inconclusive"]["inconclusive"] == 0


def test_aggregate__diagonal_reproduces_verdict_accuracy():
    rows = [
        _row("confirmed", "confirmed", True),
        _row("refuted", "inconclusive", False),
        _row("inconclusive", "inconclusive", False, "parse_failed"),
        _row("inconclusive", "inconclusive", True),
    ]
    summary = _aggregate(rows, [])
    matrix = summary["verdict_confusion_matrix"]
    diagonal = sum(matrix[label][label] for label in matrix)
    assert diagonal / len(rows) == pytest.approx(summary["verdict_accuracy"])


def test_aggregate__accuracy_answered_times_answer_rate_gives_accuracy():
    rows = [
        _row("confirmed", "confirmed", True),
        _row("refuted", "confirmed", False),
        _row("confirmed", "inconclusive", False, "pipeline_error"),
    ]
    summary = _aggregate(rows, [])
    assert math.isclose(
        summary["verdict_answer_rate"] * summary["verdict_accuracy_answered"],
        summary["verdict_accuracy"],
    )


def test_aggregate__all_items_answered():
    rows = [_row("confirmed", "confirmed", True), _row("refuted", "refuted", True)]
    summary = _aggregate(rows, [])
    assert summary["verdict_answer_rate"] == 1.0
    assert summary["verdict_accuracy_answered"] == 1.0


def test_aggregate__no_answered_item_reports_zero_instead_of_dividing():
    rows = [_row("confirmed", "", False), _row("refuted", "", False)]
    summary = _aggregate(rows, [])
    assert summary["verdict_answer_rate"] == 0.0
    assert summary["verdict_accuracy_answered"] == 0.0
