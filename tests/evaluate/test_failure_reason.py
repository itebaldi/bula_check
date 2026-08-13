import pytest

from bula_check.evaluate import _failure_reason


def _reason(**overrides):
    kwargs = {
        "pipeline_error": None,
        "parse_error": None,
        "selected_medicine": {"medicine": {"id": "m1"}},
        "retrieved_chunks": [{"chunk": {"id": "c1"}, "score": 1.0}],
        "with_rag": True,
    }
    kwargs.update(overrides)
    return _failure_reason(**kwargs)


def test_failure_reason__complete_run_has_no_reason():
    assert _reason() is None


def test_failure_reason__pipeline_error_wins_over_the_rest():
    assert (
        _reason(
            pipeline_error="RuntimeError: boom",
            parse_error="ValueError: sem JSON",
            selected_medicine=None,
            retrieved_chunks=[],
        )
        == "pipeline_error"
    )


def test_failure_reason__parse_error_wins_over_medicine_not_found():
    assert (
        _reason(parse_error="ValueError: sem JSON", selected_medicine=None)
        == "parse_failed"
    )


def test_failure_reason__medicine_not_found():
    assert _reason(selected_medicine=None, retrieved_chunks=[]) == "medicine_not_found"


def test_failure_reason__medicine_found_without_chunks():
    assert _reason(retrieved_chunks=[]) == "no_chunks"


@pytest.mark.parametrize("selected_medicine", [None, {"medicine": {"id": "m1"}}])
def test_failure_reason__closed_book_ignores_retrieval(selected_medicine):
    assert (
        _reason(
            with_rag=False, selected_medicine=selected_medicine, retrieved_chunks=[]
        )
        is None
    )


def test_failure_reason__closed_book_still_reports_parse_failure():
    assert _reason(with_rag=False, parse_error="ValueError: x") == "parse_failed"
