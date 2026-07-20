from bula_check.agents.search import _minmax_normalize


def test_minmax_normalize__empty_returns_empty():
    assert _minmax_normalize({}) == {}


def test_minmax_normalize__single_value_returns_zero():
    assert _minmax_normalize({"a": 7.5}) == {"a": 0.0}


def test_minmax_normalize__all_equal_returns_zeros():
    assert _minmax_normalize({"a": 3.0, "b": 3.0}) == {"a": 0.0, "b": 0.0}


def test_minmax_normalize__scales_to_unit_range():
    assert _minmax_normalize({"a": 2.0, "b": 4.0, "c": 6.0}) == {
        "a": 0.0,
        "b": 0.5,
        "c": 1.0,
    }


def test_minmax_normalize__is_monotonic():
    result = _minmax_normalize({"lo": 1.0, "mid": 3.0, "hi": 9.0})
    assert result["lo"] == 0.0
    assert result["hi"] == 1.0
    assert result["lo"] < result["mid"] < result["hi"]
