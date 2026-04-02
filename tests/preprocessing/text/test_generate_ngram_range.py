import pytest

from bula_check.preprocessing.text import generate_ngram_range


def test_generate_ngram_range_single_n():
    text = "a b c"
    expected = ["a b", "b c"]
    assert generate_ngram_range(text, (2, 2)) == expected


def test_generate_ngram_range_multiple_n():
    text = "one two three"
    expected = ["one", "two", "three", "one two", "two three"]
    assert generate_ngram_range(text, (1, 2)) == expected


def test_generate_ngram_range_empty_text():
    assert generate_ngram_range("", (1, 3)) == []


def test_generate_ngram_range_invalid_range_min_n_zero():
    with pytest.raises(
        ValueError, match="ngram_range must contain positive integers"
    ):
        generate_ngram_range("text", (0, 2))


def test_generate_ngram_range_invalid_range_min_n_greater_than_max_n():
    with pytest.raises(
        ValueError, match="ngram_range must contain positive integers"
    ):
        generate_ngram_range("text", (3, 2))


def test_generate_ngram_range_n_larger_than_tokens():
    text = "short text"
    expected = ["short", "text", "short text"]
    assert generate_ngram_range(text, (1, 3)) == expected
