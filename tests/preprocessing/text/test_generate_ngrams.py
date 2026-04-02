import pytest

from bula_check.preprocessing.text import generate_ngrams


def test_generate_ngrams_unigrams():
    text = "this is a test"
    expected = ["this", "is", "a", "test"]
    assert generate_ngrams(text, 1) == expected


def test_generate_ngrams_bigrams():
    text = "this is a test"
    expected = ["this is", "is a", "a test"]
    assert generate_ngrams(text, 2) == expected


def test_generate_ngrams_trigrams():
    text = "this is a test"
    expected = ["this is a", "is a test"]
    assert generate_ngrams(text, 3) == expected


def test_generate_ngrams_n_larger_than_tokens():
    assert generate_ngrams("short text", 3) == []


def test_generate_ngrams_invalid_n():
    with pytest.raises(ValueError, match="n must be greater than or equal to 1."):
        generate_ngrams("some text", 0)
