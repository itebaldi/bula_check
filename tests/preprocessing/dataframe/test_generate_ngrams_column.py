import pandas as pd
import pytest

from bula_check.preprocessing.dataframe import generate_ngrams_column


def test_generate_ngrams_column_default_output():
    data = {"text": ["this is a test", "another one"]}
    df = pd.DataFrame(data)

    result_df = generate_ngrams_column(
        df, column="text", ngram_range=(2, 3), output_column="text_ngrams"
    )

    assert "text_ngrams" in result_df.columns
    expected = [
        ["this is", "is a", "a test", "this is a", "is a test"],
        ["another one"],
    ]
    assert result_df["text_ngrams"].equals(pd.Series(expected, name="text_ngrams"))
    assert "text" in result_df.columns


def test_generate_ngrams_column_custom_output():
    data = {"sentences": ["test sentence one", "test sentence two"]}
    df = pd.DataFrame(data)

    result_df = generate_ngrams_column(
        df, column="sentences", ngram_range=(1, 1), output_column="tokens"
    )

    assert "tokens" in result_df.columns
    expected = [["test", "sentence", "one"], ["test", "sentence", "two"]]
    assert result_df["tokens"].tolist() == expected


def test_generate_ngrams_column_with_non_string_values():
    data = {"mixed_data": ["text", 123, "more text"]}
    df = pd.DataFrame(data)

    with pytest.raises(TypeError):
        generate_ngrams_column(df, column="mixed_data")
