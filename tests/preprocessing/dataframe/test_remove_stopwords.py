import pandas as pd
import pytest

from bula_check.preprocessing.dataframe import remove_stopwords
from inputs.stopwords import get_portuguese_stopwords

STOP_WORDS = get_portuguese_stopwords()


def test_remove_stopwords():
    data = {
        "text": [
            "Este é um teste simples",
            "Outro teste com mais palavras para remover",
            "Ainda UM outro teste",
            None,
        ]
    }
    df = pd.DataFrame(data)

    result_df = remove_stopwords(df, column="text", stop_words=STOP_WORDS)

    expected = ["teste simples", "Outro teste palavras remover", "Ainda outro teste"]
    assert result_df["text"].tolist()[:3] == expected


def test_remove_stopwords__with_output_column():
    data = {"text": ["uma frase de teste"]}
    df = pd.DataFrame(data)

    result_df = remove_stopwords(
        df, column="text", stop_words=STOP_WORDS, output_column="text_no_stops"
    )

    assert result_df["text"].tolist() == ["uma frase de teste"]
    assert "text_no_stops" in result_df.columns
    assert result_df["text_no_stops"].tolist() == ["frase teste"]


def test_remove_stopwords_with_non_string_values_raises_type_error():
    data = {"mixed_data": ["texto", 123, "outro texto"]}
    df = pd.DataFrame(data)

    with pytest.raises(TypeError):
        remove_stopwords(df, column="mixed_data", stop_words=STOP_WORDS)
