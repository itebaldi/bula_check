import pandas as pd
import pytest

from bula_check.preprocessing.dataframe import remove_punctuation


def test_remove_punctuation():
    data = {
        "text": [
            "Olá, mundo!",
            "Isso é um teste... com números: 123.",
            "Texto sem-pontuação",
        ]
    }
    df = pd.DataFrame(data)

    result_df = remove_punctuation(df, column="text")

    expected = ["Olá mundo", "Isso é um teste com números 123", "Texto sempontuação"]
    assert result_df["text"].tolist() == expected


def test_remove_punctuation__with_output_column():
    data = {"text": ["Frase com (parênteses)."]}
    df = pd.DataFrame(data)

    result_df = remove_punctuation(df, column="text", output_column="text_no_punct")

    assert result_df["text"].tolist() == ["Frase com (parênteses)."]
    assert "text_no_punct" in result_df.columns
    assert result_df["text_no_punct"].tolist() == ["Frase com parênteses"]


def test_remove_punctuation__with_non_string_values():
    data = {"mixed_data": ["texto", 123, "outro texto", None]}
    df = pd.DataFrame(data)

    with pytest.raises(TypeError):
        remove_punctuation(df, column="mixed_data")
