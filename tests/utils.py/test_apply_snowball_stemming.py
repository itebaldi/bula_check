import pandas as pd
import pytest

from bula_check.utils import apply_snowball_stemming


def test_apply_snowball_stemming():
    data = {
        "text": [
            "Eu estou correndo na praia",
            "Ele corria todos os dias",
            "Nós correremos amanhã",
        ]
    }
    df = pd.DataFrame(data)

    result_df = apply_snowball_stemming(df, column="text", language="portuguese")

    expected = [
        "eu estou corr na pra",
        "ele corr tod os dias",
        "nós corr amanhã",
    ]
    assert result_df["text"].tolist() == expected


def test_apply_snowball_stemming__with_output_column():
    data = {"text": ["amigavelmente"]}
    df = pd.DataFrame(data)

    result_df = apply_snowball_stemming(
        df, column="text", language="portuguese", output_column="text_stemmed"
    )

    assert result_df["text"].tolist() == ["amigavelmente"]
    assert "text_stemmed" in result_df.columns
    assert result_df["text_stemmed"].tolist() == ["amig"]


def test_apply_snowball_stemming__with_non_string_values():
    data = {"mixed_data": ["texto", 123, "outro texto"]}
    df = pd.DataFrame(data)

    with pytest.raises(TypeError):
        apply_snowball_stemming(df, column="mixed_data", language="portuguese")


def test_apply_snowball_stemming__with_unsupported_language():
    data = {"text": ["some text"]}
    df = pd.DataFrame(data)

    with pytest.raises(ValueError):
        apply_snowball_stemming(df, column="text", language="klingon")  # type: ignore
