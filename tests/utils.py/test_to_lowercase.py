import pandas as pd
import pytest

from bula_check.utils import to_lowercase


def test_to_lowercase():
    data = {"text": ["Primeira Frase", "SEGUNDA FRASE", "TeRcEiRa FrAsE"]}
    df = pd.DataFrame(data)

    result_df = to_lowercase(df, column="text")

    expected = ["primeira frase", "segunda frase", "terceira frase"]
    assert result_df["text"].tolist() == expected


def test_to_lowercase__with_output_column():
    data = {"text": ["CaSo De TeStE", "Outro Caso"]}
    df = pd.DataFrame(data)

    result_df = to_lowercase(df, column="text", output_column="text_lower")

    assert result_df["text"].tolist() == ["CaSo De TeStE", "Outro Caso"]

    expected = ["caso de teste", "outro caso"]
    assert "text_lower" in result_df.columns
    assert result_df["text_lower"].tolist() == expected


def test_to_lowercase__non_string_values():
    data = {"mixed_data": ["um", 2, "tres", None]}
    df = pd.DataFrame(data)

    with pytest.raises(TypeError):
        to_lowercase(df, column="mixed_data")
