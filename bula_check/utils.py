from typing import Any

import pandas as pd
from bula_check.constants import LANGUAGES
from nltk.stem import SnowballStemmer

from bula_check.tools import curry


@curry
def map_column_values(
    df: pd.DataFrame,
    column: str,
    mapping: dict[Any, Any],
    output_column: str | None = None,
) -> pd.DataFrame:
    """
    Map values from one DataFrame column to another set of values.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source column whose values will be mapped.
    mapping : dict[Any, Any]
        Dictionary mapping source values to target values.
    output_column : str | None, default=None
        Name of the output column. If ``None``, the source column is
        overwritten.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with the mapped column.

    Raises
    ------
    KeyError
        If ``column`` does not exist in the DataFrame.
    ValueError
        If the mapping produces missing values for any non-null input.
    """
    if column not in df.columns:
        raise KeyError(f"Column not found: {column}")

    result = df.copy()
    target_column = output_column or column

    mapped_values = result[column].map(mapping)

    unmapped_mask = result[column].notna() & mapped_values.isna()
    if unmapped_mask.any():
        unmapped_values = result.loc[unmapped_mask, column].unique().tolist()
        raise ValueError(
            f"Unmapped values found in column '{column}': {unmapped_values}"
        )

    result[target_column] = mapped_values
    return result


@curry
def to_lowercase(
    df: pd.DataFrame,
    column: str,
    output_column: str | None = None,
) -> pd.DataFrame:
    """
    Convert all string values in a DataFrame column to lowercase.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source column.
    output_column : str | None, default=None
        Name of the output column. If ``None``, the source column is
        overwritten.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with the lowercased column.

    Raises
    ------
    KeyError
        If ``column`` does not exist in the DataFrame.
    TypeError
        If the column contains non-string, non-null values.
    """
    if column not in df.columns:
        raise KeyError(f"Column not found: {column}")

    non_string_mask = df[column].notna() & ~df[column].map(
        lambda value: isinstance(value, str)
    )
    if non_string_mask.any():
        raise TypeError(
            f"Column '{column}' contains non-string values and cannot be lowercased."
        )

    result = df.copy()
    target_column = output_column or column
    result[target_column] = result[column].str.lower()

    return result


@curry
def remove_punctuation(
    df: pd.DataFrame,
    column: str,
    output_column: str | None = None,
) -> pd.DataFrame:
    """
    Remove punctuation characters from a text column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source text column.
    output_column : str | None, default=None
        Name of the output column. If ``None``, the source column is
        overwritten.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with punctuation removed from the
        selected column.

    Raises
    ------
    KeyError
        If ``column`` does not exist in the DataFrame.
    TypeError
        If the column contains non-string, non-null values.
    """
    if column not in df.columns:
        raise KeyError(f"Column not found: {column}")

    non_string_mask = df[column].notna() & ~df[column].map(
        lambda value: isinstance(value, str)
    )
    if non_string_mask.any():
        raise TypeError(
            f"Column '{column}' contains non-string values and cannot be processed."
        )

    result = df.copy()
    target_column = output_column or column

    result[target_column] = result[column].str.replace(
        r"[^\w\s]",
        "",
        regex=True,
    )

    return result


@curry
def remove_stopwords(
    df: pd.DataFrame,
    column: str,
    stop_words: set[str],
    output_column: str | None = None,
) -> pd.DataFrame:
    """
    Remove stopwords from a text column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source text column.
    stop_words : Collection[str]
        Collection of stopwords to remove.
    output_column : str | None, default=None
        Name of the output column. If ``None``, the source column is
        overwritten.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with stopwords removed from the
        selected column.

    Raises
    ------
    KeyError
        If ``column`` does not exist in the DataFrame.
    TypeError
        If the column contains non-string, non-null values.
    """
    if column not in df.columns:
        raise KeyError(f"Column not found: {column}")

    non_string_mask = df[column].notna() & ~df[column].map(
        lambda value: isinstance(value, str)
    )
    if non_string_mask.any():
        raise TypeError(
            f"Column '{column}' contains non-string values and cannot be processed."
        )

    stopword_set = {word.lower() for word in stop_words}
    target_column = output_column or column
    result = df.copy()

    result[target_column] = result[column].map(
        lambda text: (
            " ".join(
                token for token in text.split() if token.lower() not in stopword_set
            )
            if pd.notna(text)
            else text
        )
    )

    return result


@curry
def apply_snowball_stemming(
    df: pd.DataFrame,
    column: str,
    language: LANGUAGES,
    output_column: str | None = None,
    ignore_stopwords: bool = False,
) -> pd.DataFrame:
    """
    Apply Snowball stemming to a text column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source text column.
    language : str
        Language used by the Snowball stemmer.
    output_column : str | None, default=None
        Name of the output column. If ``None``, the source column is
        overwritten.
    ignore_stopwords : bool, default=False
        Whether stopwords should be ignored by the stemmer.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with stemmed text.

    Raises
    ------
    KeyError
        If ``column`` does not exist in the DataFrame.
    TypeError
        If the column contains non-string, non-null values.
    ValueError
        If ``language`` is not supported by NLTK SnowballStemmer.
    """
    if column not in df.columns:
        raise KeyError(f"Column not found: {column}")

    non_string_mask = df[column].notna() & ~df[column].map(
        lambda value: isinstance(value, str)
    )
    if non_string_mask.any():
        raise TypeError(
            f"Column '{column}' contains non-string values and cannot be processed."
        )

    if language not in SnowballStemmer.languages:
        raise ValueError(
            f"Unsupported language '{language}'. "
            f"Supported languages are: {sorted(SnowballStemmer.languages)}."
        )

    stemmer = SnowballStemmer(language, ignore_stopwords=ignore_stopwords)

    result = df.copy()
    target_column = output_column or column

    result[target_column] = result[column].map(
        lambda text: (
            " ".join(stemmer.stem(token) for token in text.split())
            if pd.notna(text)
            else text
        )
    )

    return result
