from collections import Counter
from typing import Any

import pandas as pd

from bula_check.constants import LANGUAGES
from bula_check.preprocessing.text import generate_ngram_range
from bula_check.preprocessing.text import lowercase_text
from bula_check.preprocessing.text import remove_text_punctuation
from bula_check.preprocessing.text import remove_text_stopwords
from bula_check.preprocessing.text import stem_text
from bula_check.preprocessing.utils import _validate_text_column
from bula_check.preprocessing.utils import apply_text_transform
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
        Name of the output column. If ``None``, the source column is overwritten.

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
        Name of the output column. If ``None``, the source column is overwritten.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with the lowercased column.
    """
    return apply_text_transform(
        df,
        column,
        lowercase_text,
        output_column=output_column,
    )


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
        Name of the output column. If ``None``, the source column is overwritten.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with punctuation removed from the selected column.
    """
    return apply_text_transform(
        df,
        column,
        remove_text_punctuation,
        output_column=output_column,
    )


@curry
def remove_stopwords(
    df: pd.DataFrame,
    column: str,
    stop_words: set[str],
    output_column: str | None = None,
) -> pd.DataFrame:
    """
    Remove stop words from a text column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source text column.
    stop_words : Collection[str]
        Collection of stop words to remove.
    output_column : str | None, default=None
        Name of the output column. If ``None``, the source column is overwritten.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with stop words removed from the selected column.
    """
    return apply_text_transform(
        df,
        column,
        lambda text: remove_text_stopwords(text, stop_words),
        output_column=output_column,
    )


@curry
def apply_stemming(
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
    language : LANGUAGES
        Language used by the Snowball stemmer.
    output_column : str | None, default=None
        Name of the output column. If ``None``, the source column is overwritten.
    ignore_stopwords : bool, default=False
        Whether stop words should be ignored by the stemmer.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with stemmed text.
    """
    return apply_text_transform(
        df,
        column,
        lambda text: stem_text(
            text,
            language,
            ignore_stopwords=ignore_stopwords,
        ),
        output_column=output_column,
    )


@curry
def generate_ngrams_column(
    df: pd.DataFrame,
    column: str,
    ngram_range: tuple[int, int] = (1, 1),
    output_column: str | None = None,
) -> pd.DataFrame:
    """
    Generate n-grams from a text column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source text column.
    ngram_range : tuple[int, int], default=(1, 1)
        Inclusive range of n-gram sizes.
    output_column : str, default="ngrams"
        Name of the output column.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with a column of n-gram lists.
    """
    return apply_text_transform(
        df,
        column,
        lambda text: generate_ngram_range(text, ngram_range=ngram_range),
        output_column=output_column,
    )


@curry
def create_bag_of_words_matrix(
    df: pd.DataFrame,
    column: str,
    ngram_range: tuple[int, int] = (1, 1),
    preserve_columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Create a document-term matrix from a text column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source text column.
    ngram_range : tuple[int, int], default=(1, 1)
        Inclusive range of n-gram sizes.
    preserve_columns : list[str] | None, default=None
        List of columns to preserve in the output before the term columns.

    Returns
    -------
    pd.DataFrame
        Document-term matrix where each row represents a document and
        each column represents a term frequency.

    Raises
    ------
    KeyError
        If ``column`` or any preserved column does not exist in the DataFrame.
    """
    _validate_text_column(df, column)

    preserve_columns = preserve_columns or []

    missing_columns = [col for col in preserve_columns if col not in df.columns]
    if missing_columns:
        raise KeyError(f"Columns not found: {missing_columns}")

    term_counts = df[column].map(
        lambda value: (
            Counter(generate_ngram_range(value, ngram_range=ngram_range))
            if pd.notna(value)
            else Counter()
        )
    )

    term_matrix = (
        pd.DataFrame.from_records(term_counts, index=df.index).fillna(0).astype(int)
    )

    if not preserve_columns:
        return term_matrix

    preserved_df = df[preserve_columns].copy()
    return pd.concat(
        [preserved_df.reset_index(drop=True), term_matrix.reset_index(drop=True)],
        axis=1,
    )
