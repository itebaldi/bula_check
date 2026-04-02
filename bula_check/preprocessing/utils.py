from collections.abc import Callable
from collections.abc import Sequence
from typing import TypeVar

import pandas as pd

from bula_check.tools import curry

T = TypeVar("T")
R = TypeVar("R")

TextTransform = Callable[[str], T]


def _validate_column_exists(df: pd.DataFrame, column: str) -> None:
    """
    Validate that a DataFrame column exists.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Column name.

    Returns
    -------
    None

    Raises
    ------
    KeyError
        If ``column`` does not exist in the DataFrame.
    """
    if column not in df.columns:
        raise KeyError(f"Column not found: {column}")


def _validate_text_column(df: pd.DataFrame, column: str) -> None:
    """
    Validate that a DataFrame column exists and contains only string or null values.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Column name.

    Returns
    -------
    None

    Raises
    ------
    KeyError
        If ``column`` does not exist in the DataFrame.
    TypeError
        If the column contains non-string, non-null values.
    """
    _validate_column_exists(df, column)

    non_string_mask = df[column].notna() & ~df[column].map(
        lambda value: isinstance(value, str)
    )
    if non_string_mask.any():
        raise TypeError(
            f"Column '{column}' contains non-string values and cannot be processed."
        )


def apply_text_transform(
    df: pd.DataFrame,
    column: str,
    transform: Callable[[str], T],
    output_column: str | None = None,
) -> pd.DataFrame:
    """
    Apply a string transformation function to a text column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source text column.
    transform : Callable[[str], str]
        Function that receives a string and returns a transformed string.
    output_column : str | None, default=None
        Name of the output column. If ``None``, the source column is overwritten.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with the transformed text column.
    """
    _validate_text_column(df, column)
    return apply_column_transform(
        df,
        column,
        transform,
        output_column=output_column,
    )


def _apply_text_transforms(text: str, transforms: Sequence[TextTransform]) -> str:
    """
    Apply a sequence of text transformations to a string.

    Parameters
    ----------
    text : str
        Input text.
    transforms : Sequence[TextTransform]
        Sequence of functions that receive a string and return a string.

    Returns
    -------
    str
        Transformed text.
    """
    result = text
    for transform in transforms:
        result = transform(result)
    return result


@curry
def transform_text_column(
    df: pd.DataFrame,
    column: str,
    transforms: Sequence[TextTransform],
    output_column: str | None = None,
) -> pd.DataFrame:
    """
    Apply a sequence of text transformations to a DataFrame text column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source text column.
    transforms : Sequence[TextTransform]
        Sequence of functions that receive a string and return a string.
    output_column : str | None, default=None
        Name of the output column. If ``None``, the source column is
        overwritten.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with transformed text.
    """
    return apply_text_transform(
        df,
        column,
        lambda text: _apply_text_transforms(text, transforms),
        output_column=output_column,
    )


def apply_column_transform(
    df: pd.DataFrame,
    column: str,
    transform: Callable[[T], R],
    output_column: str | None = None,
) -> pd.DataFrame:
    """
    Apply a transformation function to a DataFrame column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source column.
    transform : Callable[[T], R]
        Function applied to each non-null value in the column.
    output_column : str | None, default=None
        Name of the output column. If ``None``, the source column is overwritten.

    Returns
    -------
    pd.DataFrame
        A copy of the input DataFrame with the transformed column.
    """
    _validate_column_exists(df, column)

    result = df.copy()
    target_column = output_column or column
    result[target_column] = result[column].map(transform, na_action="ignore")
    return result
