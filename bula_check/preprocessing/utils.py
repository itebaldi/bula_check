from typing import Callable

import pandas as pd


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
    if column not in df.columns:
        raise KeyError(f"Column not found: {column}")

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
    transform: Callable[[str], str],
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

    result = df.copy()
    target_column = output_column or column
    result[target_column] = result[column].map(
        lambda value: transform(value) if pd.notna(value) else value
    )
    return result
