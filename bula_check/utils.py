from typing import Any

import pandas as pd
from toolz import curry


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
