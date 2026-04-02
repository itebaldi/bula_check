import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from bula_check.preprocessing.dataframe import create_bag_of_words_matrix


def test_create_bag_of_words_matrix_unigrams():
    data = {"text": ["a rose is a rose", "is it a flower"]}
    df = pd.DataFrame(data)

    result_df = create_bag_of_words_matrix(df, column="text")

    expected_data = {
        "a": [2, 1],
        "rose": [2, 0],
        "is": [1, 1],
        "it": [0, 1],
        "flower": [0, 1],
    }
    expected_df = pd.DataFrame(expected_data).sort_index(axis=1)
    assert_frame_equal(result_df.sort_index(axis=1), expected_df)


def test_create_bag_of_words_matrix_ngrams():
    data = {"text": ["this is a test", "this is not"]}
    df = pd.DataFrame(data)

    result_df = create_bag_of_words_matrix(df, column="text", ngram_range=(1, 2))

    expected_data = {
        "this": [1, 1],
        "is": [1, 1],
        "a": [1, 0],
        "test": [1, 0],
        "not": [0, 1],
        "this is": [1, 1],
        "is a": [1, 0],
        "a test": [1, 0],
        "is not": [0, 1],
    }
    expected_df = pd.DataFrame(expected_data).sort_index(axis=1)
    assert_frame_equal(result_df.sort_index(axis=1), expected_df)


def test_create_bag_of_words_matrix_with_preserve_columns():
    data = {"text": ["doc one", "doc two"], "id": [1, 2]}
    df = pd.DataFrame(data)

    result_df = create_bag_of_words_matrix(
        df, column="text", preserve_columns=["id"]
    )

    expected_data = {"id": [1, 2], "doc": [1, 1], "one": [1, 0], "two": [0, 1]}
    expected_df = pd.DataFrame(expected_data)

    result_df = result_df.sort_index(axis=1)
    expected_df = expected_df.sort_index(axis=1)

    assert_frame_equal(result_df, expected_df)


def test_create_bag_of_words_matrix_with_nan_values():
    data = {"text": ["some text", None, "more text"], "id": [1, 2, 3]}
    df = pd.DataFrame(data)

    result_df = create_bag_of_words_matrix(
        df, column="text", preserve_columns=["id"]
    )

    expected_data = {
        "id": [1, 2, 3],
        "some": [1, 0, 0],
        "text": [1, 0, 1],
        "more": [0, 0, 1],
    }
    expected_df = pd.DataFrame(expected_data).sort_index(axis=1)
    assert_frame_equal(result_df.sort_index(axis=1), expected_df)


def test_create_bag_of_words_matrix_raises_error_for_missing_column():
    data = {"text": ["a b c"]}
    df = pd.DataFrame(data)

    with pytest.raises(KeyError, match="Column not found: non_existent_column"):
        create_bag_of_words_matrix(df, column="non_existent_column")


def test_create_bag_of_words_matrix_raises_error_for_missing_preserve_column():
    data = {"text": ["a b c"]}
    df = pd.DataFrame(data)

    with pytest.raises(KeyError, match="Columns not found: \\['missing'\\]"):
        create_bag_of_words_matrix(df, column="text", preserve_columns=["missing"])


def test_create_bag_of_words_matrix_raises_error_for_non_string_column():
    data = {"text": [1, 2, 3]}
    df = pd.DataFrame(data)

    with pytest.raises(TypeError, match="Column 'text' contains non-string values"):
        create_bag_of_words_matrix(df, column="text")
