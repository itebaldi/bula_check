from pathlib import Path

from toolz.functoolz import pipe

from bula_check.importing import read_csv
from bula_check.preprocessing.dataframe import apply_stemming
from bula_check.preprocessing.dataframe import map_column_values
from bula_check.preprocessing.dataframe import remove_punctuation
from bula_check.preprocessing.dataframe import remove_stopwords
from bula_check.preprocessing.dataframe import to_lowercase
from inputs.stopwords import get_english_stopwords


def test_knime_project():

    table = pipe(
        read_csv(
            file_path=Path("inputs/yelp_labelled.txt"),
            separator="\t",
            header=None,
            column_names=["sentence", "sentiment"],
            dtypes={"sentence": "string", "sentiment": "int64"},
        ),
        map_column_values(
            column="sentiment",
            mapping={0: "negative", 1: "positive"},
        ),
        to_lowercase(column="sentence"),
        remove_punctuation(column="sentence"),
        remove_stopwords(column="sentence", stop_words=get_english_stopwords()),
        apply_stemming(column="sentence", language="english"),
    )

    assert set(table["sentiment"].unique()) == {"negative", "positive"}
