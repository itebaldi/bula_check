from pathlib import Path

from toolz.functoolz import pipe

from bula_check.importing import read_csv
from bula_check.utils import map_column_values


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
    )

    assert set(table["sentiment"].unique()) == {"negative", "positive"}
