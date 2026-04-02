from pathlib import Path

from bula_check.importing import read_csv


def test_read_csv():
    df = read_csv(
        file_path=Path("inputs/yelp_labelled.txt"),
        separator="\t",
        header=None,
        column_names=["sentence", "sentiment"],
        dtypes={"sentence": "string", "sentiment": "int64"},
    )

    assert not df.empty
    assert df.shape == (1000, 2)
    assert all(df.columns == ["sentence", "sentiment"])
    assert df.dtypes["sentence"] == "string"
    assert df.dtypes["sentiment"] == "int64"
