import pandas as pd
from toolz.functoolz import pipe

from bula_check.preprocessing.text import lowercase_text
from bula_check.preprocessing.text import remove_text_punctuation
from bula_check.preprocessing.text import remove_text_stopwords
from bula_check.preprocessing.text import stem_text
from bula_check.preprocessing.utils import transform_text_column
from inputs.stopwords import get_english_stopwords


def test_transform_text_column():
    table = pipe(
        pd.DataFrame(
            {
                "sentence": [
                    "Wow, I loved this place!",
                    "This is another sentence.",
                ],
                "sentiment": [1, 0],
            }
        ),
        transform_text_column(
            column="sentence",
            transforms=[
                lowercase_text,
                remove_text_punctuation,
                remove_text_stopwords(stop_words=get_english_stopwords()),
                stem_text(language="english"),
            ],
        ),
    )

    expected_sentences = ["wow love place", "anoth sentenc"]
    assert table["sentence"].tolist() == expected_sentences

    assert set(table["sentiment"].unique()) == {0, 1}
