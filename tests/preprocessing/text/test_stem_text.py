import pytest
from nltk.stem import SnowballStemmer

from bula_check.preprocessing.text import stem_text


def test_stem_text_portuguese():
    stemmer = stem_text(language="portuguese")
    assert stemmer("correndo corria correrão") == "corr corr corr"
    assert stemmer("amigavelmente amigos") == "amig amig"


def test_stem_text_english():
    stemmer = stem_text(language="english")
    assert stemmer("running runs ran") == "run run ran"
    assert stemmer("beautifully beautiful") == "beauti beauti"


def test_stem_text_unsupported_language():
    with pytest.raises(ValueError) as excinfo:
        stem_text(language="klingon")("some text")  # type: ignore
    assert "Unsupported language 'klingon'" in str(excinfo.value)
    supported = sorted(SnowballStemmer.languages)
    assert str(supported) in str(excinfo.value)
