from bula_check.preprocessing.text import remove_text_stopwords
from inputs.stopwords import get_portuguese_stopwords

STOP_WORDS = get_portuguese_stopwords()


def test_remove_text_stopwords():
    text = "o rato roeu a roupa do rei de roma"
    expected = "rato roeu roupa rei roma"
    remover = remove_text_stopwords(stop_words=STOP_WORDS)
    assert remover(text) == expected


def test_remove_text_stopwords__is_case_insensitive():
    text = "Um teste com palavras"
    expected = "teste palavras"
    remover = remove_text_stopwords(stop_words=STOP_WORDS)
    assert remover(text) == expected


def test_remove_text_stopwords__with_no_stopwords():
    text = "nenhuma stopword aqui"
    remover = remove_text_stopwords(stop_words=STOP_WORDS)
    assert remover(text) == text
