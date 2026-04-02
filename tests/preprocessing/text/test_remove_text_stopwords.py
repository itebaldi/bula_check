from bula_check.preprocessing.text import remove_text_stopwords


def test_remove_text_stopwords_from_text():
    stop_words = {"a", "o", "que", "de"}
    text = "o rato roeu a roupa do rei de roma"
    expected = "rato roeu roupa rei roma"
    remover = remove_text_stopwords(stop_words=stop_words)
    assert remover(text) == expected


def test_remove_text_stopwords_is_case_insensitive():
    stop_words = {"UM", "Com"}
    text = "Um teste com palavras"
    expected = "teste palavras"
    remover = remove_text_stopwords(stop_words=stop_words)
    assert remover(text) == expected


def test_remove_text_stopwords_with_no_stopwords():
    stop_words = {"x", "y", "z"}
    text = "nenhuma stopword aqui"
    remover = remove_text_stopwords(stop_words=stop_words)
    assert remover(text) == text
