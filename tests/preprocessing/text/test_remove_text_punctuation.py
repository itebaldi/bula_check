from bula_check.preprocessing.text import remove_text_punctuation


def test_remove_text_punctuation_with_common_punctuation():
    assert remove_text_punctuation("Olá, mundo!") == "Olá mundo"


def test_remove_text_punctuation_with_various_symbols():
    assert (
        remove_text_punctuation("Isso é um teste... com números: 123.")
        == "Isso é um teste com números 123"
    )


def test_remove_text_punctuation_with_no_punctuation():
    assert remove_text_punctuation("Texto sem pontuacao") == "Texto sem pontuacao"
    assert remove_text_punctuation("") == ""
