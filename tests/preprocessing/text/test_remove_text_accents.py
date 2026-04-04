from bula_check.preprocessing.text import remove_text_accents


def test_remove_text_accents():

    assert remove_text_accents("olá mundo") == "ola mundo"
    assert remove_text_accents("pão de açúcar") == "pao de acucar"
    assert remove_text_accents("Coração") == "Coracao"
    assert remove_text_accents("text without accents") == "text without accents"
    assert remove_text_accents("") == ""
    assert remove_text_accents("123!@#") == "123!@#"
