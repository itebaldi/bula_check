from bula_check.preprocessing.text import normalize_text_whitespace


def test_normalize_text_whitespace():

    assert normalize_text_whitespace("  olá   mundo  ") == "olá mundo"
    assert normalize_text_whitespace("olá\tmundo") == "olá mundo"
    assert normalize_text_whitespace("olá\nmundo") == "olá mundo"
    assert normalize_text_whitespace("olá mundo") == "olá mundo"
    assert normalize_text_whitespace("  ") == ""
    assert normalize_text_whitespace("") == ""
