from bula_check.preprocessing.text import lowercase_text


def test_lowercase_text_with_uppercase():
    assert lowercase_text("Hello World") == "hello world"


def test_lowercase_text_with_mixed_case():
    assert lowercase_text("TeStInG 1, 2, 3") == "testing 1, 2, 3"


def test_lowercase_text_with_no_change():
    assert lowercase_text("already lower") == "already lower"
    assert lowercase_text("") == ""
