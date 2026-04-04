from bula_check.preprocessing.text import replace_text_underscores_with_spaces


def test_replace_text_underscores_with_spaces():
    assert replace_text_underscores_with_spaces("olá_mundo") == "olá mundo"
    assert (
        replace_text_underscores_with_spaces("duplo__underscore")
        == "duplo  underscore"
    )
    assert replace_text_underscores_with_spaces("sem-underscore") == "sem-underscore"
    assert replace_text_underscores_with_spaces("") == ""
