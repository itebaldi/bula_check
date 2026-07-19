import pytest

from bula_check.agents.nodes import _parse_verdict


@pytest.mark.parametrize(
    "text, expected",
    [
        ("**Veredicto:** CONFIRMADA\n\n**Análise:** o remédio serve", "confirmed"),
        ("**Veredicto:** REFUTADA\n\n**Análise:** contradiz", "refuted"),
        ("**Veredicto:** INCONCLUSIVA\n\n**Análise:** sem dados", "inconclusive"),
        ("Veredicto: [INCONCLUSIVA]", "inconclusive"),
        ("Veredito: REFUTADA", "refuted"),
    ],
)
def test_parse_verdict__reads_verdict_line(text, expected):
    assert _parse_verdict(text) == expected


def test_parse_verdict__line_overrides_body_mentions():
    text = "**Veredicto:** INCONCLUSIVA\n\nAnálise: seria REFUTADA se a bula citasse"
    assert _parse_verdict(text) == "inconclusive"


def test_parse_verdict__line_ignores_confirmed_mention_in_body():
    text = "**Veredicto:** REFUTADA\n\nNão é CONFIRMADA pelos trechos."
    assert _parse_verdict(text) == "refuted"


@pytest.mark.parametrize(
    "text, expected",
    [
        ("A alegação é CONFIRMADA pela bula.", "confirmed"),
        ("Os trechos deixam a alegação REFUTADA.", "refuted"),
        ("Não há informação suficiente nos trechos.", "inconclusive"),
        ("", "inconclusive"),
    ],
)
def test_parse_verdict__falls_back_to_full_text(text, expected):
    assert _parse_verdict(text) == expected


def test_parse_verdict__fallback_prefers_confirmed_over_refuted():
    assert _parse_verdict("pode ser CONFIRMADA ou REFUTADA") == "confirmed"
