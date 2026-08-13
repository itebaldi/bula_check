import pytest

from bula_check.agents.tools import _extract_json


def test_extract_json__plain_object():
    assert _extract_json('{"medicine_name": "dipirona"}') == {
        "medicine_name": "dipirona"
    }


def test_extract_json__fenced_with_json_language():
    raw = '```json\n{"medicine_name": "ibuprofeno"}\n```'
    assert _extract_json(raw) == {"medicine_name": "ibuprofeno"}


def test_extract_json__fenced_without_language():
    assert _extract_json('```\n{"a": 2}\n```') == {"a": 2}


def test_extract_json__prose_around_object():
    raw = 'Claro! Segue o JSON pedido:\n{"a": 1}\nEspero ter ajudado.'
    assert _extract_json(raw) == {"a": 1}


def test_extract_json__reasoning_block_before_object():
    raw = '<think>O usuário citou Tylenol Sinus.</think>\n{"medicine_name": "Tylenol Sinus"}'
    assert _extract_json(raw) == {"medicine_name": "Tylenol Sinus"}


def test_extract_json__reasoning_block_with_braces():
    raw = '<think>Pensei em {"medicine_name": "errado"}</think>{"medicine_name": "certo"}'
    assert _extract_json(raw) == {"medicine_name": "certo"}


def test_extract_json__nested_object():
    raw = '{"a": {"b": [1, 2]}, "c": null}'
    assert _extract_json(raw) == {"a": {"b": [1, 2]}, "c": None}


def test_extract_json__braces_inside_strings():
    raw = '{"original_query": "posso tomar {isso} junto?"}'
    assert _extract_json(raw) == {"original_query": "posso tomar {isso} junto?"}


def test_extract_json__escaped_quote_inside_string():
    raw = '{"original_query": "ele disse \\"pode\\" tomar"}'
    assert _extract_json(raw) == {"original_query": 'ele disse "pode" tomar'}


def test_extract_json__unterminated_reasoning_raises():
    with pytest.raises(ValueError, match="sem JSON"):
        _extract_json('<think>Preciso identificar o medicamento e as seções')


def test_extract_json__empty_content_raises():
    with pytest.raises(ValueError, match="sem JSON"):
        _extract_json("")


def test_extract_json__truncated_object_raises():
    with pytest.raises(ValueError, match="incompleto"):
        _extract_json('{"medicine_name": "dipirona", "sections": ["indica')
