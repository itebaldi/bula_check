from bula_check.judge import _parse_json


def test_parse_json__plain_object():
    assert _parse_json('{"a": 1}') == {"a": 1}


def test_parse_json__fenced_with_json_language():
    assert _parse_json('```json\n{"verdict": "confirmed"}\n```') == {
        "verdict": "confirmed"
    }


def test_parse_json__fenced_without_language():
    assert _parse_json('```\n{"a": 2}\n```') == {"a": 2}


def test_parse_json__surrounding_whitespace():
    assert _parse_json('  \n {"a": 3}\n ') == {"a": 3}
