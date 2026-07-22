import json

from bula_check.judge import compare_human

_OK = {"verdict_ok": "sim", "medicine_ok": "sim", "evidence_ok": "sim", "status": "aprovado"}


def _write(path, items):
    path.write_text(json.dumps(items), encoding="utf-8")


def test_compare_human__full_agreement(tmp_path):
    judged = tmp_path / "j.json"
    human = tmp_path / "h.json"
    _write(judged, [{"id": "q1", "validation": dict(_OK)}])
    _write(human, [{"id": "q1", "validation": dict(_OK)}])
    result = compare_human(judged, human)
    assert result["n_common"] == 1
    assert result["agreement"]["verdict_ok"] == 1.0
    assert result["agreement"]["status"] == 1.0


def test_compare_human__disagreement_lowers_dimension(tmp_path):
    judged = tmp_path / "j.json"
    human = tmp_path / "h.json"
    _write(judged, [{"id": "q1", "validation": dict(_OK)}])
    _write(
        human,
        [
            {
                "id": "q1",
                "validation": {
                    "verdict_ok": "não",
                    "medicine_ok": "sim",
                    "evidence_ok": "sim",
                    "status": "reprovado",
                },
            }
        ],
    )
    result = compare_human(judged, human)
    assert result["agreement"]["verdict_ok"] == 0.0
    assert result["agreement"]["status"] == 0.0
    assert result["agreement"]["medicine_ok"] == 1.0


def test_compare_human__ignores_items_without_both_blocks(tmp_path):
    judged = tmp_path / "j.json"
    human = tmp_path / "h.json"
    _write(
        judged,
        [{"id": "q1", "validation": dict(_OK)}, {"id": "q2", "validation": dict(_OK)}],
    )
    _write(
        human,
        [{"id": "q1", "validation": dict(_OK)}, {"id": "q2", "validation": None}],
    )
    result = compare_human(judged, human)
    assert result["n_common"] == 1
