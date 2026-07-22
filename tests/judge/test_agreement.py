from bula_check.judge import agreement


def _o(model, medicine_ok, evidence_ok, judge_verdict, error=False):
    out = {
        "_model": model,
        "medicine_ok": medicine_ok,
        "evidence_ok": evidence_ok,
        "judge_verdict": judge_verdict,
        "verdict_ok": "sim",
        "comments": "",
    }
    if error:
        out["_error"] = "boom"
    return out


def test_agreement__perfect_agreement():
    items = [
        [_o("a", "sim", "sim", "confirmed"), _o("b", "sim", "sim", "confirmed")]
        for _ in range(4)
    ]
    result = agreement(items)
    assert result["n_items_clean"] == 4
    assert result["judge_verdict"]["pairwise_agreement"] == 1.0
    assert result["judge_verdict"]["fleiss_kappa"] == 1.0
    assert result["medicine_ok"]["pairwise_agreement"] == 1.0


def test_agreement__excludes_items_with_error():
    items = [
        [_o("a", "sim", "sim", "confirmed"), _o("b", "sim", "sim", "confirmed")],
        [_o("a", "sim", "sim", "confirmed", error=True), _o("b", "sim", "sim", "confirmed")],
    ]
    result = agreement(items)
    assert result["n_items_clean"] == 1


def test_agreement__full_disagreement_zero_pairwise():
    items = [[_o("a", "sim", "sim", "confirmed"), _o("b", "não", "sim", "refuted")]]
    result = agreement(items)
    assert result["medicine_ok"]["pairwise_agreement"] == 0.0
    assert result["judge_verdict"]["pairwise_agreement"] == 0.0
    assert result["evidence_ok"]["pairwise_agreement"] == 1.0
