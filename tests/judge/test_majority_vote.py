from bula_check.judge import majority_vote


def _out(model, verdict_ok, medicine_ok, evidence_ok, judge_verdict, comments=""):
    return {
        "_model": model,
        "verdict_ok": verdict_ok,
        "medicine_ok": medicine_ok,
        "evidence_ok": evidence_ok,
        "judge_verdict": judge_verdict,
        "comments": comments,
    }


def test_majority_vote__unanimous_sim_aprovado():
    outs = [_out(f"m{i}", "sim", "sim", "sim", "confirmed") for i in range(3)]
    block = majority_vote(outs, "gpt+gemini+llama")
    assert block["status"] == "aprovado"
    assert block["verdict_ok"] == "sim"
    assert block["medicine_ok"] == "sim"
    assert block["evidence_ok"] == "sim"
    assert block["validated_by"] == "llm-panel:gpt+gemini+llama"


def test_majority_vote__any_dim_majority_no_reprovado():
    outs = [
        _out("a", "sim", "sim", "sim", "confirmed"),
        _out("b", "não", "não", "sim", "refuted"),
        _out("c", "não", "sim", "sim", "refuted"),
    ]
    block = majority_vote(outs, "p")
    assert block["verdict_ok"] == "não"
    assert block["medicine_ok"] == "sim"
    assert block["status"] == "reprovado"


def test_majority_vote__three_way_split_is_incerto_not_reprovado():
    outs = [
        _out("a", "sim", "sim", "sim", "confirmed"),
        _out("b", "sim", "não", "sim", "confirmed"),
        _out("c", "sim", "incerto", "sim", "confirmed"),
    ]
    block = majority_vote(outs, "p")
    assert block["medicine_ok"] == "incerto"
    assert block["status"] == "aprovado"


def test_majority_vote__verdict_divergence_noted_in_comments():
    outs = [
        _out("a", "sim", "sim", "sim", "confirmed", "achei X"),
        _out("b", "não", "sim", "sim", "refuted", "achei Y"),
        _out("c", "sim", "sim", "sim", "confirmed", "achei Z"),
    ]
    block = majority_vote(outs, "p")
    assert "vereditos" in block["comments"]
    assert "achei X" in block["comments"]
