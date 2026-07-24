from bula_check.evaluate import _semantic_ir_metrics


def _chunk(cid, section, emb):
    return {"id": cid, "section": section, "embedding": emb}


def test_semantic_ir_metrics__single_gold_retrieved_recall_one():
    gold = _chunk("g", "indications", [1.0, 0.0])
    m = _semantic_ir_metrics(
        retrieved=[gold], gabarito=[gold], threshold=0.8, n_candidates=1
    )
    assert m["semantic_recall"] == 1.0
    assert m["semantic_hit_at_1"] == 1.0
    assert m["semantic_precision"] == 1.0


def test_semantic_ir_metrics__empty_retrieved_recall_zero():
    gold = _chunk("g", "indications", [1.0, 0.0])
    m = _semantic_ir_metrics(
        retrieved=[], gabarito=[gold], threshold=0.8, n_candidates=1
    )
    assert m["semantic_recall"] == 0.0
    assert m["semantic_hit_at_1"] == 0.0


def test_semantic_ir_metrics__different_section_no_match():
    gold = _chunk("g", "indications", [1.0, 0.0])
    other = _chunk("o", "contraindications", [1.0, 0.0])
    m = _semantic_ir_metrics(
        retrieved=[other], gabarito=[gold], threshold=0.8, n_candidates=1
    )
    assert m["semantic_recall"] == 0.0


def test_semantic_ir_metrics__below_threshold_no_match():
    gold = _chunk("g", "indications", [1.0, 0.0])
    weak = _chunk("w", "indications", [0.0, 1.0])
    m = _semantic_ir_metrics(
        retrieved=[weak], gabarito=[gold], threshold=0.8, n_candidates=1
    )
    assert m["semantic_recall"] == 0.0
