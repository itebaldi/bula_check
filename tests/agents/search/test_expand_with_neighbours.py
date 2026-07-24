from bula_check.agents.search import NEIGHBOUR_SCORE
from bula_check.agents.search import _expand_with_neighbours


def _chunk(cid, idx):
    return {
        "id": cid,
        "medicine_id": "m1",
        "medicine_name": "x",
        "section": "indications",
        "paragraph_idx": 0,
        "chunk_idx": idx,
        "text": "",
        "embedding": [0.0],
    }


def test_expand_with_neighbours__neighbours_get_sentinel_score():
    all_chunks = [_chunk("a", 0), _chunk("b", 1), _chunk("c", 2)]
    retrieved = [{"chunk": all_chunks[1], "score": 0.9}]
    out = _expand_with_neighbours(retrieved, all_chunks)
    by = {r["chunk"]["id"]: r["score"] for r in out}
    assert by["b"] == 0.9
    assert by["a"] == NEIGHBOUR_SCORE
    assert by["c"] == NEIGHBOUR_SCORE


def test_expand_with_neighbours__sentinel_is_negative():
    assert NEIGHBOUR_SCORE < 0


def test_expand_with_neighbours__core_zero_survives_filter_neighbour_dropped():
    all_chunks = [_chunk("a", 0), _chunk("b", 1)]
    retrieved = [{"chunk": all_chunks[0], "score": 0.0}]
    out = _expand_with_neighbours(retrieved, all_chunks)
    kept = {r["chunk"]["id"] for r in out if r["score"] >= 0}
    assert kept == {"a"}
