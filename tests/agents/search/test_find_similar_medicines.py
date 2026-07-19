import sqlite3

import pytest

from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.agents.search import find_similar_medicines

pytestmark = pytest.mark.skipif(
    not DEFAULT_CONFIG["bulagratis_db_path"].exists(),
    reason="requer bulas_gratis.db (rodar da raiz do repo)",
)


@pytest.fixture
def conn():
    connection = sqlite3.connect(DEFAULT_CONFIG["bulagratis_db_path"])
    yield connection
    connection.close()


def test_find_similar_medicines__typo_returns_correct_top(conn):
    result = find_similar_medicines(conn, "Mensiva", limit=3)
    assert result
    assert result[0]["medicine"]["name"] == "MENSYVA"
    assert 0.75 <= result[0]["score"] <= 1.0


def test_find_similar_medicines__respects_limit(conn):
    result = find_similar_medicines(conn, "Mensiva", limit=2)
    assert len(result) <= 2


def test_find_similar_medicines__sorted_by_score_desc(conn):
    result = find_similar_medicines(conn, "aceclofenato", limit=3)
    scores = [candidate["score"] for candidate in result]
    assert scores == sorted(scores, reverse=True)


def test_find_similar_medicines__garbage_returns_empty(conn):
    assert find_similar_medicines(conn, "asdkjhqwe zzz", limit=3) == []


def test_find_similar_medicines__empty_name_returns_empty(conn):
    assert find_similar_medicines(conn, "", limit=3) == []
