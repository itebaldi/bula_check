import pytest

from bula_check.agents.nodes import _open_db
from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.judge import fetch_section_context

pytestmark = pytest.mark.skipif(
    not DEFAULT_CONFIG["bulagratis_db_path"].exists(),
    reason="requer bulas_gratis.db (rodar da raiz do repo)",
)


@pytest.fixture
def conn():
    connection = _open_db(DEFAULT_CONFIG["bulagratis_db_path"])
    yield connection
    connection.close()


def test_fetch_section_context__marks_cited_chunk(conn):
    cited = "202eadd0-7d64-489c-b687-d82bc606c2f7"
    text, cited_id = fetch_section_context(conn, [cited], ["indications"])
    assert cited_id == cited
    assert cited in text
    assert "TRECHO CITADO" in text


def test_fetch_section_context__missing_chunk_returns_empty(conn):
    text, cited_id = fetch_section_context(conn, ["nao-existe"], ["indications"])
    assert text == ""
    assert cited_id is None


def test_fetch_section_context__empty_ids_returns_empty(conn):
    assert fetch_section_context(conn, [], ["indications"]) == ("", None)
