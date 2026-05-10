from bula_check.agents.nodes import _open_db
from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.agents.protocol import ParsedQuery
from bula_check.agents.search import find_medicine_candidates


def test_find_medicine_candidates():
    config = DEFAULT_CONFIG
    query = ParsedQuery(
        medicine_name="Tylenol",
        active_ingredient="paracetamol",
        sections=[],
        expanded_keywords=[],
        claim_type="question",
        original_query="Pode tomar dipirona?",
    )

    # Executa a função a ser testada
    bulagratis_conn = _open_db(config["bulagratis_db_path"])
    anvisa_conn = _open_db(config["anvisa_db_path"])

    result = find_medicine_candidates(
        bulagratis_conn=bulagratis_conn,
        anvisa_conn=anvisa_conn,
        name=query["medicine_name"],
        active_ingredient=query["active_ingredient"],
        cfg=config,
    )

    bulagratis_conn.close()
    anvisa_conn.close()

    # Verifica se o resultado contém os candidatos a medicamentos
    assert "medicine_candidates" in result
