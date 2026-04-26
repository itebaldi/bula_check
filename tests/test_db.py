from pathlib import Path

from bula_check.db import search_in_db


def test_search_in_db():
    # search = search_in_db(
    #     db_path=Path("inputs/bula_gratis_crawler/bula_gratis.db"),
    #     keyword="paracetamol",
    #     table_name="bulas",
    # )

    search = search_in_db(
        db_path=Path("inputs/anvisa_crawler/medicamentos.db"),
        keyword="NISTATINA OXIDO DE ZINCO",
        table_name="produto",
    )

    assert search


# NISTATINA OXIDO DE ZINCO
# OXIDO DE ZINCO NISTATINA
# ÓXIDO DE ZINCO, NISTATINA
# ÓXIDO DE ZINCO + NISTATINA
