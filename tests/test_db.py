import sqlite3
from contextlib import closing
from pathlib import Path

from bula_check.db import search_by_filters
from bula_check.db import search_in_db

# Metoprolol — 25 mg / 1 dia
# Olmesartana — 40 mg / 1 dia
# Anlodipina — 5 mg / noite
# Provavelmente é “Amlodipina”, mas a letra parece “Anlodipina”.
# Plenance — 10 mg / dia
# Esse quarto está difícil de ler com certeza. Parece algo como “Plenance” ou “Plenance”.
# Duomo HP — 1 comp / noite


def test_search_in_db__normalized_text():

    db_path = Path("inputs/anvisa_crawler/medicamentos.db")

    with closing(sqlite3.connect(db_path)) as conn:
        search = search_in_db(
            db_connection=conn,
            keyword="NISTATINA OXIDO DE ZINCO",
            table_name="produto",
        )

        assert len(search) == 15

        search = search_in_db(
            db_connection=conn,
            keyword="OXIDO DE ZINCO NISTATINA",
            table_name="produto",
        )

        assert len(search) == 15

        search = search_in_db(
            db_connection=conn,
            keyword="ÓXIDO DE ZINCO, NISTATINA",
            table_name="produto",
        )

        assert len(search) == 15

        search = search_in_db(
            db_connection=conn,
            keyword="oxido de zinco nistatina",
            table_name="produto",
        )

        assert len(search) == 15


def test_search_in_db__search_by_filters():

    db_path = Path("inputs/anvisa_crawler/medicamentos.db")

    with closing(sqlite3.connect(db_path)) as conn:
        search = search_by_filters(
            db_connection=conn,
            table_name="produto",
            filters={"principioAtivo": "NISTATINA, ÓXIDO DE ZINCO"},
        )
        # search = search_in_db(
        #     db_connection=conn,
        #     table_name="produto",
        #     keyword="oxido de zinco nistatina",
        #     columns=["principioAtivo"],
        # )

        assert len(search) == 11
