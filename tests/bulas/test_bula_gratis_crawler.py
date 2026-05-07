from bula_check.bula_gratis_crawler import get_by_name


def test_bula_gratis_get_by_name():

    result = get_by_name(
        name="PARACETAMOL", save_jsons=True, save_sqlite=True, embed=False
    )

    assert True
