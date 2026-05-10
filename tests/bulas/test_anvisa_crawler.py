from bula_check.anvisa_crawler import get_by_name


def test_anvisa_get_by_name():

    medices = get_by_name(
        name="ALICURA", save_jsons=False, save_sqlite=True, download_pdf=False
    )

    assert medices
