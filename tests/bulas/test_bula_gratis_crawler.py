from bula_check.bula_gratis_crawler import get_by_name


def test_bula_gratis_get_by_name():
    result = get_by_name(
        name="paracetamol + fosfato de codeína",
        save_jsons=True,
        save_pdf_json=True,
        save_sqlite=False,
        embed=False,
    )

    assert result
