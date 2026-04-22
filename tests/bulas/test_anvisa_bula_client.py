from bula_check.anvisa_bula_client import AnvisaBulaClient


def test_anvisa_bula_client():

    client = AnvisaBulaClient()

    # Por nome
    records = client.search("ALICURA")

    # Por número de registro (vindo da OBM)
    # records = client.search_by_registration("1.0236.0161.001-9")

    # Baixar PDF
    client.download_pdf(records[0], folder="pdfs/")
