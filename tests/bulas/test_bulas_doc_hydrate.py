from bula_check.bulas_doc_hydrate import _download_pdf_bytes


def test_bulas_doc_hydrate():
    source_url = (
        "https://consultas.anvisa.gov.br/#/medicamentos/1040236?numeroProcesso=25351711335201410",
    )

    _download_pdf_bytes(
        pdf_url="https://consultas.anvisa.gov.br/api/consulta/medicamentos/arquivo/bula/parecer/eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiIyMTIzMzQyNyIsIm5iZiI6MTc3NjUyMTA1MiwiZXhwIjoxNzc2NTIxMzUyfQ.XTxgAAS96ztuFd-3cLQsPOrDr8EAslZ_zdBwJLaUn2Hqnvep-g5q5dDxtG6tshKuRu3oJt6RLW00teMFrIluVA/?Authorization=Guest",
        timeout=10.0,
    )
