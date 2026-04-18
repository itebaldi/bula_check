from bula_check.bulas_doc_hydrate import _download_pdf_bytes
from bula_check.bulas_pdf import download_patient_pdf_from_anvisa


def test_bulas_doc_hydrate():
    source_url = (
        "https://consultas.anvisa.gov.br/#/medicamentos/1040236?numeroProcesso=25351711335201410",
    )

    _download_pdf_bytes(
        pdf_url="https://consultas.anvisa.gov.br/api/consulta/medicamentos/arquivo/bula/parecer/eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiIyMTIzMzQyNyIsIm5iZiI6MTc3NjUyMTA1MiwiZXhwIjoxNzc2NTIxMzUyfQ.XTxgAAS96ztuFd-3cLQsPOrDr8EAslZ_zdBwJLaUn2Hqnvep-g5q5dDxtG6tshKuRu3oJt6RLW00teMFrIluVA/?Authorization=Guest",
        timeout=10.0,
    )


def test_bulas_df(tmp_path):
    # Bulário devolve idBulaPacienteProtegido; a busca precisa de um nome próximo ao
    # cadastro (processo 25351407569201619 — paracetamol + fosfato de codeína Germed).
    pdf_bytes = download_patient_pdf_from_anvisa(
        source_url="https://consultas.anvisa.gov.br/#/medicamentos/1164911?numeroProcesso=25351407569201619",
        product_id=1164911,
        numero_processo="25351407569201619",
        output_path=tmp_path / "bula_paciente.pdf",
        medication_hint="PARACETAMOL",
    )

    assert pdf_bytes.startswith(b"%PDF")
