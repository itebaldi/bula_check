from pathlib import Path

from nemo.files.pdf import read_pdf

from bula_check.bula import gen_sections_from_pdf


def test_read_pdf():

    path = Path("inputs/bulas/pdf/tylenol_sinus__kenvue_ltda__59748988000114.pdf")

    documents = read_pdf(path)

    gen_sections_from_pdf(documents)

    x = 10
