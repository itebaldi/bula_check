from pathlib import Path

from langchain_community.document_loaders import PyPDFLoader


def test_read_pdf():

    path = Path("inputs/bulas/pdf/tylenol_sinus__kenvue_ltda__59748988000114.pdf")

    loader = PyPDFLoader(str(path))

    docs = loader.load()
    x = 10
