from pathlib import Path

from nemo.files.html import read_html

from bula_check.bula_gratis import gen_bula_instance

SAVE_JSON = False


def test_gen_bula_instance():
    med_url = "https://bula.gratis/eurofarma_laboratorios_s_a/0/algicod/paciente"
    html = read_html(Path("inputs/bulas/html/algicod.html"))
    instance = gen_bula_instance(med_url, html)

    if SAVE_JSON:
        instance.write_to_json(Path("tests/bulas/algicod.json"))
