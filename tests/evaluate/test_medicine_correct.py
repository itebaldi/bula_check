import pytest

from bula_check.agents.nodes import _open_db
from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.evaluate import _medicine_correct

pytestmark = pytest.mark.skipif(
    not DEFAULT_CONFIG["anvisa_db_path"].exists(),
    reason="requer bulas_anvisa.db (rodar da raiz do repo)",
)


@pytest.fixture
def anvisa():
    conn = _open_db(DEFAULT_CONFIG["anvisa_db_path"])
    yield conn
    conn.close()


def test_medicine_correct__exact_substring(anvisa):
    assert _medicine_correct(anvisa, "ibuprofeno", "ibuprofeno", "") is True


def test_medicine_correct__brand_generic_equivalence(anvisa):
    assert (
        _medicine_correct(anvisa, "MENSYVA", "HEMIFUMARATO DE QUETIAPINA", "") is True
    )


def test_medicine_correct__different_drugs_not_equivalent(anvisa):
    assert _medicine_correct(anvisa, "ibuprofeno", "paracetamol", "") is False


def test_medicine_correct__combo_generic_matches_itself(anvisa):
    assert (
        _medicine_correct(
            anvisa,
            "LEVONORGESTREL+ETINILESTRADIOL",
            "LEVONORGESTREL+ETINILESTRADIOL",
            "",
        )
        is True
    )
