from bula_check.bulas_anvisa import AnvisaBularioClient
from bula_check.omb import buscar_na_obm


def test_omb():

    obm = buscar_na_obm("tylenol")

    client = AnvisaBularioClient(
        timeout=30,
        sleep_between_requests=1.0,
    )

    registration_number = obm[0]["NU_SANREG"]

    # results = client.get_by_registration_number(
    #     registration_number,
    #     # save_json=True,
    #     # save_pdf=True,
    #     # save_sqlite=True,
    # )
