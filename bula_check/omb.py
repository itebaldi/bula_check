import os

import requests


# https://portal-obm.saude.gov.br/docs/#endpoints-GETapi-route
def buscar_na_obm(medicine: str) -> list[dict]:
    """
    Busca apresentações comerciais relacionadas a 'Tylenol' na OBM.

    Retorna itens como:
    [
        {
            "NU_APPID": "...",
            "NO_NM": "...",
            "NU_SANREG": "...",
            "CO_SUPPCD": "..."
        }
    ]
    """
    from dotenv import load_dotenv

    load_dotenv()

    api_token = os.environ.get("OBM_TOKEN", "").strip()

    if not api_token:
        raise Exception("Set OBM_TOKEN in .env at repo root or in the environment")

    url = "https://portal-obm.saude.gov.br/api/ampp"
    params = {
        "api_token": api_token,
        "fields": "NU_APPID,NO_NM,NU_SANREG,CO_SUPPCD",
    }

    response = requests.get(
        url,
        params=params,
        headers={"Accept": "application/json"},
        timeout=60,
    )
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError(f"Resposta inesperada da OBM: {data}")

    resultados = [item for item in data if medicine in item.get("NO_NM", "").lower()]

    return resultados
