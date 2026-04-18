from bula_check.bulas_anvisa import AnvisaBularioClient


def test_search():
    client = AnvisaBularioClient(
        timeout=30,
        sleep_between_requests=1.0,
    )

    results = client.search("paracetamol", limit=1, save_json=True)

    for result in results:
        print(result)


def test_save_all():
    client = AnvisaBularioClient(
        timeout=30,
        sleep_between_requests=1.0,
    )

    summary = client.save_all(
        limit=2,
        continue_on_error=True,
        save_logs=True,
        save_json=True,
        save_sqlite=True,
        # Um intervalo curto + um chunk evita dezenas de aberturas Playwright
        # (um por ano) quando vários anos vêm vazios do Bulário.
        chunk_by_year=False,
        publication_start="2022-01-01",
    )

    print(summary.saved)
    print(summary.failure)
