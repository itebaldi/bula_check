from bula_check.bulas import BulaGratisClient


def test_search():
    client = BulaGratisClient(
        timeout=30,
        sleep_between_requests=1.0,
    )

    results = client.search("algicod", limit=5, save_json=False)

    for result in results:
        print(result)


def test_save_all_to_json():
    client = BulaGratisClient(
        timeout=30,
        sleep_between_requests=1.0,
    )

    summary = client.save_all_to_json(
        limit=2,
        continue_on_error=True,
        save_logs=False,
    )

    print(summary.saved)
    print(summary.failure)
