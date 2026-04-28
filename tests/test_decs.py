import os
from pprint import pprint

from dotenv import load_dotenv

from bula_check.decs import DeCSC

load_dotenv()


client = DeCSC(
    api_key=os.environ.get("DECS_API_KEY", "").strip(),
    timeout=20.0,
)


def test_decs__search_by_words():
    result = client.search_by_words("náusea", lang="pt")
    pprint(result)


def test_decs__search_boolean():
    result = client.search_boolean("náusea OR vômito", lang="pt")
    pprint(result)


def test_decs__get_tree():
    result = client.get_tree("C23.888.821.712", lang="pt")
    pprint(result)
