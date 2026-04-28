import os
from typing import Any
from typing import Mapping

import requests
from requests import Response


class DeCSC:
    """
    This client provides a minimal wrapper around the official DeCS API
    endpoints used to search descriptors by words, boolean expressions,
    and hierarchical tree identifiers.

    Parameters
    ----------
    api_key : str | None, optional
        API key used to authenticate requests to the DeCS API.
        If not provided, the value will be read from the
        ``DECS_API_KEY`` environment variable.
    timeout : float, default=20.0
        Request timeout in seconds.

    Raises
    ------
    ValueError
        If no API key is provided and the ``DECS_API_KEY`` environment
        variable is not defined.

    Notes
    -----
    The DeCS API requires authentication through the ``apikey`` header.
    The base URL used by this client is::

        https://api.bvsalud.org/decs/v2
    """

    BASE_URL: str = "https://api.bvsalud.org/decs/v2"

    def __init__(self, api_key: str | None = None, timeout: float = 20.0) -> None:
        resolved_api_key = api_key or os.getenv("DECS_API_KEY")
        if resolved_api_key is None:
            raise ValueError(
                "DeCS API key not found. Provide `api_key` or set "
                "the `DECS_API_KEY` environment variable."
            )

        self.api_key: str = resolved_api_key
        self.timeout: float = timeout

    def _build_url(self, endpoint: str) -> str:
        """
        Build the full URL for a DeCS API endpoint.

        Parameters
        ----------
        endpoint : str
            API endpoint path starting with ``/``.

        Returns
        -------
        str
            Full URL for the requested endpoint.
        """
        return f"{self.BASE_URL}{endpoint}"

    def _headers(self) -> dict[str, str]:
        """
        Build request headers.

        Returns
        -------
        dict[str, str]
            Headers required by the DeCS API.
        """
        return {"apikey": self.api_key}

    def _request(
        self,
        endpoint: str,
        params: Mapping[str, str],
    ) -> dict[str, Any]:
        """
        Perform a GET request to the DeCS API.

        Parameters
        ----------
        endpoint : str
            API endpoint path.
        params : Mapping[str, str]
            Query parameters sent with the request.

        Returns
        -------
        dict[str, Any]
            Parsed JSON response from the API.

        Raises
        ------
        requests.HTTPError
            If the HTTP response contains an unsuccessful status code.
        requests.RequestException
            If a network-related error occurs.
        ValueError
            If the API response cannot be decoded as JSON.
        """
        response: Response = requests.get(
            self._build_url(endpoint),
            headers=self._headers(),
            params=dict(params),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def search_by_words(self, words: str, lang: str = "pt") -> dict[str, Any]:
        """
        Search DeCS descriptors by words.

        Parameters
        ----------
        words : str
            Words to search for.
        lang : str, default="pt"
            Language code for the search. Common values include
            ``"pt"``, ``"en"``, ``"es"``, and ``"fr"``.

        Returns
        -------
        dict[str, Any]
            JSON response returned by the API.

        Notes
        -----
        According to the DeCS API documentation, words in this
        endpoint are internally combined with logical AND.
        """
        return self._request(
            "/search-by-words",
            {
                "words": words,
                "lang": lang,
                "format": "json",
            },
        )

    def search_boolean(self, expression: str, lang: str = "pt") -> dict[str, Any]:
        """
        Search DeCS descriptors using a boolean expression.

        Parameters
        ----------
        expression : str
            Boolean expression to execute, such as
            ``"paracetamol OR acetaminophen"``.
        lang : str, default="pt"
            Language code for the search.

        Returns
        -------
        dict[str, Any]
            JSON response returned by the API.
        """
        return self._request(
            "/search-boolean",
            {
                "bool": expression,
                "lang": lang,
                "format": "json",
            },
        )

    def get_tree(self, tree_id: str, lang: str = "pt") -> dict[str, Any]:
        """
        Retrieve DeCS information by hierarchical tree identifier.

        Parameters
        ----------
        tree_id : str
            Hierarchical tree identifier.
        lang : str, default="pt"
            Language code for the search.

        Returns
        -------
        dict[str, Any]
            JSON response returned by the API.
        """
        return self._request(
            "/get-tree",
            {
                "tree_id": tree_id,
                "lang": lang,
                "format": "json",
            },
        )


# t = {
#     "attr": {"service": "", "tree_id": "D01.362.635.600"},
#     "record_list": {
#         "record": {
#             "allowable_qualifier_list": [
#                 {"allowable_qualifier": "AD", "attr": {"id": "22000"}},
#                 {"allowable_qualifier": "AE", "attr": {"id": "22020"}},
#                 {"allowable_qualifier": "AG", "attr": {"id": "32387"}},
#                 {"allowable_qualifier": "AI", "attr": {"id": "22006"}},
#                 {"allowable_qualifier": "AN", "attr": {"id": "22002"}},
#                 {"allowable_qualifier": "BL", "attr": {"id": "22062"}},
#                 {"allowable_qualifier": "CF", "attr": {"id": "22043"}},
#                 {"allowable_qualifier": "CH", "attr": {"id": "29165"}},
#                 {"allowable_qualifier": "CL", "attr": {"id": "22011"}},
#                 {"allowable_qualifier": "CS", "attr": {"id": "22065"}},
#                 {"allowable_qualifier": "EC", "attr": {"id": "22018"}},
#                 {"allowable_qualifier": "HI", "attr": {"id": "22034"}},
#                 {"allowable_qualifier": "IM", "attr": {"id": "22038"}},
#                 {"allowable_qualifier": "IP", "attr": {"id": "22001"}},
#                 {"allowable_qualifier": "ME", "attr": {"id": "22044"}},
#                 {"allowable_qualifier": "PD", "attr": {"id": "22078"}},
#                 {"allowable_qualifier": "PK", "attr": {"id": "22079"}},
#                 {"allowable_qualifier": "PO", "attr": {"id": "22025"}},
#                 {"allowable_qualifier": "RE", "attr": {"id": "22022"}},
#                 {"allowable_qualifier": "SD", "attr": {"id": "22055"}},
#                 {"allowable_qualifier": "ST", "attr": {"id": "22048"}},
#                 {"allowable_qualifier": "TO", "attr": {"id": "22068"}},
#                 {"allowable_qualifier": "TU", "attr": {"id": "22073"}},
#                 {"allowable_qualifier": "UR", "attr": {"id": "22050"}},
#             ],
#             "attr": {"db": "decs", "lang": "pt", "mfn": "9770"},
#             "definition": {
#                 "occ": {
#                     "attr": {
#                         "n": "Óxido de nitrogênio (NO2). Um gás altamente venenoso cuja exposição produz inflamação dos pulmões causando uma leve dor ou mesmo passando despercebida, porém levando a um edema pulmonar muitos dias depois que pode causar a morte. É um dos principais poluentes da atmosfera, responsável por absorver os raios ultravioleta que não chegam a superfície da terra."
#                     }
#                 }
#             },
#             "descriptor_list": [
#                 {"attr": {"lang": "en"}, "descriptor": "Nitrogen Dioxide"},
#                 {"attr": {"lang": "es"}, "descriptor": "Dióxido de Nitrógeno"},
#                 {"attr": {"lang": "pt-br"}, "descriptor": "Dióxido de Nitrogênio"},
#                 {"attr": {"lang": "fr"}, "descriptor": "Dioxyde d'azote"},
#                 {"attr": {"lang": "es-es"}, "descriptor": "dióxido de nitrógeno"},
#             ],
#             "entry_combination_list": [],
#             "indexing_annotation": "um gas altamente venenoso",
#             "pharmacological_action_list": [
#                 {
#                     "attr": {"lang": "pt"},
#                     "pharmacological_action": "Oxidantes Fotoquímicos",
#                 }
#             ],
#             "see_related_list": [],
#             "synonym_list": [],
#             "tree_id_list": [
#                 {"tree_id": "D01.362.635.600"},
#                 {"tree_id": "D01.625.550.525"},
#                 {"tree_id": "D01.650.550.587.625"},
#                 {"tree_id": "SP4.606.806.945.200.202"},
#             ],
#             "unique_identifier_nlm": "D009585",
#         }
#     },
#     "tree": {
#         "ancestors": {
#             "attr": {"lang": "pt"},
#             "term_list": [
#                 {"attr": {"tree_id": "D"}, "term": "COMPOSTOS QUÍMICOS E DROGAS"},
#                 {"attr": {"tree_id": "D01"}, "term": "Compostos Inorgânicos"},
#                 {"attr": {"tree_id": "D01.362"}, "term": "Gases"},
#                 {"attr": {"tree_id": "D01.362.635"}, "term": "Óxidos de Nitrogênio"},
#                 {"attr": {"tree_id": "D"}, "term": "COMPOSTOS QUÍMICOS E DROGAS"},
#                 {"attr": {"tree_id": "D01"}, "term": "Compostos Inorgânicos"},
#                 {"attr": {"tree_id": "D01.625"}, "term": "Compostos de Nitrogênio"},
#                 {"attr": {"tree_id": "D01.625.550"}, "term": "Óxidos de Nitrogênio"},
#                 {"attr": {"tree_id": "D"}, "term": "COMPOSTOS QUÍMICOS E DROGAS"},
#                 {"attr": {"tree_id": "D01"}, "term": "Compostos Inorgânicos"},
#                 {"attr": {"tree_id": "D01.650"}, "term": "Compostos de Oxigênio"},
#                 {"attr": {"tree_id": "D01.650.550"}, "term": "Óxidos"},
#                 {
#                     "attr": {"tree_id": "D01.650.550.587"},
#                     "term": "Óxidos de Nitrogênio",
#                 },
#                 {"attr": {"tree_id": "SP"}, "term": "SAÚDE PÚBLICA"},
#                 {"attr": {"tree_id": "SP4"}, "term": "Saúde Ambiental"},
#                 {"attr": {"tree_id": "SP4.606"}, "term": "Meio Ambiente"},
#                 {"attr": {"tree_id": "SP4.606.806"}, "term": "Poluição Ambiental"},
#                 {
#                     "attr": {"tree_id": "SP4.606.806.945"},
#                     "term": "Poluentes Ambientais",
#                 },
#                 {
#                     "attr": {"tree_id": "SP4.606.806.945.200"},
#                     "term": "Poluentes Atmosféricos",
#                 },
#             ],
#         },
#         "descendants": {"attr": {"lang": "pt"}, "term_list": []},
#         "following_sibling": {
#             "attr": {"lang": "pt"},
#             "term_list": [
#                 {
#                     "attr": {"leaf": "true", "tree_id": "D01.362.635.625"},
#                     "term": "Óxido Nitroso",
#                 }
#             ],
#         },
#         "preceding_sibling": {"attr": {"lang": "pt"}, "term_list": []},
#         "self": {
#             "attr": {"lang": "pt"},
#             "term_list": [
#                 {
#                     "attr": {"leaf": "true", "tree_id": "D01.362.635.600"},
#                     "term": "Dióxido de Nitrogênio",
#                 }
#             ],
#         },
#     },
# }
