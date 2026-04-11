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
