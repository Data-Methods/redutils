""" OData logic and helper classes"""

from typing import Dict
import urllib


class ODataUrl:
    """
    Converts a simple url string to be easily manipulated to the OData standards.

    Example Usage:

    ```python
    from pygcu.api.odata import ODataUrl

    url = ODataUrl("https://example/api")

    uri = "workers"
    params = {"$skip": 0, "$count": "true", "$filter": "UpdatedDate gt 1990-01-01}

    parsed_url = url.parse(uri, params)
    print(parsed_url)
    # https://example/api/workers?$skip=0&$count=true&$filter=UpdatedDate gt 1990-01-01
    ```


    :TODO: This class needs much building on and made better...
    """

    def __init__(self, base_url: str) -> None:
        """
        :param base_url: (str) - Base url to convert into odata url
        """
        self.base_url = base_url

    def parse(self, uri: str = "/", params: Dict[str, str] = {}) -> str:
        """
        construct valid odata url with given context

        :param uri: (str) - the desired uri ``/workers`` for example without base url
        :param params: (Dict(str, str)) - a dictionary of key/value pairs that must be valid odata language

        :return: str
        """
        url = str(urllib.parse.urljoin(self.base_url, uri))  # type: ignore

        if params:
            query = "?"
            pl = len(params)
            for idx, (k, v) in enumerate(params.items()):
                query += f"{k}={v}"
                if idx != (pl - 1):
                    query += "&"

            url += query
        return url

    def count(self, uri: str = "/") -> str:
        """helper function to return parsed url to get total counts within entity

        :return: str
        """
        url = str(urllib.parse.urljoin(self.base_url, uri))  # type: ignore
        url += "/$count"
        return url
