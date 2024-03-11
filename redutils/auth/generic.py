"""Implementation of a generic authentication protocol consisting of
special token or password embedded in the header of a request.
"""

import requests
import aiohttp
from typing import Any, Callable, Dict, Optional
from typing_extensions import Self
from faker import Faker
from ..red import Exit, Red, LEVEL_ERROR


class GenericToken:
    """
    Implementation of a generic authentication protocol consisting of
    special token or password embedded in the header of a request.
    """

    def __init__(self, token_or_key: str) -> None:
        self._token = token_or_key
        self._headers = {"User-Agent": Faker().user_agent()}
        self.setup()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    async def call(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        **params: dict[str, Any],
    ) -> aiohttp.ClientResponse:
        """
        :param func: (Callable) - function to call
        :param url: (str) - url to call
        :param params: (dict) - parameters to pass to function

        :return: (requests.Response) - response from function call
        """

        Red.debug(f"Calling {method} with url: {url} and params: {params}")
        try:
            match method:
                case "get":
                    async with session.get(
                        url, headers=self.headers, **params
                    ) as response:
                        response.raise_for_status()
                        return response
                case "post":
                    async with session.post(
                        url, headers=self.headers, **params
                    ) as response:
                        response.raise_for_status()
                        return response
            Exit(LEVEL_ERROR, f"Invalid method {method} provided")

        except Exception as e:
            Exit(
                LEVEL_ERROR,
                f"Error calling {method} with url: {url} and params: {params}: {e}",
            )

    def set_header(self, key: str, value: str) -> Self:
        """
        :param key: (str) - key to set
        :param value: (str) - value to set
        """

        self._headers[key] = value
        return self

    @property
    def token(self):
        return self._token

    @property
    def headers(self):
        return self._headers

    def setup(self):
        pass
