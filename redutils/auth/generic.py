"""Implementation of a generic authentication protocol consisting of
special token or password embedded in the header of a request.
"""

import requests
from typing import Any, Callable, Dict, Optional
from typing_extensions import Self
from faker import Faker
from ..red import Exit, Red, LEVEL_ERROR, LEVEL_CRITICAL


class GenericToken:
    """
    Implementation of a generic authentication protocol consisting of
    special token or password embedded in the header of a request.
    """

    def __init__(self, token_or_key: str) -> None:
        self._token = token_or_key
        self._main_session = requests.Session()
        self._headers = {"User-Agent": Faker().user_agent()}
        self.setup()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._main_session.close()

    def call(
        self, func: Callable[..., requests.Response], url: str, **params: dict[str, Any]
    ) -> Optional[requests.Response]:
        """
        :param func: (Callable) - function to call
        :param url: (str) - url to call
        :param params: (dict) - parameters to pass to function

        :return: (requests.Response) - response from function call
        """

        if not callable(func):
            Exit(LEVEL_ERROR, "func is not callable")

        Red.debug(f"Calling {func.__name__} with url: {url} and params: {params}")

        try:
            response = func(url, headers=self.headers, **params)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            Exit(
                LEVEL_ERROR,
                f"Error calling {func.__name__} with url: {url} and params: {params}: {e}",
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
