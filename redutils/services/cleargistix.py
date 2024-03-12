from typing import List

import aiohttp

from ..red import Exit, LEVEL_ERROR, Red
from ..auth.generic import GenericToken
from ..api.templates import AsyncIngestionTemplate


class CleargistixBase(GenericToken, AsyncIngestionTemplate):
    """
    Base class for interacting with the Cleargistix API

    :param token_or_key: (str) - token or key to use for authentication
    :param server_url: (str) - url of the server to connect to

    :return: (CleargistixBase) - instance of the CleargistixBase class

    Authentication is handled by the GenericToken class, requires a Base64 encoded token provided by Cleargistix.
    Ensure header includes the following key,value pair of "token=token_or_key"

    Automatically handles error handling when interacting with Cleargisitx API
    """

    def __init__(self, token_or_key: str, server_url: str) -> None:
        super().__init__(token_or_key)
        self.server = server_url
        self.uri_prefix = "api"

    def setup(self):
        (
            self.set_header("token", f"{self.token}")
            .set_header("Accept", "*/*")
            .set_header("Accept-Encoding", "gzip, deflate, br")
            .set_header("Connection", "keep-alive")
        )

        self.invalid_id = "00000000-0000-0000-0000-000000000000"

    def full_url(self, uri: str):
        return f"{self.server}/{self.uri_prefix}/{uri}"

    async def async_request(
        self,
        session: aiohttp.ClientSession,
        uri,
        method: str = "get",
        verbose: bool = False,
        **method_kwargs,
    ) -> List[dict]:
        # async with aiohttp.ClientSession() as session:

        if "timeout" not in method_kwargs:
            if verbose:
                Red.debug("Timeout not set, setting to default")
            method_kwargs["timeout"] = 60
        if verbose:
            Red.debug(f"Calling {method} with url: {uri} and params: {method_kwargs}")
        async with session.request(
            method, f"{self.full_url(uri)}", headers=self.headers, **method_kwargs
        ) as response:
            response.raise_for_status()
            resp = await response.json()
            if resp.get("IsSuccess") is not True:
                Exit(
                    LEVEL_ERROR,
                    f"Error calling {uri}\nCode:{resp.get('ErrorCode')}\nMessage:{resp.get('ErrorMessage')}",
                )

            jdata = resp.get("json")
            if jdata is None:
                Exit(LEVEL_ERROR, f"No data returned from {uri}")

            return jdata
