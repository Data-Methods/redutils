""" Implementation for RestAPI OAuth2.0 protocol
 
"""

from pathlib import Path
from datetime import datetime
import traceback
import tempfile
import time
from typing import Callable, Dict, Optional
from typing_extensions import Self
import requests


from ..red import Exit, Red, LEVEL_ERROR, LEVEL_CRITICAL


class OAuthApi:
    """
    RestAPI implementation of OAuth2.0.

    This class is designed to be subclassed around a specific data source.
    Manages two sessions one for authentication and token refreshing and the other
    is used as the base URL for api calls. Built-in token refresh and auto timeout.

    Example Usage:

    .. code-block:: python

        from pygcu.auth import OAuthApi

        class ExampleDataSourceUsing(OAuthApi):
            def setup(self):
                self.authorize_url = "https://example.com/token"
                self.endpoint_url = "https://example.com/api"

            def get_all_the_things(self):
                resp = self._main_session.get(
                    "https://example.com/api/get_all_the_things",
                    headers={"Authorization": f"Bearer {self._api_token}"}
                )

                if resp.status_code != 200:
                    print("invalid response")
                return resp.json()

        client_id = "xxxxxxx"
        client_secret = "xxxxxxx"

        with ExampleDataSourceUsing(client_id, client_secret) as src:

            # use built-in methods
            resp = src.smart_call(
                func=src._main_session.get,
                url="https://example/api/get_all_the_things",
                headers={"Authorization": f"Bearer {src._api_token}}
            )

            ## or.. create your own
            resp = src.get_all_the_things()

    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        grant_type: str = "client_credentials",
        timeout: int = 600,
    ):
        """
        :param client_id: (str) - client id or username
        :param client_secret: (str) - client secret or password
        :param grant_type: (str) - grant_type, defaults to 'client_credentials'
        :param timeout: (int) - time, in seconds, to wait for response timeout, defaults to 600 seconds (10 min)
        """
        self.client_id: str = client_id
        self.client_secret: str = client_secret
        self.authorize_url: Optional[str] = None
        self.endpoint_url: Optional[str] = None
        self._timeout: int = timeout
        self._main_session: requests.Session = requests.Session()
        self._auth_session: requests.Session = requests.Session()

        self._auth_data: Dict[str, str] = {
            "grant_type": grant_type,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        self._api_token: Optional[str] = None
        self._api_expires_in: Optional[int] = None
        self._api_internal_timer: Optional[datetime] = None
        self._cache_dir: Path = Path(tempfile.gettempdir())

        self.setup()

        if self.authorize_url is None or self.endpoint_url is None:
            Exit(LEVEL_CRITICAL, "Authorize/Endpoint Url is not defined")

    def __enter__(self) -> Self:
        self.refresh_token()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: traceback.TracebackException | None,
    ) -> None:
        self._main_session.close()
        self._auth_session.close()

    def smart_call(
        self, func: Callable[..., requests.Response], url: str, **params: str
    ) -> requests.Response | None:
        """Helper caller blueprint method that handles auto token refresh

        :param func: (callable) - python requests valid methods `[get(), post(), put(), ....]`
        :param url: (str) - the completed url desired to request data from
        :param params: (**kwargs) - key-value pairs valid depending on the underlying requests method.

        :returns: `Response`_

        .. _Response: https://docs.python-requests.org/en/v1.1.0/api/#requests.Response

        """

        if not callable(func):
            Exit(LEVEL_CRITICAL, f"passed func is not callable: {func}")

        # redundant, but handle if internal timer comes close to
        # expiry time
        if self._api_expires_in and self._api_internal_timer:
            if (
                datetime.now() - self._api_internal_timer
            ).seconds >= self._api_expires_in:
                Red.log("Token about to expire...refreshing")
                self.refresh_token(sleep=True)

        try:
            # hard to type this without generics
            # but, `func` expects to be a valid requests HTTP method
            # i.e: `get`, `post`
            Red.log(f"Calling: {url}")
            resp = func(url, timeout=self._timeout, **params)

            if resp.status_code == 401:  # edge case: expired token
                Red.log("Token expired...refreshing")
                self.refresh_token(sleep=True)

                ## TODO: Fix me
                resp = func(url, timeout=self._timeout, **params)

            if 200 > resp.status_code > 299:
                Exit(LEVEL_ERROR, f"Unsuccessful api call: {resp.url}")
            return resp
        except Exception as e:
            Exit(LEVEL_CRITICAL, str(e))
        return None

    def refresh_token(self, sleep: bool = False) -> str | None:
        """Refreshes token based on set value for authorized_url and provided credentials

        :param sleep: (bool, defaults=False) - enforce a thread-stopping wait before refreshing

        :return: str

        """
        if sleep:
            time.sleep(60)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if self._api_internal_timer is None:
            self._api_internal_timer = datetime.now()

        try:
            resp = self._auth_session.post(
                str(self.authorize_url), headers=headers, data=self._auth_data
            )
            if 200 > resp.status_code > 299:
                Exit(LEVEL_ERROR, f"Unable to refresh token: {resp.status_code}")
        except Exception as e:
            Exit(LEVEL_CRITICAL, f"{e}\n{traceback.format_exc()}")

        try:
            payload = resp.json()
            token = payload.get("access_token")
            expires_in = payload.get("expires_in")
        except Exception as e:
            Exit(LEVEL_CRITICAL, f"{traceback.format_exc()}")

        if not token or not expires_in:
            Exit(LEVEL_ERROR, "Refresh token failed")

        self._api_token = token
        self._api_expires_in = expires_in
        self._api_internal_timer = datetime.now()
        Red.log("Token refreshed")
        return self._api_token

    def setup(self) -> None:
        """Overload this function on custom classes to setup object"""
