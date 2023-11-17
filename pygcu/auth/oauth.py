""" logic for different kinds of API authentication protocols
 
"""

from pathlib import Path
from datetime import time, datetime
import traceback
import tempfile
from typing import Dict, Optional
import requests


from ..red import Exit, Red, LEVEL_ERROR, LEVEL_CRITICAL


# pylint: disable=too-many-instance-attributes
# this is just a silly linting check...
class OAuthApi:
    """OAuth2.0 protocol implementation"""

    def __init__(self, client_id: str, client_secret: str, timeout: int = 600):
        self.client_id: str = client_id
        self.client_secret: str = client_secret
        self._authorize_url: Optional[str] = None
        self._endpoint_url: Optional[str] = None
        self._timeout: int = timeout
        self._main_session: requests.Session = requests.Session()
        self._auth_session: requests.Session = requests.Session()

        self._auth_data: Dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        self._api_token: Optional[str] = None
        self._api_expires_in: Optional[int] = None
        self._api_internal_timer: Optional[datetime] = None
        self._cache_dir: Path = Path(tempfile.gettempdir())

        self.setup()

        if self._authorize_url is None or self._endpoint_url is None:
            Exit(LEVEL_CRITICAL, "Authorize/Endpoint Url is not defined")

    def __enter__(self):
        self._refresh_token()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._main_session.close()
        self._auth_session.close()

    def _call_api(self, func, url, **params):
        # because.. python

        if not callable(func):
            Exit(LEVEL_CRITICAL, f"passed func is not callable: {func}")

        # redundant, but handle if internal timer comes close to
        # expiry time
        if self._api_expires_in and self._api_internal_timer:
            if (
                datetime.now() - self._api_internal_timer
            ).seconds >= self._api_expires_in:
                Red.log("Token about to expire...refreshing")
                self._refresh_token(sleep=True)

        try:
            # hard to type this without generics
            # but, `func` expects to be a valid requests HTTP method
            # i.e: `get`, `post`
            Red.log(f"Calling: {url}")
            resp = func(url, timeout=self._timeout, **params)

            if resp.status_code == 401:  # edge case: expired token
                Red.log("Token expired...refreshing")
                self._refresh_token(sleep=True)

                ## TODO: Fix me
                resp = func(url, timeout=self._timeout, **params)

            if 200 > resp.status_code > 299:
                Exit(LEVEL_ERROR, f"Unsuccessful api call: {resp.url}")
            return resp
        except Exception as e:
            Exit(LEVEL_CRITICAL, str(e))

    def _refresh_token(self, sleep=False) -> str:
        if sleep:
            time.sleep(60)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if self._api_internal_timer is None:
            self._api_internal_timer = datetime.now()

        try:
            resp = self._auth_session.post(
                self._authorize_url, headers=headers, data=self._auth_data
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

    def setup(self):
        pass
