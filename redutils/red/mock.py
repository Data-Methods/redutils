"""
This file mocks the database connection to the "emulated" red repo.

Little functionality actually takes place here, but at the very least it supports

- The parameter feature in red by reading/writing to parameters which is actually a
JSON file locally
"""

from typing import List, Tuple, Dict
from ..logger import Logger
import json
import pathlib
import tempfile
import sys
import os


class _MockCursor:
    pfile = open(tempfile.gettempdir() + "/params.json")
    _params: Dict[str, Dict[str, str]] = json.loads(pfile.read())
    pfile.close()

    expfile = open(tempfile.gettempdir() + "/extended_properties.json")
    _extended_properties: Dict[str, Dict[str, str]] = json.loads(expfile.read())
    expfile.close()

    @property
    def params(self):
        return self._params

    def __init__(self) -> None:
        self.logger = Logger(".", "mock_cursor.log").logger("mock_cursor")
        self.mock_returns: List[str] = []

    def execute(self, sql_query: str, *params: str) -> None:
        self.mock_returns.clear()
        for p in params:
            self.mock_returns.append(p)

        self.logger.info(f"Mocking Execution: Query:{sql_query}\nParams: {params}")

    def close(self) -> None:
        self.logger.info("Closing Mock Cursor")

    def fetchone(self) -> Tuple[str, str]:
        idx1 = self.mock_returns[0]
        r = self._params[idx1]
        return r["value"], r["desc"]

    def fetchall(self) -> List[Tuple[str, str]]:
        return [
            (self._params.get(x["value"]), self._params.get(x["desc"]))
            for x in self.mock_returns
        ]

    def extended_properties(self, object_name: str, property_name: str) -> str | None:
        return self._extended_properties.get(object_name, {}).get(property_name, None)


class MockConnection:
    def __init__(self, dns: str) -> None:
        self.dns = dns
        self.logger = Logger(".", "mock_connection.log").logger("mock_connection")

    def cursor(self) -> _MockCursor:
        return _MockCursor()


class SecretServer:
    """
    example_secret

    {
        1234: {
            "username": "myusername",
            "password": "mypassword"
        }
    }
    """

    def __init__(self) -> None:
        try:
            script_path = os.path.dirname(os.path.abspath(sys.argv[0]))
            with open(pathlib.Path(script_path) / "stubs/local-secrets.json") as f:
                self._local_secrets = json.load(f)
        except FileNotFoundError as e:
            print("File Not Found `local-secrets.json` found")
            sys.exit()
        except Exception as e:
            print(f"Uncaught exception encountered: {e}")
            sys.exit()

    def get_password(self, id: int, name: str) -> str:
        return str(self._local_secrets[str(id)][name])


class AzureKeyVault(SecretServer):
    """example_secret
    {
        "secret_name": {
            "value": "decoded_secret_value",
            "content_type": "additional metadata"
        }
    }
    """

    def get_secret(self, secret_name: str) -> str:
        return self._local_secrets[secret_name]["value"]
