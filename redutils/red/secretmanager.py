"""access to secret server within red"""

import subprocess
import os
from azure.identity import EnvironmentCredential
from azure.keyvault.secrets import SecretClient
from typing import Any, Optional

from . import Exit, LEVEL_ERROR


class SecretServer:
    """
    The Secret Server SDK can be accessed through the command-line
    with the scheduler account in Wherescape Red. The program
    below gets the password to use with APIs, online accounts, and
    other use cases as relevant.

    ```python
    from pygcu.red.secretmanager import SecretServer

    secret_id = 1234
    secret_type = "username"

    ss = SecretServer()
    decrypted_secret = ss.get_password(secret_id, secret_type)
    ```
    """

    def get_password(self, secret_id: int, secret_type: str) -> str | None:
        """gets password using secret server sdk

        :param secret_id: (int) - id of secret
        :param secret_type: (str) - type of secret

        :return: (str) - decrypted value of secret

        :raises: Exception
        """
        home = os.path.expanduser("~")
        path = os.path.join(home, "SS_SDK")
        os.chdir(path)
        try:
            password = subprocess.run(
                ["tss", "secret", "-s", str(secret_id), "-f", str(secret_type)],
                capture_output=True,
            ).stdout.decode("utf-8")
            return password
        except Exception as err:
            Exit(LEVEL_ERROR, f"Could not get password: {err}")
        return None


class AzureKeyVault:
    """
    Azure Key Vault is a cloud service for securely storing and
    accessing secrets. The program below gets the password to use
    with APIs, online accounts, and other use cases as relevant.

    :param kv_name: (str) - name of key vault
    :param credential: (Any) - credential object based on [valid](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity?view=azure-python) Azure Credentials to access key vault

    ```python
    """

    def __init__(self, kv_name: str, credential: Any):
        self._base_url = f"https://{kv_name}.vault.azure.net/"
        self.client = SecretClient(vault_url=self._base_url, credential=credential)

    def get_secret(self, secret_name: str) -> Optional[str]:
        """gets secret from key vault

        :param secret_name: (str) - name of secret

        :return: (str) - value of secret

        :raises: Exception
        """
        try:
            secret = self.client.get_secret(secret_name)
            return secret.value
        except Exception as err:
            Exit(LEVEL_ERROR, f"Could not get secret: {err}")
        return None

    @staticmethod
    def credential_with_username_and_password(
        client_id: str,
        username: str,
        password: str,
        tenant_id: Optional[str] = None,
        authority_host: Optional[str] = None,
    ) -> EnvironmentCredential:
        """authenticate with username and password

        :param client_id: (str) - client id
        :param username: (str) - username
        :param password: (str) - password
        :param tenant_id: (str) - tenant id
        :param authority_host: (str) - authority host; defaults to "https://login.microsoftonline.com"

        :return: (EnvironmentCredential) - credential object
        """
        return EnvironmentCredential(
            authority=authority_host,
            tenant_id=tenant_id,
            client_id=client_id,
            username=username,
            password=password,
        )
