"""access to secret server within red
"""
import subprocess
import os

from ..red import Exit, LEVEL_ERROR


class SecretServer:
    """
    The Secret Server SDK can be accessed through the command-line
    with the scheduler account in Wherescape Red. The program
    below gets the password to use with APIs, online accounts, and
    other use cases as relevant.

    ```python
    from pygcu.red.secret_server import SecretServer

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
