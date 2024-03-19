"""
a python friendly way to interact with BCP (Bulk Copy Program) from Microsoft SQL Server
"""

from pathlib import Path

import shutil
import subprocess


from ..red import Red, Exit, LEVEL_ERROR


def bcp(
    dbtable_or_query: str, direction: str, datafile: str, **kwargs
) -> subprocess.CompletedProcess:
    """
    a python friendly way to interact with BCP (Bulk Copy Program) from Microsoft SQL Server
    Needs to be installed on the system and in the PATH of host system

    :param dbtable_or_query: (str) - database table or query to use
    :param direction: (str) - direction to use
    :param datafile: (str) - path to the data file to use

    :param kwargs: (dict) - additional arguments to pass to BCP

    example usage:

    bcp("mydb.dbo.mytable", "in", Path("mytable.csv"), S=".", T=";", U="sa", P="mypassword", c=None)

    # will generate args:
    # ['bcp', 'mydb.dbo.mytable', 'in', 'mytable.csv', '-S', '.', '-T', ';', '-U', 'sa', '-P', 'mypassword', '-c']
    """
    datafile = Path(datafile)
    if not (bcp := shutil.which("bcp")):
        raise FileNotFoundError("BCP not found, please ensure it is installed")

    if not datafile.exists() or not datafile.is_file():
        raise FileNotFoundError(f"Data file not found: {datafile}")

    args = [
        bcp,
        dbtable_or_query,
        direction,
        str(datafile.absolute()),
    ]

    for k, v in kwargs.items():
        args.append(f"-{k}")
        if v is not None:
            args.append(str(v))

    Red.debug(f"bcp args: {args}")

    try:
        result = subprocess.run(
            args, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        return result

    except subprocess.CalledProcessError as e:
        Red.error(f"bcp failed: {e}")
        Red.error(f"bcp failed: {e.output.decode()}")
        Exit(LEVEL_ERROR)


class BCPBuilder:
    """
    a class to build BCP commands
    """

    def __init__(self) -> None:
        self.dbtable_or_query = None
        self.direction = None
        self.datafile = None
        self.args = {}

    def _remove_arg(self, k: str):
        """
        remove an argument from the BCP command

        :param k: (str) - key of the argument
        """
        if k in self.args:
            del self.args[k]

    def with_dbtable_or_query(self, dbtable_or_query: str):
        """
        add a database table or query to the BCP command

        :param dbtable_or_query: (str) - database table or query to use
        """
        self.dbtable_or_query = str(dbtable_or_query)
        return self

    def with_direction(self, direction: str):
        if direction not in ["in", "out", "format", "queryout"]:
            Exit(LEVEL_ERROR, f"Invalid direction for BCP: {direction}")
        self.direction = direction
        return self

    def with_datafile(self, datafile: str):
        """
        add a data file to the BCP command

        :param datafile: (str) - data file to use
        """
        self.datafile = str(datafile)
        return self

    def with_user(self, user: str):
        """
        add a user to the BCP command

        :param user: (str) - user to use
        """
        self.add_arg("U", user)
        return self

    def with_password(self, password: str):
        """
        add a password to the BCP command

        :param password: (str) - password to use
        """
        self.add_arg("P", password)
        return self

    def with_server(self, server: str):
        """
        add a server to the BCP command

        :param server: (str) - server to use
        """
        self.add_arg("S", server)
        return self

    def with_database(self, database: str):
        """
        add a database to the BCP command

        :param database: (str) - database to use
        """
        self.add_arg("d", database)
        return self

    def with_field_terminator(self, terminator: str = "\n"):
        """
        add a field terminator to the BCP command

        :param terminator: (str) - field terminator to use
        """
        self.add_arg("t", terminator)
        return self

    def with_row_terminator(self, terminator: str = "\n"):
        """
        add a row terminator to the BCP command

        :param terminator: (str) - row terminator to use
        """
        self.add_arg("r", terminator)
        return self

    def with_character_type(self):
        """
        add a character type to the BCP command
        """
        self.add_arg("c")
        if "w" in self.args:
            self._remove_arg("w")
            Red.warning(
                "Character type and wide character type cannot be used together, using character type"
            )
        return self

    def with_wide_character_type(self):
        """
        add a wide character type to the BCP command
        """
        self.add_arg("w")
        if "c" in self.args:
            self._remove_arg("c")
            Red.warning(
                "Character type and wide character type cannot be used together, using wide character type"
            )
        return self

    def add_arg(self, k: str, v: str = None):
        """
        add an argument to the BCP command

        :param k: (str) - key of the argument
        :param v: (str) - value of the argument
        """
        self.args[k] = v
        return self

    def build(self) -> subprocess.CompletedProcess:
        """
        build the BCP command

        :return: (subprocess.CompletedProcess) - the result of the BCP command
        """
        if not self.dbtable_or_query:
            Exit(LEVEL_ERROR, "dbtable_or_query not set")
        if not self.direction:
            Exit(LEVEL_ERROR, "direction not set")
        if not self.datafile:
            Exit(LEVEL_ERROR, "datafile not set")

        return bcp(self.dbtable_or_query, self.direction, self.datafile, **self.args)
