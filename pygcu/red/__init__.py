""" Wherescape Red specific functions, utilities, and logic.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
import pyodbc


LEVEL_CRITICAL = -3
LEVEL_ERROR = -2
LEVEL_WARNING = -1
LEVEL_SUCCESS = 1


class RedReturn:
    """Red logging utility class to capture output in Wherescape Red
    and allow easy exiting of software

    .. code-block:: python
        :linenos:

        from pygcu.red import RedReturn

        Red = RedReturn()
        Exit = Red.rreturn
        Red.log("this is a log")
        Red.info("this is some info")

        if do_something():
            Exit(LEVEL_SUCCESS, "done and return")
        else:
            Exit(LEVEL_ERROR, "something went wrong")



    .. code-block:: text

        This following code will generate the following output as a string

        1 # or -2 if error
        (2) done and return
        (0) this is a log
        (1) this is some info

    """

    def __init__(self) -> None:
        self.msgs: List[str] = []
        self._cntr: int = 0
        self._crash_file: Path = Path(".crash_detected")

    def _valid_code(self, code: int) -> bool:
        """check if a valid return code"""
        if code not in (LEVEL_CRITICAL, LEVEL_ERROR, LEVEL_WARNING, LEVEL_SUCCESS):
            return False
        return True

    def log(self, msg: str) -> None:
        """logs message"""
        msg = f"({self._cntr}) {msg}"
        self.msgs.append(msg)
        self._cntr += 1

    def debug(self, msg: str) -> None:
        """helper method for debugging info"""
        self.log(f"DEBUG: {msg}")

    def warn(self, msg: str) -> None:
        """helper method for warn info"""
        self.log(f"WARNING: {msg}")

    def error(self, msg: str) -> None:
        """helper method for error info"""
        self.log(f"ERROR: {msg}")

    def info(self, msg: str) -> None:
        """helper method for info info"""
        self.log(f"INFO: {msg}")

    def rreturn(self, code: int, msg: str = "") -> None:
        """exits software with a return code

        :param code: (int) - level of return, this will indicate to red whether it is pass or fail
        :param msg: (str) - last message that will be displayed in red

        :return: None

        """
        if not self._valid_code(code):
            raise ValueError("Invalid error code")

        print(code)

        if msg != "":
            self.log(msg)

        # addresses quirk in red: take last element in buffer and insert it into front
        # this will show in red as the final output for the given activity
        self.msgs.insert(0, self.msgs[-1])
        self.msgs = self.msgs[:-1]  # cut out last element to remove duplicated value

        msg = "\n".join(self.msgs)
        print(msg)

        if code in (LEVEL_CRITICAL, LEVEL_ERROR):
            self._crash_file.touch(exist_ok=True)
        sys.exit()


class RedParameter:
    """container class to hold red parameters

    :param name: (str) - name of parameter
    :param value: (str) - value of parameter
    :param desc: (str, optional) - description of parameter


    .. code-block:: python
        :linenos:

        from pygcu.red import RedParameter

        rp = RedParameter("FOO", "BAR", "Bar of Foo")

        print(rp) # $PFOO$

    """

    def __init__(self, name: str, value: str, desc: str = "") -> None:
        self.name: str = name
        self.value: str = value
        self.desc: str = desc

    def __str__(self) -> str:
        return f"$P{self.name}$"


class Wherescape:
    """Helper class that communicates with Wherescape database

    :param parameters: (Dict[str, RedParameter]) - A caching system to lazily hold parameters read from Red
    :param conn: (Connection) - pyodbc connection object


    Simple usage:

    .. code-block:: python
        :linenos:

        from pygcu.red import Wherescape

        db = Wherescape()
        db.connect("DNS_For_DB")

        rp = db.ws_parameter_read("Client_Loading_Time")
        ...

    """

    def __init__(self) -> None:
        # cached parameters
        self.parameters: Dict[str, RedParameter] = {}
        self.conn: Optional[pyodbc.Connection] = None

    def connect(self, dsn: str, autocommit: bool = True) -> None:
        """connects to red wherescape repo database

        :param dsn: (str) - domain service name of wherescape repo
        :param autoocommit: (str, default=True) - If True, instructs the database to commit after each SQL statement

        """
        # pylint: disable=c-extension-no-member

        try:
            self.conn = pyodbc.connect(DSN=dsn, autocommit=autocommit)
        except pyodbc.Error as err:
            Exit(
                LEVEL_CRITICAL,
                f"Failed to connect - sql_state: {err.args[0]}\nsql_state_description {err.args[1]}",
            )
        finally:
            Exit(LEVEL_CRITICAL, "An uncaught exception occurred")

    def execute(self, sql_query: str, *params: Any) -> pyodbc.Cursor:
        """
        Executes a SQL query and returns any data from the result

        :param sql_query: (str) - sql query to execute
        :param params: (list) - list of parameters valid for pyodbc

        """
        # pylint: disable=broad-exception-caught
        cursor: Optional[pyodbc.Cursor] = None
        if not self.conn:
            Exit(LEVEL_ERROR, "Database connection not established")

        try:
            cursor = self.conn.cursor()  # type: ignore
            cursor.execute(sql_query, *params)
        except Exception as e:
            Exit(LEVEL_ERROR, f"Error executing query: {e}\n{sql_query}")

        return cursor

    def ws_parameter_read(self, parameter: str, refresh: bool = False) -> RedParameter:
        """Reads a parameter from Wherescape Red.

        THis function calls the built-in WsParameterRead sproc, and
        returns with both the name and description of the parameter.
        Function is case-sensitive and will return None, if not found


        :params parameter: (str) - The parameter to query
        :params refresh: (bool, default=False) - refreshes the parameter value in cache by reading from db

        """

        if self.parameters.get(parameter) and not refresh:
            return self.parameters[parameter]

        stmt = (
            "DECLARE @OUT_NAME VARCHAR(2000);"
            "DECLARE @OUT_DESC VARCHAR(256);"
            "EXEC WsParameterRead ? , @p_value=@OUT_NAME OUTPUT, @p_comment=@OUT_DESC OUTPUT;"
            "SELECT @OUT_NAME, @OUT_DESC;"
        )
        cursor = self.execute(stmt, parameter)
        value, desc = cursor.fetchone()

        if value is None:
            Red.warn(f"Unknown Red Parameter: {parameter}")
        rp = RedParameter(parameter, value, desc)

        cursor.close()

        #  cache parameter
        self.parameters[parameter] = rp
        return rp

    def ws_parameter_write(self, param: RedParameter) -> None:
        """Writes a parameter to Wherescape Red.

        This function simply wraps the execute statement, necessary
        to write a parameter in Wherescape Red, in textwrap dedent to
        remove whitespace in the execute statement.

        :param parameter: (str) - the parameter to write to
        :param value: (str) - the value of the written parameter
        :param desc: (str) - the description field to write information about the value of parameter

        """
        self.execute(
            "EXEC dbo.WsParameterWrite ?, ?, ?", (param.name, param.value, param.desc)
        )


Red = RedReturn()
Exit = Red.rreturn
