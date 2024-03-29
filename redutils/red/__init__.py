"""Wherescape Red specific functions, utilities, and logic."""

from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol
from logging import DEBUG, INFO, WARN, ERROR

import sys
import pyodbc
import json
import pathlib
import os
import logging

logging.basicConfig(level=logging.CRITICAL)


LEVEL_CRITICAL = -3
LEVEL_ERROR = -2
LEVEL_WARNING = -1
LEVEL_SUCCESS = 1


class WherescapeProtocol(Protocol):
    """
    Interface for Wherescape database object
    """

    # params: Dict[str, RedParameter]

    def connect(self, dsn: str, autocommit: bool) -> None: ...

    def execute(self, sql_query: str, *params: Any) -> pyodbc.Cursor: ...

    def ws_parameter_read(self, parameter: str, refresh: bool) -> "RedParameter": ...

    def ws_parameter_write(self, param: "RedParameter") -> None: ...

    def ws_extended_property(
        self, object_name: str, property_name
    ) -> Optional[str]: ...


class RedParameter:
    """container class to hold red parameters

    :param name: (str) - name of parameter
    :param value: (str) - value of parameter
    :param desc: (str, optional) - description of parameter



    ```python
    from pygcu.red import RedParameter

    rp = RedParameter("FOO", "BAR", "Bar of Foo")

    print(rp) # $PFOO$
    ```

    """

    def __init__(self, name: str, value: str, desc: str = "") -> None:
        self.name: str = name
        self.value: str = value
        self.desc: str = desc

    def __str__(self) -> str:
        return f"$P{self.name}$"


class RedReturn:
    """Red logging utility class to capture output in Wherescape Red
    and allow easy exiting of software

    ```python
    from pygcu.red import RedReturn

    Red = RedReturn()
    Exit = Red.rreturn
    Red.log("this is a log")
    Red.info("this is some info")

    if do_something():
        Exit(LEVEL_SUCCESS, "done and return")
    else:
        Exit(LEVEL_ERROR, "something went wrong")

    ```

    ```
    This following code will generate the following output as a string

    1 # or -2 if error
    (2) done and return
    (0) this is a log
    (1) this is some info
    ```

    """

    DEBUG = DEBUG
    INFO = INFO
    WARN = WARN
    ERROR = ERROR

    def __init__(self) -> None:
        self.msgs: List[str] = []
        self._cntr: int = 0
        self._crash_file: Path = Path(".crash_detected")
        self.flush_each_log: bool = False
        self._log_level = WARN
        self._old_stdout = sys.stdout
        self._old_stderr = sys.stderr
        # self._redirect_stdout() # this doesn't work correctly... yet

    def _redirect_stdout(self) -> None:
        sys.stdout = self
        sys.stderr = self

    def _restore_stdout(self) -> None:
        sys.stdout = self._old_stdout
        sys.stderr = self._old_stderr

    def _valid_code(self, code: int) -> bool:
        """check if a valid return code"""
        if code not in (LEVEL_CRITICAL, LEVEL_ERROR, LEVEL_WARNING, LEVEL_SUCCESS):
            return False
        return True

    def write(self, msg: str) -> None:
        self.log(msg)

    def set_log_level(self, level: int) -> None:
        self._log_level = level

    def log(self, msg: str) -> None:
        msg = f"({self._cntr}) {msg}"
        if self.flush_each_log:
            print("FLUSHING ON: " + msg, flush=True)
            self.msgs.append(msg)
        else:
            self.msgs.append(msg)
        self._cntr += 1

    def debug(self, msg: str) -> None:
        """helper method for debugging info"""
        if self._log_level <= DEBUG:
            self.log(f"DEBUG: {msg}")

    def warn(self, msg: str) -> None:
        """helper method for warn info"""
        if self._log_level <= WARN:
            self.log(f"WARNING: {msg}")

    def error(self, msg: str) -> None:
        """helper method for error info"""
        if self._log_level <= ERROR:
            self.log(f"ERROR: {msg}")

    def info(self, msg: str) -> None:
        """helper method for info info"""
        if self._log_level <= INFO:
            self.log(f"INFO: {msg}")

    def rreturn(self, code: int, msg: str = "") -> None:
        """exits software with a return code

        :param code: (int) - level of return, this will indicate to red whether it is pass or fail
        :param msg: (str) - last message that will be displayed in red

        :return: None

        """
        if not self._valid_code(code):
            raise ValueError("Invalid error code")

        # self._restore_stdout()

        print(code)

        if msg != "":
            self.log(msg)

        # addresses quirk in red: take last element in buffer and insert it into front
        # this will show in red as the final output for the given activity

        if len(self.msgs) > 0:
            self.msgs.insert(0, self.msgs[-1])
            self.msgs = self.msgs[
                :-1
            ]  # cut out last element to remove duplicated value

        msg = "\n".join(self.msgs)
        print(msg)

        if code in (LEVEL_CRITICAL, LEVEL_ERROR):
            self._crash_file.touch(exist_ok=True)
        sys.exit()


class Wherescape:
    """Helper class that communicates with Wherescape database

     :param parameters: (Dict[str, RedParameter]) - A caching system to lazily hold parameters read from Red
     :param conn: (Connection) - pyodbc connection object


     Simple usage:

    ```python
     from pygcu.red import Wherescape

     db = Wherescape()
     db.connect("DNS_For_DB")

     rp = db.ws_parameter_read("Client_Loading_Time")
     ...
     ```
    """

    def __init__(self) -> None:
        # cached parameters
        self.parameters: Dict[str, RedParameter] = {}
        self.conn: Optional[pyodbc.Connection] = None

    def get_env(self, env_name: str) -> str:
        """gets environment variable from system

        :param env_name: (str) - name of environment variable to retrieve

        """
        try:
            return os.environ[env_name]
        except KeyError:
            Exit(LEVEL_ERROR, f"Environment variable {env_name} not set")

    def connect(self, dsn: str, autocommit: bool = True) -> None:
        """connects to red wherescape repo database

        :param dsn: (str) - domain service name of wherescape repo
        :param autocommit: (str, default=True) - If True, instructs the database to commit after each SQL statement

        """
        # pylint: disable=c-extension-no-member

        try:
            self.conn = pyodbc.connect(DSN=dsn, autocommit=autocommit)
        except Exception:
            # try constructing connection string from ENVs that should be set by Red.
            try:
                dsn = self.get_env("WSL_META_DSN")
                user = self.get_env("WSL_META_USER")
                password = self.get_env("WSL_META_PWD")
                db = self.get_env("WSL_META_DB")
                schema = self.get_env("WSL_META_SCHEMA")
                connstring = (
                    f"DSN={dsn};UID={user};PWD={password};DATABASE={db};SCHEMA={schema}"
                )
                self.conn = pyodbc.connect(connstring)
            except Exception as e:
                Exit(LEVEL_CRITICAL, f"An uncaught exception occurred: {e}")

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

    def ws_extended_property(self, object_name: str, property_name) -> Optional[str]:
        """Reads the extended property of an object in Wherescape Red.

        This function reads the extended property of an object in Wherescape Red.

        :param object_name: (str) - the object to read the extended property from

        """

        stmt = (
            "select pv.epv_value "
            "FROM dbo.ws_ext_prop_value pv "
            "JOIN dbo.ws_ext_prop_def pd ON pd.epd_key = pv.epv_def_key "
            "JOIN dbo.ws_load_tab lt ON lt.lt_obj_key = pv.epv_obj_key "
            "WHERE lt.lt_table_name = ? "
            "AND pd.epd_variable_name = ?;"
        )
        cursor = self.execute(stmt, object_name, property_name)
        value = cursor.fetchone()

        if value is None:
            Red.warn(f"Unknown Extended Property for: {object_name}")
            return None

        cursor.close()
        return value[0]


class WherescapeLocal:
    def connect(self, dsn: str, autocommit: bool = True) -> None:
        from .mock import MockConnection

        self.conn = MockConnection(dsn)

    def execute(self, sql_query: str, *params: Any) -> None:
        if not self.conn:
            Exit(LEVEL_ERROR, "Database connection not established.")

            return
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_query)
        except Exception as err:
            Exit(LEVEL_ERROR, f"Error executing routine query: {err}")
        finally:
            cursor.close()

    def ws_parameter_read(self, parameter: str, refresh: bool = False) -> RedParameter:
        values = self.conn.cursor().params[parameter]
        return RedParameter(parameter, values["value"], values["desc"])

    def ws_parameter_write(self, p: RedParameter) -> None:
        cursor = self.conn.cursor()
        parameter = p.name
        cursor.params[parameter]["value"] = p.value
        cursor.params[parameter]["desc"] = p.desc
        pfile = pathlib.Path(r"stubs/params.json")
        json.dump(cursor.params, open(pfile, "w"), indent=4)
        Red.log(f"Writing params file to {pfile.absolute()}")

    def ws_extended_property(self, object_name: str, property_name: str) -> str:
        value = self.conn.cursor().extended_properties(object_name, property_name)
        return value


Red = RedReturn()
Exit = Red.rreturn


class _WherescapeEnvironmentManager:
    def get(self, name: str) -> str:
        try:
            return os.environ[name]
        except KeyError:
            Exit(LEVEL_ERROR, f"Environment variable {name} not set")
        except Exception as e:
            Exit(LEVEL_ERROR, f"An uncaught exception occurred: {e}")


class _WherescapeParameterManager:
    def __init__(self, db: WherescapeProtocol):
        self._params: Dict[str, RedParameter] = {}
        self.db = db

    def get(self, name: str, refresh: bool = False) -> RedParameter | None:
        try:
            param = self.db.ws_parameter_read(name, refresh=refresh)
            self._params[name] = param
            return param
        except Exception as e:
            Exit(LEVEL_ERROR, f"Unable to retrieve parameter: `{name}`; {e}")
        return None

    def set(self, name: str, value: Any) -> None:
        if name not in self._params:
            _ = self.get(name)

        if not isinstance(value, (tuple, list)):
            value, desc = (str(value), "")
        else:
            if len(value) > 2:
                Red.warn("Iterable longer than two elements, ignoring >2")

            if len(value) == 1:
                value, desc = value[0], ""
            else:
                value, desc = value[0], value[1]

        __new_value = self._params[name]
        __new_value.value = value
        __new_value.desc = desc

        # save to py object
        self._params[name] = __new_value

        # update red database
        self.db.ws_parameter_write(__new_value)


class _WherescapeExtendedPropertyManager:
    def __init__(self, db: WherescapeProtocol):
        self._params: Dict[str, RedParameter] = {}
        self.db = db

    def get(self, object_name: str, property_name: str) -> str | None:
        return self.db.ws_extended_property(object_name, property_name)


class WherescapeManager:
    def __init__(self, repo_name: str, use_local_env: bool = False) -> None:
        if use_local_env:
            Red.log("Utilizing local Wherescape emulator")
            self.db: WherescapeProtocol = WherescapeLocal()
        else:
            Red.log(f"Connecting to :{repo_name}")
            self.db = Wherescape()

        self.db.connect(repo_name, autocommit=True)
        self.params = _WherescapeParameterManager(self.db)
        self.envs = _WherescapeEnvironmentManager()
        self.ext_prop = _WherescapeExtendedPropertyManager(self.db)
