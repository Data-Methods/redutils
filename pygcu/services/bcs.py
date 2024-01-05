import base64
import lzma
import pickle
import tempfile
import traceback
import time

import pandas as pd
import numpy as np

from pathlib import Path
from typing import Any, Callable, Tuple, List, Dict, Optional
from datetime import date


from ..red import Red, Exit, LEVEL_CRITICAL, LEVEL_ERROR, WherescapeProtocol
from ..api.templates import IngestionTemplate
from ..api.odata import ODataUrl
from ..auth.oauth import OAuthApi


class BCSApi(OAuthApi, IngestionTemplate):
    """
    Dedicated Logic for the data source NextGen BCS.

    BCS comes with many entities and follows the `OData`_ standard, this makes abstraction very easy and repeatable.

    .. _OData: https://www.odata.org/

    """

    def setup(self) -> None:
        """setup bcs authorize and base url endpoints

        Class Attributes

        * `BCSApi.authorize_url: str` - https://odata-nextgen.bakerhillsolutions.net/token
        * `BCSApi.endpoint_url: str` - https://odata-nextgen.bakerhillsolutions.net/odata/
        """
        self.authorize_url = "https://odata-nextgen.bakerhillsolutions.net/token"
        self.endpoint_url = "https://odata-nextgen.bakerhillsolutions.net/odata/"

        self.load_dir = None

    def query(self, prepared_url: str, max_pages: Optional[int] = None) -> Any:
        """query the data from bcs, and continue until no more data is retrieved
        Smart query, saves a checkpoint of records as it pulls data from object.

        As data is pulled for each page, the results are pickled in an object. If a crash occurs,
        it will create a save state and read from save state and resume operations from last crash.

        Refer to the nested functions of ``__load_state()`` and ``__save_state()``

        :param prepared_url: (str) - fully prepared odata url
        :param max_pages: (int|None) - pages to process, if ``None`` then all pages

        :return:

        *Current machine has 32GB RAM, hopefully a single call to a specific object doesn't
        cause issues, before clearing the cache.*

        **TODO**: this method could use some tlc and optimization
        """

        ser_file = Path(base64.urlsafe_b64encode(prepared_url.encode()).decode())

        def __load_state() -> Tuple[Optional[List[str]], Optional[str]]:
            crash_file = Path(".crash_detected")
            if crash_file.is_file():
                crash_file.unlink()
                data = []
                if ser_file.is_file():
                    # we have a checkpoint, pick it up from there
                    # data will be deserialized python dictionary
                    # [{"url": "....", "records": []}, ...]
                    with lzma.open(ser_file, "rb") as f:
                        try:
                            while True:
                                data.append(pickle.load(f))
                        except EOFError:
                            pass

                    # get last recorded url
                    last_url: str = data[-1]["url"]
                    all_records = [i for sublist in data for i in sublist["data"]]
                    Red.log(
                        f"Loading state. {len(all_records)} records found. Resuming last url: {last_url}"
                    )
                    return all_records, last_url
            return None, None

        def __save_state(fobj: Path, obj: Any) -> None:
            fobj.touch(exist_ok=True)
            with lzma.open(fobj, "a") as f:
                pickle.dump(obj, f)

        records: Dict[str, Any] = {"url": prepared_url, "data": []}
        page = 0
        content = None

        recovered_data, last_url = __load_state()

        if recovered_data:
            records["data"].extend(recovered_data)
            prepared_url = str(last_url)

        if max_pages is not None:
            try:
                max_pages = int(max_pages)
            except:
                Red.warn(f"max_pages supplied is not integer")
                max_pages = None

        try:
            while True:
                Red.log(f"Querying Page....{page}")
                resp = self.smart_call(
                    self._main_session.get,
                    prepared_url,
                    headers={"Authorization": f"Bearer {self._api_token}"},  # type: ignore
                )

                try:
                    content = resp.json()  # type: ignore

                except Exception as e:
                    Exit(
                        LEVEL_CRITICAL,
                        f"Response payload is not valid JSON: {e}\n{resp}",
                    )

                r = content.get("value")  # type: ignore

                if not r:
                    # check if None or just empty list

                    if r is None:
                        Exit(
                            LEVEL_CRITICAL,
                            f"Payload did not recieve proper response: {content}",
                        )
                    else:
                        # Exit(LEVEL_SUCCESS, f"No new records found....")
                        return

                records["data"].extend(r)

                prepared_url = content.get("@odata.nextLink", None)  # type: ignore

                Red.log(f"Found next link: {prepared_url}")
                if not prepared_url:
                    break

                __save_state(ser_file, {"url": prepared_url, "data": r})

                if max_pages is not None:
                    if max_pages - 1 == page:
                        Red.info(f"Max pages reached... aborting ingestion")
                        break

                page += 1

            # we are all done.. remove checkpoint as we don't need it any longer
            if ser_file.is_file():
                ser_file.unlink()

            Red.log(f"Finished query with {len(records['data'])} records")
            return records["data"]

        except Exception as e:
            Exit(
                LEVEL_CRITICAL,
                f"Failed to query on url: {prepared_url} with error: {traceback.format_exc()}",
            )


class BaseEntity(BCSApi):
    """All BCS tables are pendantically `Entities`. Any new or existing entities should
    inherit this class as it takes care of 90% of the ingestion process and controlled with metadata

    Example Usage:

    ```python
    from pathlib import Path
    from typing import Tuple

    import pandas as pd

    from pygcu.red.secret_server import SecretServer
    from pygcu.red.mock import SecretServer as _localSecretServer  # type: ignore
    from pygcu.red import Exit, LEVEL_SUCCESS, WherescapeManager, Red
    from pygcu.services.bcs import BaseEntity


    class ClientOwnerships(BaseEntity):

        __entity_name__ = "ClientOwnerships"
        __headers__: Tuple[str, ...] = (
            "Id",
            "Owner",
            "OwnershipPercentage",
            "EntityKey_EntitySetName",
            "EntityKey_EntityContainerName",
            "EntityKey_EntityKeyValues",
        )

        def post_extract(self, df: pd.DataFrame) -> None:
            df.to_csv(self._output_file, index=False, header=True)
            Red.info(f"Writing {len(df)} records to: {self._output_file.absolute()}")


    def main():
        db = WherescapeManager("$PRED_Database$", parameters=["load_bcs"])

        odata_params = {}
        if db.local_execution:
            SecretServer = _localSecretServer
            odata_params["$top"] = "100"

        ss = SecretServer()
        sid = 7791

        client_id = ss.get_password(sid, "username").strip()
        client_secret = ss.get_password(sid, "password").strip()

        with ClientOwnerships(
            client_id=client_id,
            client_secret=client_secret,
            repo_db=db,
            odata_params=odata_params,
            load_dir=Path(db["load_bcs"].value),
            delta=None,
            force_full_reload=True,
        ) as bcs:
            bcs.run()

        Exit(LEVEL_SUCCESS, "Success")


    if __name__ == "__main__":
        main()
    ```

    """

    __entity_name__: str | None = None
    __headers__: Tuple[str, ...] = ()

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        repo_db: WherescapeProtocol,
        odata_params: Dict[str, str] = {},
        load_dir: Path = Path(tempfile.gettempdir()),
        delta: Tuple[str, str] | None = None,
        force_full_reload: bool = False,
    ):
        """Base Entity class for any BCS NextGen table. Inherit this class for finer control of ingestion.

        Class Attributes

        * `__entity_name__: (str|None)` Name of the entity (table) from BCS NextGen APIs
        * `__headers__: (Tuple[str, ...])` A tuple of headers expected from desired entity


        :param client_id: client_id to connect to API
        :param client_secret: client_secret to connect to API
        :param repo_db: Connection object to Wherescape Repo Database
        :param odata_params: Additional OData params in to pass to api. Defaults to {}.
        :param load_dir:  _description_. Defaults to Path(tempfile.gettempdir()).
        :param delta: : _description_. Defaults to None.
        :param force_full_reload:  _description_. Defaults to False.
        """
        if self.__entity_name__ is None:
            Exit(
                LEVEL_ERROR,
                "Entity name not defined, make sure to define __entity_name__ attribute",
            )

        if not self.__headers__:
            Exit(LEVEL_ERROR, "No supplied headers...")

        super().__init__(client_id, client_secret)

        self._params = {"$skip": "0", "$count": "true", **odata_params}

        self._today = date.today().strftime("%Y-%m-%d")

        self._load_dir = load_dir
        self._delta = delta
        self._force_full_reload = force_full_reload
        self._output_file = (
            load_dir / f"{self.__entity_name__}_{time.strftime('%Y%m%d')}.csv"
        )
        self._repo_db = repo_db

    @property
    def url(self) -> ODataUrl:
        """makes odata url"""
        return ODataUrl(str(self.endpoint_url)).parse(
            str(self.__entity_name__),
            params=self._params,
        )

    def pre_extract(self) -> None:
        """Pre extration logic override this method to change default behavior"""
        if self._delta:
            if self._params.get("$filter"):
                del self._params["$filter"]

            field_name, field_value = self._delta
            self._params["$filter"] = f"{field_name} ge {field_value}"
        else:
            self._force_full_reload = True

        if self._force_full_reload:
            if self._params.get("$filter"):
                del self._params["$filter"]

    def extract(
        self, apply_func: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    ) -> pd.DataFrame:
        """The extraction process for defined entity

        Example Usage:

        ```python
        def cast_as_str(df: pd.DataFrame) -> pd.DataFrame:
            df['mycolumn'] = df['mycolumn'].astype(str)
            return df

        class MyCustomEntity(BaseEntity):
            ...
            def extract(apply_func=cast_as_str):
                super().extract(apply_func=apply_func)
            ...
        ```

        :param apply_func:  Provide a custom function to handle any post-processing to data. Defaults to None.

        :return: post-processed data from entity as a pandas dataframe
        """
        data = self.query(self.url)
        if data is None:
            df = pd.DataFrame(columns=self.__headers__)
        else:
            df = pd.json_normalize(data, sep="_")

        got = set(df.columns)
        want = set(self.__headers__)

        unknown_columns = got.difference(want)

        if unknown_columns:
            errmsg = f"columns mismatch from provided and retrieved: {unknown_columns}"
            df.drop(columns=unknown_columns, inplace=True)
            Red.warn(errmsg)

        # add any missing columns with default null value
        for column in want:
            if column not in df.columns:
                df[column] = np.NaN

        try:
            df = df[list(self.__headers__)]
        except KeyError as e:
            Exit(LEVEL_CRITICAL, f"Key Error: Necessary columns not found\n{e}")

        if apply_func:
            return apply_func(df)

        df.to_csv(self._output_file, index=False, header=True)
        Red.info(f"Writing {len(df)} records to: {self._output_file.absolute()}")

        return df

    def post_extract(self, df: pd.DataFrame) -> None:
        """post extraction logic. Use this to block for any clean up and parameter changes in red"""

    def run(self):
        """run"""
        self.pre_extract()
        df = self.extract()
        self.post_process(df)
        self.post_extract(df)
