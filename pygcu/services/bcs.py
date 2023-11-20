import base64
import lzma
import pickle
import traceback
import requests
import pandas as pd

from pathlib import Path
from typing import Any, Tuple, List, Dict, Optional

from ..red import Red, Exit, LEVEL_CRITICAL, LEVEL_SUCCESS
from ..api.templates import IngestionTemplate
from ..api.odata import ODataUrl
from ..auth.oauth import OAuthApi


class BCSApi(OAuthApi, IngestionTemplate):
    """
    Dedicated Logic for the data source NextGen BCS.

    BCS comes with many entities and follows the `OData`_ standard, this makes abstraction very easy and repeatable.

    Example Usage for getting client communication table

    .. code-block:: python

        from pathlib import Path
        from datetime import date
        from typing import Tuple

        import time
        import pandas as pd

        from pygcu.red import (
            Wherescape,
            Exit,
            Red,
            LEVEL_SUCCESS,
        )
        from pygcu.services.bcs import BCSApi
        from secret_server import SecretServer


        db = Wherescape()
        db.connect("$PRED_Database$")


        _ENTITY_NAME = "ClientCommunications"
        _FULL_RELOAD = db.ws_parameter_read(f"BCS_{_ENTITY_NAME}_Full_Reload")
        _SINCE_LAST_RUN = db.ws_parameter_read(f"BCS_{_ENTITY_NAME}_LastLoad_Date")
        _LOAD_DIR = db.ws_parameter_read("load_bcs")
        _OUTPUT_FILE = f"{_ENTITY_NAME}_{time.strftime('%Y%m%d')}.csv"


        class BCSClientCommunications(BCSApi):
            __entity_name__: str = _ENTITY_NAME
            __headers: Tuple = (
                "ClientId",
                "CreatedDate",
                "Family",
                "FamilyCode",
                "Id",
                "IsPrimary",
                "Key",
                "Type",
                "TypeCode",
                "UpdatedDate",
                "Value",
                "ClientReference_EntityKey_EntitySetName",
                "ClientReference_EntityKey_EntityContainerName",
                "ClientReference_EntityKey_EntityKeyValues",
                "EntityKey_EntitySetName",
                "EntityKey_EntityContainerName",
                "EntityKey_EntityKeyValues",
            )

            _today: str = date.today().strftime("%Y-%m-%d")

            def pre_extract(self) -> None:
                params = {"$skip": 0, "$count": "true"}

                if _FULL_RELOAD.value == "0":
                    params["$filter"] = f"UpdatedDate gt {_SINCE_LAST_RUN.value}"

                self.url: str = self._endpoint_url.parse(
                    self.__entity_name__,
                    params=params,
                )

                self.output_file: Path = Path(_LOAD_DIR.value) / _OUTPUT_FILE


            def extract(self) -> pd.DataFrame:

                data = self.query(self.url)
                if data is None:
                    return pd.DataFrame(columns=self.__headers)

                df = pd.json_normalize(data, sep="_")
                return df

            def post_extract(self, df: pd.DataFrame):
                df.to_csv(self.output_file, index=False, header=True)

                Red.info(f"Writing {len(df)} records to: {self.output_file.absolute()}")

                if _FULL_RELOAD.value == "1":
                    _FULL_RELOAD.value = "0"
                    db.ws_parameter_write(_FULL_RELOAD)
                    Red.info("Updating Full Reload parameter to 0")

                # update last_update time
                _SINCE_LAST_RUN.value = self._today
                db.ws_parameter_write(_SINCE_LAST_RUN)
                Red.info(f"Updating last update time to: {self._today}")


        def main():
            ss = SecretServer()
            sid = 7791

            client_id = ss.get_password(sid, "username").strip()
            client_secret = ss.get_password(sid, "password").strip()

            with BCSClientCommunications(client_id, client_secret) as bcs:
                bcs.run()

            Exit(LEVEL_SUCCESS, "Success")


        if __name__ == "__main__":
            main()



    .. _OData: https://www.odata.org/

    """

    def setup(self) -> None:
        """setup bcs authorize and base url endpoints

        :ivar BCSApi.authorize_url: - https://odata-nextgen.bakerhillsolutions.net/token
        :ivar BCSApi.endpoint: - https://odata-nextgen.bakerhillsolutions.net/odata/
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

        **Current machine as 32GB RAM, hopefully a single call to a specific object doesn't
        cause issues, before clearing the cache.**
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

    # def run(self):
    #     self.pre_extract()
    #     df = self.extract()
    #     self.post_extract(df)

    # def pre_extract(self):
    #     raise NotImplementedError()

    # def extract(self) -> pd.DataFrame:
    #     raise NotImplementedError()

    # def post_extract(self, df: pd.DataFrame):
    #     raise NotImplementedError()
