import base64
import lzma
import pickle
import traceback
import tempfile
from urllib import request
import pandas as pd

from pathlib import Path

from ..red import Red, Exit, LEVEL_CRITICAL, LEVEL_SUCCESS
from ..api.odata import ODataUrl
from ..auth.oauth import OAuthApi


class BCSApi(OAuthApi):
    def setup(self):
        self.authorize_url = "https://odata-nextgen.bakerhillsolutions.net/token"
        self.endpoint_url = ODataUrl(
            "https://odata-nextgen.bakerhillsolutions.net/odata/"
        )

        self.load_dir = None

    def query(self, prepared_url, max_pages=None):
        """query the data from bcs, and continue until no more data is retrieved

        Smart query, saves a checkpoint of records as it pulls data from object.

        NOTE: Current machine as 32GB RAM, hopefully a single call to a specific object doesn't
                cause issues, before clearing the cache.
        TODO: Refactor to be more memory-sensitive
        """

        ser_file = Path(base64.urlsafe_b64encode(prepared_url.encode()).decode())

        def __load_state():
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
                    last_url = data[-1]["url"]
                    all_records = [i for sublist in data for i in sublist["data"]]
                    Red.log(
                        f"Loading state. {len(all_records)} records found. Resuming last url: {last_url}"
                    )
                    return all_records, last_url
            return None, None

        def __save_state(fobj, obj):
            fobj.touch(exist_ok=True)
            with lzma.open(fobj, "a") as f:
                pickle.dump(obj, f)

        records = {"url": prepared_url, "data": []}
        page = 0
        content = None

        recovered_data, last_url = __load_state()

        if recovered_data:
            records["data"].extend(recovered_data)
            prepared_url = last_url

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
                    headers={"Authorization": f"Bearer {self._api_token}"},
                )
                try:
                    content = resp.json()
                except Exception as e:
                    Exit(
                        LEVEL_CRITICAL,
                        f"Response payload is not valid JSON: {e}\n{resp}",
                    )

                r = content.get("value")

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

                prepared_url = content.get("@odata.nextLink", None)

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

    def run(self):
        self.pre_extract()
        df = self.extract()
        self.post_extract(df)

    def pre_extract(self):
        raise NotImplementedError()

    def extract(self) -> pd.DataFrame:
        raise NotImplementedError()

    def post_extract(self, df: pd.DataFrame):
        raise NotImplementedError()
