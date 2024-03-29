"""
Dedicated to templates and design patterns
"""

from typing import Callable

# try to import pandas, if not available, then use polars
# if polars is not available, then raise an ImportError

try:
    import pandas as pd
except ImportError:
    try:
        import polars as pd
    except ImportError:
        raise ImportError(
            "You must have either pandas or polars installed to use this module"
        )

import asyncio


class AsyncIngestionTemplate:
    """
    A template designed for repeatable ingestion pattern by providing skeleton methods that should be
    defined by designer.

    Example Usage

    ```python
    import pandas as pd
    # or import polars as pd


    class CustomIngestion(AsyncIngestionTemplate):
        def pre_extract(self):
            pass

        async def extract(self):
            data = get_all_the_data()
            df = pd.DataFrame(data)
            return df

        def post_processing(self, df):
            return df.fillna(0)

        def post_extract(self, df):
            df.columns = ("A", "B", "C")
            df.to_csv("mydata.csv", index=False)

    ci = CustomIngestion()
    ci.run() # this will spin up the async loop and run the ingestion process
    ```
    """

    def pre_extract(self) -> None:
        """pre-hook logic before actual ingestion takes place, use this to set things up"""
        raise NotImplementedError()

    async def extract(self) -> pd.DataFrame:
        """overwrite method that handles actual pulling of data

        :return: `DataFrame`_

        .. _DataFrame: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html
        """

    def post_extract(self, df: pd.DataFrame) -> None:
        """post-hook logic after ingestion of data. Override this method
        for any updates to database or cleanup work.
        """

    async def _run(self) -> None:
        """Run the ingestion process"""
        self.pre_extract()
        df = await self.extract()
        self.post_extract(df)

    def run(self) -> None:
        """Run the ingestion process"""
        asyncio.run(self._run())


class IngestionTemplate:
    """
    A template designed for repeatable ingestion pattern by providing skeleton methods that should be
    defined by designer.

    Example Usage

    ```python
    import pandas as pd
    # or import polars as pd


    class CustomIngestion(IngestionTemplate):
        def pre_extract(self):
            pass

        def extract(self):
            data = get_all_the_data()
            df = pd.DataFrame(data)
            return df

        def post_processing(self, df):
            return df.fillna(0)

        def post_extract(self, df):
            df.columns = ("A", "B", "C")
            df.to_csv("mydata.csv", index=False)


    ci = CustomIngestion()
    ci.run()
    ```
    """

    def pre_extract(self) -> None:
        """pre-hook logic before actual ingestion takes place, use this to set things up"""
        raise NotImplementedError()

    def extract(self) -> pd.DataFrame:
        """overwrite method that handles actual pulling of data

        :return: `DataFrame`_

        .. _DataFrame: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html
        """

    def post_extract(self, df: pd.DataFrame) -> None:
        """post-hook logic after ingestion of data. Override this method
        for any updates to database or cleanup work.
        """

    def run(self) -> None:
        """Run the ingestion process"""
        self.pre_extract()
        df = self.extract()
        self.post_extract(df)
        return df
