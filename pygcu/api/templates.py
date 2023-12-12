"""
Dedicated to templates and design patterns
"""
from typing import Callable
import pandas as pd


class IngestionTemplate:
    """
    A template designed for repeatable ingestion pattern by providing skeleton methods that should be
    defined by designer.

    Example Usage

    ```python
    import pandas as pd

    def replace_nas_with_zero(df):
        return df.fillna(0)

    class CustomIngestion(IngestionTemplate):
        def pre_extract(self):
            pass

        def extract(self, replace_nas_with_zero):
            data = get_all_the_data()
            df = pd.DataFrame(data)
            return df

        def post_extract(self, df):
            df.columns = ("A", "B", "C")
            df.to_csv("mydata.csv", index=False)


    ci = CustomIngestion()
    ci.run()
    ```
    """

    def run(self) -> None:
        """Call this method to kickstart the ingestion"""
        self.pre_extract()
        df: pd.DataFrame = self.extract(apply_func=None)
        self.post_extract()

    def pre_extract(self) -> None:
        """pre-hook logic before actual ingestion takes place, use this to set things up"""
        raise NotImplementedError()

    def extract(
        self, apply_func: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    ) -> pd.DataFrame:
        """overwrite method that handles actual pulling of data


        :param apply_func: (Callable) - A passable function that accepts a DataFrame and return the
        the same DataFrame. Use this function to do some post-processing of the data.

        :return: `DataFrame`_

        .. _DataFrame: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html
        """

    def post_extract(self) -> None:
        """post-hook logic after ingestion of data. Override this method
        for any updates to database or cleanup work.
        """
