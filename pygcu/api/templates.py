"""
Dedicated to templates and design patterns
"""
import pandas as pd


class IngestionTemplate:
    """
    A template designed for repeatable ingestion pattern by providing skeleton methods that should be
    defined by designer.

    Example Usage

    ```python
    import pandas as pd

    class CustomIngestion(IngestionTemplate):
        def pre_extract(self):
            pass

        def extract(self):
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
        df: pd.DataFrame = self.extract()
        self.post_extract(df)

    def pre_extract(self) -> None:
        """pre-hook logic before actual ingestion takes place, use this to set things up"""
        raise NotImplementedError()

    def extract(self) -> pd.DataFrame:
        """overwrite method that handles actual pulling of data

        :return: `DataFrame`_

        .. _DataFrame: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html
        """

    def post_extract(self, df: pd.DataFrame) -> None:
        """post-hook logic after ingestion of data

        :param df: (pd.DataFrame) - Data ingested from `IngestionTemplate.extract()`
        """
