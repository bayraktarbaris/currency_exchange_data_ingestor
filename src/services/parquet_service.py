import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

from src.utils.singleton import Singleton


class ParquetService(metaclass=Singleton):
    @staticmethod
    def write_parquet(df: pd.DataFrame) -> None:
        df["date"] = df["date"].map(lambda t: pd.to_datetime(t, format="%Y-%m-%d"))
        df["year"], df["month"], df["day"] = (
            df["date"].apply(lambda x: x.year),
            df["date"].apply(lambda x: x.month),
            df["date"].apply(lambda x: x.day),
        )
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table, "../data/parquet_files/", partition_cols=["year", "month", "day"]
        )

    @staticmethod
    def read_dataset() -> pd.DataFrame:
        dataset = pq.ParquetDataset("../data/parquet_files/")
        table = dataset.read()
        return table.to_pandas()

    @staticmethod
    def read_dataframe(path: str) -> pd.DataFrame:
        table2 = pq.read_table(path)
        return table2.to_pandas()
