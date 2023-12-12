import json
import os

import pandas as pd
from pyarrow import json as pyarrow_json

from src.utils.singleton import Singleton


class JsonService(metaclass=Singleton):
    @staticmethod
    def read_json(date: str) -> pd.DataFrame:
        save_path = f"../data/json_files/{date}/currency_rates"
        table = pyarrow_json.read_json(save_path + ".json")
        df = table.to_pandas()
        return df

    @staticmethod
    def write_json(data: list[dict], date: str):
        filename = f"../data/json_files/{date}/currency_rates.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w+") as f:
            f.write("\n".join(map(json.dumps, data)))
