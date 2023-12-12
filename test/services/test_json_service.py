import unittest
import os
import json
import pandas as pd
from src.services.json_service import JsonService


class TestJsonService(unittest.TestCase):
    def setUp(self):
        self.json_service = JsonService()

        self.sample_json_directory = "test_data/json_files"

    def test_read_json(self):
        date = "2023-01-01"

        json_data = [{"source": "eur", "target": "usd", "rate": 1.2, "date": date}]
        self._write_sample_json_file(json_data, date)

        result_df = self.json_service.read_json(date)

        expected_df = pd.DataFrame(json_data)
        pd.DataFrame.equals(result_df, expected_df)

    def test_write_json(self):
        date = "2023-01-01"
        data = [{"source": "eur", "target": "usd", "rate": 1.2, "date": date}]

        self.json_service.write_json(data, date)

        result_df = self.json_service.read_json(date)

        expected_df = pd.DataFrame(data)
        pd.DataFrame.equals(result_df, expected_df)

    def tearDown(self):
        if os.path.exists(self.sample_json_directory):
            for root, dirs, files in os.walk(self.sample_json_directory, topdown=False):
                for file in files:
                    os.remove(os.path.join(root, file))
                for dir in dirs:
                    os.rmdir(os.path.join(root, dir))
            os.rmdir(self.sample_json_directory)

    def _write_sample_json_file(self, data, date):
        filename = os.path.join(self.sample_json_directory, date, "currency_rates.json")
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w+") as f:
            f.write("\n".join(map(json.dumps, data)))
