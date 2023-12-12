import shutil
import unittest
from datetime import datetime
import pandas as pd
import os
from pyarrow import parquet as pq
from src.utils.missing_dates import MissingDatesService
import pyarrow


class TestMissingDatesService(unittest.TestCase):
    def setUp(self):
        self.missing_dates_service = MissingDatesService()

        self.sample_parquet_directory = "test_data/parquet_files"

    def test_find_missing_dates(self):
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 5)

        result = self.missing_dates_service.find_missing_dates(start_date, end_date)

        expected_result = [
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
        ]
        self.assertEqual(result, expected_result)

    def test_find_missing_dates_empty_directory(self):
        pass

    def test_find_missing_dates_missing_files(self):
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 5)

        partitioned_directory = "test_data/partitioned_parquet"
        os.makedirs(partitioned_directory)

        dates_with_missing = ["2023-01-01", "2023-01-03"]
        for date in dates_with_missing:
            date_directory = os.path.join(partitioned_directory, date)
            os.makedirs(date_directory)
            file_path = os.path.join(date_directory, "data.parquet")
            df = pd.DataFrame({"date": [date], "value": [1]})
            table = pyarrow.Table.from_pandas(df)
            pq.write_table(table, file_path)

        result = self.missing_dates_service.find_missing_dates(start_date, end_date)

        expected_result = ["2023-01-02", "2023-01-04", "2023-01-05"]
        self.assertEqual(result, expected_result)

    def tearDown(self):
        if os.path.exists(self.sample_parquet_directory):
            for root, dirs, files in os.walk(
                self.sample_parquet_directory, topdown=False
            ):
                for file in files:
                    os.remove(os.path.join(root, file))
                for dir in dirs:
                    os.rmdir(os.path.join(root, dir))
            os.rmdir(self.sample_parquet_directory)

        partitioned_directory = "test_data/partitioned_parquet"
        if os.path.exists(partitioned_directory):
            shutil.rmtree(partitioned_directory)
