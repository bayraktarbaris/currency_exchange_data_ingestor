import os
import unittest

import pandas as pd

from src.services.parquet_service import ParquetService


class TestParquetService(unittest.TestCase):
    def setUp(self):
        self.parquet_service = ParquetService()

        self.sample_parquet_directory = "test_data/parquet_files"
        self.df = pd.DataFrame({"date": ["2023-01-01", "2023-01-02"], "value": [1, 2]})

    def test_write_parquet(self):
        self.parquet_service.write_parquet(self.df)

        for root, dirs, files in os.walk(self.sample_parquet_directory):
            self.assertGreater(len(files), 0)

    def test_read_dataset(self):
        dt = self.parquet_service.read_dataset()
        pd.DataFrame.equals(dt, self.df)

    def test_read_dataframe(self):
        pass

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
