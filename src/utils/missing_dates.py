import pandas as pd
import os
from datetime import datetime

from pyarrow import parquet as pq

from src.utils.singleton import Singleton


class MissingDatesService(metaclass=Singleton):
    @staticmethod
    def find_missing_dates(start_date: datetime, end_date: datetime):
        folder_path = "../data/parquet_files"

        expected_date_range = pd.date_range(
            start=start_date, end=end_date, freq="D", normalize=True
        )

        # Create an empty list to store actual dates
        actual_dates = []

        # Iterate through subfolders or files in the main folder
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # Assuming the files contain a date column named 'date'
                file_path = os.path.join(root, file)
                df = pq.read_table(file_path)

                # Extract the dates from the DataFrame
                dates_in_file = pd.to_datetime(
                    df["date"], errors="coerce"
                ).date.tolist()
                actual_dates.extend(dates_in_file)

        # Convert the actual dates to a set for faster comparison
        actual_date_set = set(actual_dates)

        # Identify missing dates
        missing_dates = expected_date_range[~expected_date_range.isin(actual_date_set)]
        missing_dates_in_str = [
            pd.to_datetime(str(date)).strftime("%Y-%m-%d")
            for date in missing_dates.values
        ]

        return missing_dates_in_str
