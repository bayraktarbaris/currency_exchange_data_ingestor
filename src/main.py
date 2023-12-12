import logging
import sys
from datetime import datetime, timedelta

from src.clients.currency_rate_client import CurrencyRateClient
from src.services.currency_rate_transformer_service import (
    CurrencyRateTransformerService,
)
from src.services.json_service import JsonService
from src.services.parquet_service import ParquetService
from src.utils.missing_dates import MissingDatesService

app_name = "currency_exchange_data_ingestor"


def _register_logger():
    _logger = logging.getLogger(app_name)
    _logger.setLevel(logging.INFO)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    _logger.addHandler(ch)


_register_logger()
logger = logging.getLogger(app_name)


def run_today():
    json_service = JsonService()
    parquet_service = ParquetService()
    currency_rate_client = CurrencyRateClient()
    yesterday = datetime.now() - timedelta(days=1)

    is_yesterday_missing = MissingDatesService.find_missing_dates(
        start_date=yesterday, end_date=yesterday
    )
    if is_yesterday_missing:
        currency_rates = currency_rate_client.get_currency_rates()
        if currency_rates:
            date = currency_rates[0]["date"]
            json_service.write_json(currency_rates, date=date)
            df = json_service.read_json(date=date)
            df = CurrencyRateTransformerService.add_reciprocal_rates(df)
            parquet_service.write_parquet(df)
            df = parquet_service.read_dataset()
            print(df)


def run_historical():
    json_service = JsonService()
    parquet_service = ParquetService()
    currency_rate_client = CurrencyRateClient()
    start_date = datetime(2023, 12, 1)
    yesterday = datetime.now() - timedelta(days=1)

    missing_dates = MissingDatesService.find_missing_dates(
        start_date=start_date, end_date=yesterday
    )
    for date in missing_dates:
        currency_rates = currency_rate_client.get_currency_rates(date=date)
        if currency_rates:
            date = currency_rates[0]["date"]
            json_service.write_json(currency_rates, date=date)
            df = json_service.read_json(date=date)
            df = CurrencyRateTransformerService.add_reciprocal_rates(df)
            parquet_service.write_parquet(df)
            df = parquet_service.read_dataset()
            print(df)


if __name__ == "__main__":
    # run_today()
    run_historical()
    # parquet_service = ParquetService()
    # parquet_service.read_dataframe('../data/parquet_files/year=2023/month=12/day=7/5ab5d4ae1e9244eaa1d055008fb85826-0.parquet')
