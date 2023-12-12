import json
import logging
import os
import time

import backoff
import requests

from src.utils.config import Config
from src.utils.singleton import Singleton


class CurrencyRateClient(metaclass=Singleton):
    def __init__(self):
        self.currency_rate_api_base_url = (
            "https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api"
        )
        self.logger = logging.getLogger("currency_exchange_data_ingestor")
        self.currency_rates_latest_url = (
            self.currency_rate_api_base_url + "@1/latest/currencies/eur.json"
        )

    def get_currency_rates(self, date: str = None) -> list[dict]:
        url = (
            f"{self.currency_rate_api_base_url}@1/{date}/currencies/eur.json"
            if date
            else self.currency_rates_latest_url
        )
        url_response = self._get_url_response(url)

        if len(url_response) > 0:
            data = [
                {
                    "source": "eur",
                    "target": target,
                    "rate": rate,
                    "date": url_response["date"],
                }
                for target, rate in url_response["eur"].items()
            ]
            return data

        return []

    @backoff.on_predicate(
        backoff.expo,
        lambda e: not (200 <= e.status_code < 300),
        max_tries=4,
    )
    def _get_url_response_with_backoff(self, url: str):
        resp = requests.get(
            url, headers={"User-Agent": "Currency rate ingestor by Baris Bayraktar"}
        )
        return resp

    def _get_url_response(self, url: str) -> dict:
        resp = self._get_url_response_with_backoff(url)
        if resp.status_code != 200:
            self.logger.error("Failed while getting currency rates")
            return {}

        try:
            json_data = resp.json()
        except json.decoder.JSONDecodeError:
            self.logger.error(f"JSON decode error")
            return {}

        if len(json_data["eur"]) == 0:
            self.logger.error(f"JSON without currency rates")
            return {}

        # TODO: add something more in order to check the data is correct

        return json_data
