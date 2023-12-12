import json
import logging
import os
import sys

from src.utils.singleton import Singleton


class Config(metaclass=Singleton):
    def __init__(self):
        self._load_config()

    def _load_config(self):
        try:
            with open(self.get_config_path()) as f:
                config = json.load(f)

            self.config = config
        except FileNotFoundError as ex:
            logger = logging.getLogger("currency_exchange_data_ingestor")
            logger.error("Config file not found.")
            sys.exit()

    @staticmethod
    def get_config_path():  # TODO: add multiple config JSON files for development, staging, production
        return f"../config/config.json"

    def get(self, key):
        if key in self.config:
            return self.config[key]
        else:
            return None
