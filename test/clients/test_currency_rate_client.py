import unittest
from unittest.mock import MagicMock, patch

from src.clients.currency_rate_client import CurrencyRateClient


class TestCurrencyRateClient(unittest.TestCase):
    def setUp(self):
        self.requests_patcher = patch("src.clients.currency_rate_client.requests")
        self.mock_requests = self.requests_patcher.start()

        self.mock_logger = MagicMock()

        self.currency_rate_client = CurrencyRateClient()
        self.currency_rate_client.logger = self.mock_logger

    def tearDown(self):
        self.requests_patcher.stop()

    def test_get_currency_rates_latest(self):
        mock_response = {"date": "2023-01-01", "eur": {"usd": 1.2, "gbp": 0.9}}
        self.mock_requests.get.return_value.status_code = 200
        self.mock_requests.get.return_value.json.return_value = mock_response

        result = self.currency_rate_client.get_currency_rates()

        self.assertEqual(len(result), 2)
        expected_data = [
            {"source": "eur", "target": "usd", "rate": 1.2, "date": "2023-01-01"},
            {"source": "eur", "target": "gbp", "rate": 0.9, "date": "2023-01-01"},
        ]
        self.assertEqual(result, expected_data)
        self.mock_logger.assert_not_called()

    def test_get_currency_rates_with_date(self):
        mock_response = {"date": "2023-01-01", "eur": {"usd": 1.2, "gbp": 0.9}}
        self.mock_requests.get.return_value.status_code = 200
        self.mock_requests.get.return_value.json.return_value = mock_response

        result = self.currency_rate_client.get_currency_rates(date="2023-01-01")

        self.assertEqual(len(result), 2)
        expected_data = [
            {"source": "eur", "target": "usd", "rate": 1.2, "date": "2023-01-01"},
            {"source": "eur", "target": "gbp", "rate": 0.9, "date": "2023-01-01"},
        ]
        self.assertEqual(result, expected_data)
        self.mock_logger.assert_not_called()

    def test_get_currency_rates_invalid_response(self):
        self.mock_requests.get.return_value.status_code = 500

        result = self.currency_rate_client.get_currency_rates()

        self.assertEqual(result, [])
        self.mock_logger.error.assert_called_once_with(
            "Failed while getting currency rates"
        )

    @patch("src.clients.currency_rate_client.time.sleep", return_value=None)
    def test_get_url_response_with_backoff_retry(self, mock_sleep):
        self.mock_requests.get.side_effect = [
            MagicMock(status_code=404),
            MagicMock(
                status_code=200,
                json=lambda: {"date": "2023-01-01", "eur": {"usd": 1.2}},
            ),
        ]

        result = self.currency_rate_client._get_url_response_with_backoff("mock_url")

        self.assertEqual(result, {"date": "2023-01-01", "eur": {"usd": 1.2}})
        self.assertEqual(self.mock_requests.get.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)
