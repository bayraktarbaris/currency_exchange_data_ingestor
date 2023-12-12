import unittest

import pandas as pd

from src.services.currency_rate_transformer_service import (
    CurrencyRateTransformerService,
)


class TestCurrencyRateTransformerService(unittest.TestCase):
    def setUp(self):
        self.currency_rate_transformer = CurrencyRateTransformerService()

    def test_add_reciprocal_rates(self):
        input_df = pd.DataFrame(
            {
                "source": ["eur", "usd", "gbp"],
                "target": ["usd", "gbp", "eur"],
                "rate": [1.2, 0.9, 1.5],
                "date": ["2023-01-01"] * 3,
            }
        )

        result_df = self.currency_rate_transformer.add_reciprocal_rates(input_df)

        expected_df = pd.DataFrame(
            {
                "source": ["eur", "usd", "gbp", "usd", "gbp", "eur"],
                "target": ["usd", "gbp", "eur", "eur", "usd", "gbp"],
                "rate": [1.2, 0.9, 1.5, 1 / 1.2, 1 / 0.9, 1 / 1.5],
                "date": ["2023-01-01"] * 6,
            }
        )
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_calculate_reciprocal_rate(self):
        result = self.currency_rate_transformer._calculate_reciprocal_rate(2.0)

        self.assertEqual(result, 0.5)
