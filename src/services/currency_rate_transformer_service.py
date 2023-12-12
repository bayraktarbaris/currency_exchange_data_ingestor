from src.utils.singleton import Singleton
import pandas as pd


class CurrencyRateTransformerService(metaclass=Singleton):
    @staticmethod
    def add_reciprocal_rates(df: pd.DataFrame) -> pd.DataFrame:
        reciprocal_df = pd.DataFrame(
            {
                "source": df["target"],
                "target": df["source"],
                "rate": df["rate"].apply(
                    CurrencyRateTransformerService._calculate_reciprocal_rate
                ),
                "date": df["date"],
            }
        )
        df = pd.concat([df, reciprocal_df], ignore_index=True)

        return df

    @staticmethod
    def _calculate_reciprocal_rate(exchange_rate: float) -> float:
        reciprocal_rate = 1 / exchange_rate
        return reciprocal_rate
