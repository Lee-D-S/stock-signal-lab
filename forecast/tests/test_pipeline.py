from __future__ import annotations

import unittest

import pandas as pd

from forecast.data.schemas import validate_numeric_columns
from forecast.evaluation.walk_forward import walk_forward_splits
from forecast.features.builder import build_numeric_features
from forecast.labels.returns import add_return_labels
from forecast.universe.filters import apply_universe_filters


class ForecastPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        dates = pd.date_range("2024-01-01", periods=80, freq="B")
        self.frame = pd.DataFrame({
            "ticker": ["000001"] * len(dates), "date": dates,
            "open": range(100, 180), "high": range(102, 182), "low": range(98, 178), "close": range(101, 181),
            "volume": [100_000] * len(dates), "foreign_net": [1_000] * len(dates),
            "institution_net": [500] * len(dates), "individual_net": [-1_500] * len(dates),
        })

    def test_labels_use_future_close_and_direction(self) -> None:
        labels = add_return_labels(self.frame)
        self.assertAlmostEqual(labels.loc[0, "future_return_1d"], 1 / 101, places=8)
        self.assertEqual(labels.loc[0, "direction_20d"], 1)
        self.assertTrue(pd.isna(labels.iloc[-1]["future_return_1d"]))

    def test_features_are_numeric_and_have_missingness(self) -> None:
        features = build_numeric_features(self.frame)
        validate_numeric_columns(features)
        self.assertIn("return_20d", features)
        self.assertIn("foreign_net_missing", features)
        self.assertIn("candle_body_pct", features)

    def test_forbidden_text_column_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            validate_numeric_columns(self.frame.assign(news_sentiment=0.5))

    def test_walk_forward_keeps_purge_gap(self) -> None:
        labels = add_return_labels(build_numeric_features(self.frame))
        folds = list(walk_forward_splits(labels, n_splits=2, test_size=10, purge=5, min_train_dates=20))
        self.assertEqual(len(folds), 2)
        train_dates = set(labels.loc[folds[0][0], "date"])
        test_dates = set(labels.loc[folds[0][1], "date"])
        self.assertLess(max(train_dates), min(test_dates) - pd.Timedelta(days=5))

    def test_universe_filters_return_reasons(self) -> None:
        universe = pd.DataFrame([
            {"ticker": "000001", "market": "KOSPI", "security_type": "COMMON", "price": 10_000, "history_days": 300, "avg_value_20d": 1_000_000_000, "suspended": False},
            {"ticker": "000002", "market": "KOSDAQ", "security_type": "ETF", "price": 10_000, "history_days": 300, "avg_value_20d": 1_000_000_000, "suspended": False},
        ])
        accepted, rejected = apply_universe_filters(universe, as_of=pd.Timestamp("2024-01-01"))
        self.assertEqual(accepted["ticker"].tolist(), ["000001"])
        self.assertEqual(rejected.loc[0, "filter_reason"], "security_type")


if __name__ == "__main__":
    unittest.main()
