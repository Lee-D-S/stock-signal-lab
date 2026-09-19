from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from forecast.experiment.training_dataset import (
    build_one_day_labels,
    build_price_volume_features,
    build_training_frame,
)


class TrainingDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        dates = pd.date_range("2014-01-01", periods=520, freq="B")
        self.raw = pd.DataFrame({
            "ticker": ["000001"] * len(dates),
            "name": ["A"] * len(dates),
            "market": ["KOSPI"] * len(dates),
            "date": dates,
            "open": range(100, 100 + len(dates)),
            "high": range(102, 102 + len(dates)),
            "low": range(98, 98 + len(dates)),
            "close": range(101, 101 + len(dates)),
            "volume": [1000] * len(dates),
            "turnover": [100000] * len(dates),
        })

    def test_labels_use_next_trading_date_and_cutoff(self) -> None:
        labels = build_one_day_labels(
            self.raw,
            feature_start=date(2015, 1, 1),
            target_cutoff=date(2015, 12, 31),
            snapshot="test",
        )
        self.assertTrue((labels["target_end"] > labels["feature_asof"]).all())
        self.assertTrue((labels["target_end"] <= pd.Timestamp("2015-12-31")).all())
        self.assertTrue(labels["direction"].isin([0, 1]).all())

    def test_features_require_warmup_and_training_join_is_point_in_time(self) -> None:
        features = build_price_volume_features(
            self.raw,
            feature_start=date(2015, 1, 1),
            feature_end=date(2015, 12, 31),
            snapshot="test",
        )
        labels = build_one_day_labels(
            self.raw,
            feature_start=date(2015, 1, 1),
            target_cutoff=date(2015, 12, 31),
            snapshot="test",
        )
        train = build_training_frame(features, labels)
        self.assertIn("return_252d", features.columns)
        self.assertIn("volume_ratio_20d", features.columns)
        self.assertIn("gap_return", features.columns)
        self.assertIn("open_close_return", features.columns)
        self.assertIn("candle_body_pct", features.columns)
        self.assertIn("upper_wick_pct", features.columns)
        self.assertIn("lower_wick_pct", features.columns)
        self.assertIn("close_position_20d", features.columns)
        self.assertIn("close_position_60d", features.columns)
        self.assertIn("distance_to_high_20d", features.columns)
        self.assertIn("distance_from_low_20d", features.columns)
        self.assertTrue(features["feature_schema_version"].eq("price-volume-v2").all())
























        self.assertTrue((train["target_end"] > train["feature_asof"]).all())
        self.assertTrue(train["feature_ready"].all())


if __name__ == "__main__":
    unittest.main()
