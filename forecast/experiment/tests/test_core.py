from __future__ import annotations

import unittest

import pandas as pd

from forecast.experiment.core import (
    ExperimentConfig,
    build_experiment_labels,
    build_feature_frame,
    feature_group_columns,
)


class ExperimentCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        dates = pd.date_range("2024-12-20", periods=15, freq="B")
        self.frame = pd.DataFrame({
            "ticker": ["000001"] * len(dates) + ["000002"] * len(dates),
            "date": list(dates) * 2,
            "open": [100 + i for i in range(len(dates))] * 2,
            "high": [102 + i for i in range(len(dates))] * 2,
            "low": [98 + i for i in range(len(dates))] * 2,
            "close": [101 + i for i in range(len(dates))] * 2,
            "volume": [100_000] * (len(dates) * 2),
            "foreign_net": [1_000] * (len(dates) * 2),
            "institution_net": [500] * (len(dates) * 2),
            "individual_net": [-1_500] * (len(dates) * 2),
        })

    def test_year_end_label_can_be_marked_pending(self) -> None:
        features = build_feature_frame(self.frame)
        labels = build_experiment_labels(features, complete_years=[])
        self.assertTrue(labels["maturity_year_end"].eq("pending").all())

    def test_feature_groups_exclude_future_columns(self) -> None:
        features = build_experiment_labels(build_feature_frame(self.frame), complete_years=[2024])
        all_columns = feature_group_columns(features, "all")
        self.assertTrue(all(not column.startswith("future_") for column in all_columns))
        self.assertTrue(all(not column.startswith("direction_") for column in all_columns))
        self.assertGreater(len(feature_group_columns(features, "price_volume")), 0)

    def test_config_defaults_match_experiment(self) -> None:
        config = ExperimentConfig()
        self.assertEqual(config.top_k, 5)
        self.assertEqual(config.train_end, "2024-12-31")
        self.assertEqual(config.horizons[-1], "year_end")


if __name__ == "__main__":
    unittest.main()

