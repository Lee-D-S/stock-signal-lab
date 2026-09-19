from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

from forecast.experiment.daily_price_volume import (
    _validate_cutoff,
    attach_daily_actuals,
    refresh_daily_price_volume_scorecard,
    summarise_daily_price_volume_predictions,
)


class DailyPriceVolumeTests(unittest.TestCase):
    def setUp(self) -> None:
        dates = pd.to_datetime(["2026-09-15", "2026-09-16", "2026-09-17"])
        self.raw = pd.DataFrame(
            {
                "ticker": ["000001"] * 3 + ["000002"] * 3,
                "name": ["A"] * 3 + ["B"] * 3,
                "market": ["KOSPI"] * 6,
                "date": list(dates) * 2,
                "close": [100.0, 101.0, 99.0, 200.0, 198.0, 201.0],
                "volume": [1000.0] * 6,
            }
        )

    def test_actuals_use_next_trading_date(self) -> None:
        predictions = pd.DataFrame(
            {
                "ticker": ["000001", "000002"],
                "feature_asof": pd.to_datetime(["2026-09-16"] * 2),
                "prediction_date": pd.to_datetime(["2026-09-17"] * 2),
                "candidate": ["logistic", "logistic"],
                "probability_up": [0.8, 0.2],
                "prediction_available": [True, True],
            }
        )
        scored = attach_daily_actuals(predictions, self.raw)
        self.assertEqual(scored["target_date"].dt.date.tolist(), [date(2026, 9, 17)] * 2)
        self.assertEqual(scored["actual_direction"].tolist(), [0, 1])
        self.assertTrue(scored["maturity_status"].eq("mature").all())

    def test_summary_and_scorecard_keep_model_level_metrics(self) -> None:
        predictions = pd.DataFrame(
            {
                "ticker": ["000001", "000002"],
                "feature_asof": pd.to_datetime(["2026-09-16"] * 2),
                "prediction_date": pd.to_datetime(["2026-09-17"] * 2),
                "candidate": ["logistic", "logistic"],
                "probability_up": [0.8, 0.2],
                "prediction_available": [True, True],
                "actual_direction": [1, 0],
                "actual_return": [0.01, -0.01],
                "target_date": pd.to_datetime(["2026-09-17"] * 2),
                "maturity_status": ["mature", "mature"],
            }
        )
        summary = summarise_daily_price_volume_predictions(predictions)
        self.assertEqual(len(summary), 1)
        self.assertEqual(int(summary.loc[0, "mature_rows"]), 2)
        self.assertAlmostEqual(float(summary.loc[0, "balanced_accuracy"]), 1.0)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            predictions.to_parquet(root / "daily_price_volume_predictions_2026-09-16.parquet", index=False)
            scorecard = refresh_daily_price_volume_scorecard(root)
            self.assertEqual(len(scorecard), 1)
            self.assertEqual(int(scorecard.loc[0, "prediction_days"]), 1)

    def test_prediction_date_must_follow_feature_cutoff(self) -> None:
        with self.assertRaises(ValueError):
            _validate_cutoff("2026-09-16", "2026-09-16")


if __name__ == "__main__":
    unittest.main()
