from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from forecast.labels.daily_ground_truth import _date_windows, build_daily_direction_labels


class DailyGroundTruthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.universe = pd.DataFrame({
            "ticker": ["000001", "000002"],
            "name": ["A", "B"],
            "market": ["KOSPI", "KOSDAQ"],
        })
        self.prices = pd.DataFrame({
            "ticker": ["000001"] * 3 + ["000002"] * 2,
            "date": ["2024-12-30", "2025-01-02", "2025-01-03", "2024-12-30", "2025-01-02"],
            "close": [100, 110, 105, 200, 190],
        })

    def test_date_windows_cover_period_without_gaps(self) -> None:
        windows = _date_windows(date(2025, 1, 1), date(2025, 3, 31), window_days=30)
        self.assertEqual(windows[0][0], date(2025, 1, 1))
        self.assertEqual(windows[-1][1], date(2025, 3, 31))
        for previous, current in zip(windows, windows[1:]):
            self.assertEqual(current[0], previous[1] + pd.Timedelta(days=1))

    def test_direction_uses_previous_available_trading_close(self) -> None:
        labels = build_daily_direction_labels(
            self.prices,
            self.universe,
            target_start=date(2025, 1, 1),
            target_end=date(2025, 1, 3),
            snapshot="test-snapshot",
        )
        first = labels[(labels["ticker"] == "000001") & (labels["target_date"] == pd.Timestamp("2025-01-02"))].iloc[0]
        self.assertEqual(first["feature_asof"], pd.Timestamp("2024-12-30"))
        self.assertAlmostEqual(first["future_return_1d"], 0.1)
        self.assertEqual(first["direction_1d"], 1)

    def test_missing_target_price_is_retained_with_status(self) -> None:
        labels = build_daily_direction_labels(
            self.prices,
            self.universe,
            target_start=date(2025, 1, 1),
            target_end=date(2025, 1, 3),
            snapshot="test-snapshot",
        )
        missing = labels[(labels["ticker"] == "000002") & (labels["target_date"] == pd.Timestamp("2025-01-03"))].iloc[0]
        self.assertEqual(missing["maturity_status"], "missing_price")
        self.assertTrue(pd.isna(missing["direction_1d"]))


if __name__ == "__main__":
    unittest.main()
