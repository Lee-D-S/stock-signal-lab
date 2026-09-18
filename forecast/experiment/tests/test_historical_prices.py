from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from forecast.experiment.historical_prices import build_price_coverage_report, prepare_training_prices


class HistoricalPriceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.universe = pd.DataFrame({
            "ticker": ["000001", "000002"],
            "name": ["A", "B"],
            "market": ["KOSPI", "KOSDAQ"],
        })
        self.prices = pd.DataFrame({
            "ticker": ["000001", "000001", "000002"],
            "date": ["2014-12-30", "2015-01-02", "2015-01-02"],
            "open": [100, 101, 200],
            "high": [102, 103, 202],
            "low": [99, 100, 198],
            "close": [101, 102, 201],
            "volume": [1000, 1100, 2000],
            "turnover": [100000, 112000, 402000],
        })

    def test_prepare_attaches_metadata_and_filters_period(self) -> None:
        prepared = prepare_training_prices(
            self.prices,
            self.universe,
            raw_start=date(2015, 1, 1),
            raw_end=date(2015, 12, 31),
        )
        self.assertEqual(len(prepared), 2)
        self.assertEqual(set(prepared["name"]), {"A", "B"})
        self.assertTrue(prepared["price_basis"].eq("adjusted_close").all())

    def test_coverage_report_keeps_missing_fixed_ticker(self) -> None:
        prepared = prepare_training_prices(
            self.prices,
            self.universe,
            raw_start=date(2015, 1, 1),
            raw_end=date(2015, 12, 31),
        )
        report = build_price_coverage_report(prepared, self.universe)
        self.assertEqual(report.loc[report["ticker"].eq("000001"), "coverage_status"].iloc[0], "ok")
        self.assertEqual(report.loc[report["ticker"].eq("000002"), "coverage_status"].iloc[0], "ok")


if __name__ == "__main__":
    unittest.main()
