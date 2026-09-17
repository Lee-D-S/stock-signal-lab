from __future__ import annotations

import unittest

import pandas as pd

from forecast.experiment.contracts import build_dataset_contract
from forecast.experiment.universe import UniverseConfig, build_fixed_universe, validate_fixed_universe


class ContractAndUniverseTests(unittest.TestCase):
    def test_universe_selection_is_dated_ranked_and_fixed(self) -> None:
        candidates = pd.DataFrame([
            {"ticker": "1", "name": "A", "market": "KOSPI", "security_type": "COMMON", "market_cap": 30, "history_days": 300, "avg_value_20d": 100, "suspended": False},
            {"ticker": "2", "name": "B ETF", "market": "KOSPI", "security_type": "ETF", "market_cap": 40, "history_days": 300, "avg_value_20d": 100, "suspended": False},
            {"ticker": "3", "name": "C", "market": "KOSDAQ", "security_type": "COMMON", "market_cap": 20, "history_days": 300, "avg_value_20d": 100, "suspended": False},
        ])
        result = build_fixed_universe(candidates, config=UniverseConfig(selection_date="2024-12-31", size=2))
        self.assertEqual(result.manifest["ticker"].tolist(), ["000001", "000003"])
        validate_fixed_universe(result.manifest, expected_size=2)
        self.assertEqual(result.rejected.loc[0, "exclusion_reason"], "not_common_stock")

    def test_dataset_contract_separates_future_labels(self) -> None:
        dates = pd.date_range("2024-01-01", periods=25, freq="B")
        raw = pd.DataFrame({
            "ticker": ["000001"] * len(dates),
            "date": dates,
            "open": range(100, 125),
            "high": range(102, 127),
            "low": range(98, 123),
            "close": range(101, 126),
            "volume": [100_000] * len(dates),
            "foreign_net": [1_000] * len(dates),
            "institution_net": [500] * len(dates),
            "individual_net": [-1_500] * len(dates),
        })
        dataset = build_dataset_contract(raw, complete_years=[2024])
        self.assertNotIn("future_return_1d", dataset.features.columns)
        self.assertIn("future_return", dataset.labels.columns)
        self.assertEqual(set(dataset.labels["horizon"]), {"1d", "5d", "20d", "year_end"})
        self.assertIn("feature_asof", dataset.features.columns)


if __name__ == "__main__":
    unittest.main()
