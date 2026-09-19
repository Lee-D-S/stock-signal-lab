from __future__ import annotations

import unittest

import pandas as pd

from forecast.experiment.price_volume_models import _year_folds, evaluate_candidates


class PriceVolumeModelTests(unittest.TestCase):
    def setUp(self) -> None:
        dates = pd.date_range("2015-01-01", periods=6 * 260, freq="B")
        frame = pd.DataFrame({
            "ticker": ["000001"] * len(dates),
            "date": dates,
            "feature_asof": dates,
            "target_end": dates + pd.offsets.BDay(1),
            "direction": [index % 2 for index in range(len(dates))],
        })
        for index in range(4):
            frame[f"feature_{index}"] = (frame.index % 10) + index
        self.frame = frame

    def test_year_folds_purge_target_before_validation_year(self) -> None:
        folds = _year_folds(self.frame, (2018, 2019))
        self.assertEqual([year for year, _, _ in folds], [2018, 2019])
        for year, train, test in folds:
            self.assertTrue((train["target_end"] < pd.Timestamp(year=year, month=1, day=1)).all())
            self.assertTrue((test["feature_asof"].dt.year == year).all())

    def test_candidate_evaluation_returns_all_baselines(self) -> None:
        evaluations, folds = evaluate_candidates(
            self.frame,
            feature_columns=["feature_0", "feature_1", "feature_2", "feature_3"],
            validation_years=(2018, 2019),
        )
        self.assertEqual(set(evaluations["candidate"]), {"baseline", "logistic", "random_forest", "hist_gradient_boosting"})
        self.assertEqual(set(folds), set(evaluations["candidate"]))


if __name__ == "__main__":
    unittest.main()
