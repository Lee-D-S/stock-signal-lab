from __future__ import annotations

import unittest

import pandas as pd

from forecast.evaluation.metrics import classification_metrics
from forecast.experiment.price_volume_models import SELECTION_POLICY, _selection_summary


class MetricPolicyTests(unittest.TestCase):
    def test_probability_metrics_include_log_loss(self) -> None:
        metrics = classification_metrics(
            pd.Series([0, 1, 0, 1]),
            pd.Series([0.1, 0.9, 0.8, 0.2]),
        )
        self.assertIn("log_loss", metrics)
        self.assertGreater(metrics["log_loss"], 0.0)

    def test_selection_policy_keeps_years_separate(self) -> None:
        self.assertEqual(SELECTION_POLICY["averaging"], "none")
        self.assertEqual(SELECTION_POLICY["primary_metric"], "balanced_accuracy_worst_year")
        self.assertEqual(
            [item["metric"] for item in SELECTION_POLICY["tie_breakers"]],
            ["baseline_beaten_years", "roc_auc_worst_year", "brier_worst_year", "log_loss_worst_year"],
        )

    def test_selection_summary_ranks_by_worst_year_without_mean(self) -> None:
        per_fold = pd.DataFrame([
            {"candidate": "baseline", "model_name": "majority_probability", "validation_year": 2020, "test_rows": 10, "accuracy": 0.5, "balanced_accuracy": 0.5, "roc_auc": 0.5, "pr_auc": 0.5, "brier": 0.25, "log_loss": 0.69, "calibration_error": 0.0},
            {"candidate": "baseline", "model_name": "majority_probability", "validation_year": 2021, "test_rows": 10, "accuracy": 0.5, "balanced_accuracy": 0.5, "roc_auc": 0.5, "pr_auc": 0.5, "brier": 0.25, "log_loss": 0.69, "calibration_error": 0.0},
            {"candidate": "strong", "model_name": "test_model", "validation_year": 2020, "test_rows": 10, "accuracy": 0.8, "balanced_accuracy": 0.6, "roc_auc": 0.6, "pr_auc": 0.6, "brier": 0.20, "log_loss": 0.60, "calibration_error": 0.0},
            {"candidate": "strong", "model_name": "test_model", "validation_year": 2021, "test_rows": 10, "accuracy": 0.51, "balanced_accuracy": 0.51, "roc_auc": 0.55, "pr_auc": 0.55, "brier": 0.24, "log_loss": 0.68, "calibration_error": 0.0},
        ])
        summary = _selection_summary(per_fold)
        self.assertEqual(summary.iloc[0]["candidate"], "strong")
        self.assertAlmostEqual(summary.iloc[0]["balanced_accuracy_worst_year"], 0.51)
        self.assertEqual(summary.iloc[0]["baseline_beaten_years"], 2)
        self.assertEqual(len(summary), 2)


if __name__ == "__main__":
    unittest.main()