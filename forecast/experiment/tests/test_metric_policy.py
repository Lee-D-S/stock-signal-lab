from __future__ import annotations

import unittest

import pandas as pd

from forecast.evaluation.metrics import classification_metrics
from forecast.experiment.price_volume_models import SELECTION_POLICY


class MetricPolicyTests(unittest.TestCase):
    def test_probability_metrics_include_log_loss(self) -> None:
        metrics = classification_metrics(
            pd.Series([0, 1, 0, 1]),
            pd.Series([0.1, 0.9, 0.8, 0.2]),
        )
        self.assertIn("log_loss", metrics)
        self.assertGreater(metrics["log_loss"], 0.0)

    def test_selection_policy_uses_balanced_accuracy_first(self) -> None:
        self.assertEqual(SELECTION_POLICY["primary_metric"], "balanced_accuracy")
        self.assertEqual(
            [item["metric"] for item in SELECTION_POLICY["tie_breakers"]],
            ["balanced_accuracy_std", "roc_auc", "brier", "log_loss"],
        )


if __name__ == "__main__":
    unittest.main()
