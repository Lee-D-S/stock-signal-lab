from __future__ import annotations

import unittest

import pandas as pd

from forecast.data.point_in_time import enforce_public_date


class PointInTimeTests(unittest.TestCase):
    def test_future_public_value_is_rejected(self) -> None:
        frame = pd.DataFrame({"date": ["2024-01-01"], "public_at": ["2024-01-02"]})
        with self.assertRaises(ValueError):
            enforce_public_date(frame)

    def test_public_value_on_or_before_observation_is_allowed(self) -> None:
        frame = pd.DataFrame({"date": ["2024-01-01"], "public_at": ["2023-12-31"]})
        self.assertEqual(len(enforce_public_date(frame)), 1)
