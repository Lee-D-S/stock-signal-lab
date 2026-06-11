from __future__ import annotations

import unittest
import sys
from pathlib import Path

import pandas as pd

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from generate_watchlist_signals import match_strategy
from run_event_condition_discovery import Predicate, apply_no_overlap, verdict


class EventConditionDiscoveryTest(unittest.TestCase):
    def test_overlap_removes_same_ticker_during_holding_period(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "ticker": "000001",
                    "date": pd.Timestamp("2024-01-02"),
                    "exit_date": pd.Timestamp("2024-01-10"),
                },
                {
                    "ticker": "000001",
                    "date": pd.Timestamp("2024-01-05"),
                    "exit_date": pd.Timestamp("2024-01-15"),
                },
                {
                    "ticker": "000002",
                    "date": pd.Timestamp("2024-01-05"),
                    "exit_date": pd.Timestamp("2024-01-15"),
                },
            ]
        )

        selected = apply_no_overlap(frame)

        self.assertEqual(len(selected), 2)
        self.assertEqual(set(selected["ticker"]), {"000001", "000002"})

    def test_verdict_requires_baseline_lift_in_both_periods(self) -> None:
        baseline = {
            "n": 100,
            "avg_return_pct": 1.0,
            "median_return_pct": 0.3,
            "win_rate_pct": 52.0,
            "profit_factor": 1.2,
        }
        strong = {
            "n": 30,
            "avg_return_pct": 2.0,
            "median_return_pct": 1.0,
            "win_rate_pct": 58.0,
            "profit_factor": 1.5,
        }
        weak_validation = {
            **strong,
            "avg_return_pct": 1.2,
            "win_rate_pct": 53.0,
        }

        self.assertEqual(verdict(strong, strong, baseline, baseline, 20, 0.5, 3.0), "KEEP")
        self.assertEqual(
            verdict(strong, weak_validation, baseline, baseline, 20, 0.5, 3.0),
            "DROP",
        )

    def test_predicate_applies_numeric_threshold(self) -> None:
        frame = pd.DataFrame({"chg_pct": [-7.0, -4.0, 3.0]})
        predicate = Predicate("chg_pct<=-5", "하락률 5% 이상", "chg_pct", "le", -5)

        self.assertEqual(predicate.mask(frame).tolist(), [True, False, False])

    def test_strategy_match_supports_wildcards_and_change_threshold(self) -> None:
        signal = {
            "market_regime": "변동성 장세",
            "direction": "down",
            "chg_pct": -6.0,
            "amount_tag": "거래대금약함",
            "flow_category": "외국인기관동반매수",
            "dart_tag": "DART공시동반",
            "window_category": "직접반응",
        }
        strategy = pd.DataFrame(
            [
                {
                    "hypothesis_id": "NEW-TEST-EV01",
                    "status": "research_candidate",
                    "priority": 1,
                    "use_type": "신규조건 반등 감시 후보",
                    "market_regime": "*",
                    "direction": "down",
                    "amount_tag": "*",
                    "flow_category": "외국인기관동반매수",
                    "dart_tag": "*",
                    "window_category": "*",
                    "min_chg_pct": "",
                    "max_chg_pct": -5,
                    "suggested_response": "관찰",
                    "preferred_entry_mode": "next_open",
                    "preferred_hold_days": 5,
                    "avg_score_return_pct": 1.0,
                    "hit_rate": 0.6,
                    "risk_note": "",
                }
            ]
        )

        matches = match_strategy(signal, strategy)

        self.assertEqual([match["hypothesis_id"] for match in matches], ["NEW-TEST-EV01"])


if __name__ == "__main__":
    unittest.main()
