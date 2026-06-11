from __future__ import annotations

import argparse
import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

from analysis_paths import RAW_DIR, SNAPSHOT_DIR  # noqa: E402

EVENTS_CSV = RAW_DIR / "이벤트.csv"
CACHE_DIR = ROOT / "data" / "ohlcv_cache"

CATEGORICAL_FEATURES = [
    "market_regime",
    "amount_tag",
    "flow_category",
    "dart_tag",
    "window_category",
    "needs_external_review",
]
OBSERVABLE_FEATURES = {
    "market_regime",
    "amount_tag",
    "flow_category",
    "dart_tag",
    "window_category",
    "chg_pct",
}
CHANGE_THRESHOLDS = [3.0, 5.0, 7.0, 10.0]


@dataclass(frozen=True)
class Predicate:
    key: str
    description: str
    column: str
    operator: str
    value: Any

    def mask(self, frame: pd.DataFrame) -> pd.Series:
        series = frame[self.column]
        if self.operator == "eq":
            return series == self.value
        if self.operator == "ge":
            return pd.to_numeric(series, errors="coerce") >= float(self.value)
        if self.operator == "le":
            return pd.to_numeric(series, errors="coerce") <= float(self.value)
        raise ValueError(f"unsupported operator: {self.operator}")


def read_events(path: Path) -> pd.DataFrame:
    events = pd.read_csv(path, encoding="utf-8-sig", dtype={"ticker": str})
    events["ticker"] = events["ticker"].astype(str).str.zfill(6)
    events["date"] = pd.to_datetime(events["date"])
    events["chg_pct"] = pd.to_numeric(events["chg_pct"], errors="coerce")
    return events


def load_ohlcv(ticker: str) -> pd.DataFrame:
    path = CACHE_DIR / f"{ticker}.pkl"
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_pickle(path)
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    return frame.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)


def build_trade_records(
    events: pd.DataFrame,
    hold_days: int,
    fee_bps: float,
) -> pd.DataFrame:
    cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    round_trip_fee_pct = fee_bps * 2 / 100

    for _, event in events.sort_values(["ticker", "date"]).iterrows():
        ticker = event["ticker"]
        if ticker not in cache:
            cache[ticker] = load_ohlcv(ticker)
        ohlcv = cache[ticker]
        result: dict[str, Any] = {
            **event.to_dict(),
            "status": "skip",
            "skip_reason": "ohlcv_cache_missing",
            "entry_date": "",
            "exit_date": "",
            "entry_price": "",
            "exit_price": "",
            "return_pct": "",
        }
        if ohlcv.empty:
            rows.append(result)
            continue

        positions = ohlcv.index[ohlcv["date"] == event["date"]].tolist()
        if not positions:
            result["skip_reason"] = "event_date_not_in_cache"
            rows.append(result)
            continue
        entry_idx = positions[0] + 1
        exit_idx = entry_idx + hold_days
        if entry_idx >= len(ohlcv):
            result["skip_reason"] = "no_next_trading_day"
            rows.append(result)
            continue
        if exit_idx >= len(ohlcv):
            result["skip_reason"] = f"no_d{hold_days}_exit_day"
            rows.append(result)
            continue

        entry = ohlcv.iloc[entry_idx]
        exit_row = ohlcv.iloc[exit_idx]
        entry_price = float(entry["open"])
        exit_price = float(exit_row["close"])
        if entry_price <= 0 or exit_price <= 0:
            result["skip_reason"] = "invalid_price"
            rows.append(result)
            continue

        result.update(
            {
                "status": "ok",
                "skip_reason": "",
                "entry_date": entry["date"],
                "exit_date": exit_row["date"],
                "entry_price": entry_price,
                "exit_price": exit_price,
                "return_pct": (exit_price / entry_price - 1) * 100 - round_trip_fee_pct,
            }
        )
        rows.append(result)
    return pd.DataFrame(rows)


def build_predicates(events: pd.DataFrame, direction: str) -> list[Predicate]:
    predicates: list[Predicate] = []
    for column in CATEGORICAL_FEATURES:
        values = events[column].dropna().unique().tolist()
        for value in sorted(values, key=str):
            predicates.append(
                Predicate(
                    key=f"{column}={value}",
                    description=f"{column}={value}",
                    column=column,
                    operator="eq",
                    value=value,
                )
            )

    for threshold in CHANGE_THRESHOLDS:
        if direction == "down":
            predicates.append(
                Predicate(
                    key=f"chg_pct<=-{threshold:g}",
                    description=f"당일 하락률 {threshold:g}% 이상",
                    column="chg_pct",
                    operator="le",
                    value=-threshold,
                )
            )
        else:
            predicates.append(
                Predicate(
                    key=f"chg_pct>={threshold:g}",
                    description=f"당일 상승률 {threshold:g}% 이상",
                    column="chg_pct",
                    operator="ge",
                    value=threshold,
                )
            )
    return predicates


def non_redundant_pair(left: Predicate, right: Predicate) -> bool:
    if left.column != right.column:
        return True
    return left.column == "chg_pct" and left.operator != right.operator


def apply_no_overlap(group: pd.DataFrame) -> pd.DataFrame:
    selected: list[int] = []
    active_until: dict[str, pd.Timestamp] = {}
    for idx, row in group.sort_values(["date", "ticker"]).iterrows():
        ticker = row["ticker"]
        event_date = pd.Timestamp(row["date"])
        if ticker in active_until and event_date <= active_until[ticker]:
            continue
        selected.append(idx)
        active_until[ticker] = pd.Timestamp(row["exit_date"])
    return group.loc[selected].copy()


def profit_factor(returns: pd.Series) -> float:
    positive = float(returns[returns > 0].sum())
    negative = float(-returns[returns < 0].sum())
    if negative == 0:
        return 99.9 if positive > 0 else 0.0
    return min(positive / negative, 99.9)


def stats(group: pd.DataFrame) -> dict[str, Any]:
    returns = pd.to_numeric(group["return_pct"], errors="coerce").dropna()
    if returns.empty:
        return {
            "n": 0,
            "ticker_count": 0,
            "avg_return_pct": "",
            "median_return_pct": "",
            "win_rate_pct": "",
            "profit_factor": "",
            "worst_return_pct": "",
        }
    return {
        "n": len(returns),
        "ticker_count": group.loc[returns.index, "ticker"].nunique(),
        "avg_return_pct": round(float(returns.mean()), 2),
        "median_return_pct": round(float(returns.median()), 2),
        "win_rate_pct": round(float((returns > 0).mean() * 100), 1),
        "profit_factor": round(profit_factor(returns), 2),
        "worst_return_pct": round(float(returns.min()), 2),
    }


def verdict(
    train: dict[str, Any],
    validation: dict[str, Any],
    train_baseline: dict[str, Any],
    validation_baseline: dict[str, Any],
    min_samples: int,
    min_avg_lift_pct: float,
    min_win_rate_lift_pct: float,
) -> str:
    if train["n"] < min_samples or validation["n"] < min_samples:
        return "DROP_SAMPLE"
    lift_positive = (
        train["avg_return_pct"] >= train_baseline["avg_return_pct"] + min_avg_lift_pct
        and validation["avg_return_pct"]
        >= validation_baseline["avg_return_pct"] + min_avg_lift_pct
        and train["win_rate_pct"] >= train_baseline["win_rate_pct"] + min_win_rate_lift_pct
        and validation["win_rate_pct"]
        >= validation_baseline["win_rate_pct"] + min_win_rate_lift_pct
    )
    train_positive = (
        train["avg_return_pct"] > 0
        and train["median_return_pct"] > 0
        and train["win_rate_pct"] >= 50
        and train["profit_factor"] > 1
    )
    validation_positive = (
        validation["avg_return_pct"] > 0
        and validation["median_return_pct"] > 0
        and validation["win_rate_pct"] >= 50
        and validation["profit_factor"] > 1
    )
    if train_positive and validation_positive and lift_positive:
        if validation["win_rate_pct"] >= 55 and validation["profit_factor"] >= 1.2:
            return "KEEP"
        return "CHECK"
    return "DROP"


def evaluate_condition(
    records: pd.DataFrame,
    predicates: tuple[Predicate, ...],
    train_end: pd.Timestamp,
    validation_end: pd.Timestamp,
    min_samples: int,
    train_baseline: dict[str, Any],
    validation_baseline: dict[str, Any],
    min_avg_lift_pct: float,
    min_win_rate_lift_pct: float,
) -> dict[str, Any]:
    mask = pd.Series(True, index=records.index)
    for predicate in predicates:
        mask &= predicate.mask(records)
    matched = apply_no_overlap(records[mask])
    train_group = matched[matched["date"] <= train_end]
    validation_group = matched[
        (matched["date"] > train_end) & (matched["date"] <= validation_end)
    ]
    train_stats = stats(train_group)
    validation_stats = stats(validation_group)
    return {
        "condition_count": len(predicates),
        "condition_key": " & ".join(predicate.key for predicate in predicates),
        "description": " + ".join(predicate.description for predicate in predicates),
        **{f"train_{key}": value for key, value in train_stats.items()},
        **{f"validation_{key}": value for key, value in validation_stats.items()},
        "train_avg_lift_pct": round(
            float(train_stats["avg_return_pct"] - train_baseline["avg_return_pct"]), 2
        )
        if train_stats["n"]
        else "",
        "validation_avg_lift_pct": round(
            float(validation_stats["avg_return_pct"] - validation_baseline["avg_return_pct"]), 2
        )
        if validation_stats["n"]
        else "",
        "train_win_rate_lift_pct": round(
            float(train_stats["win_rate_pct"] - train_baseline["win_rate_pct"]), 1
        )
        if train_stats["n"]
        else "",
        "validation_win_rate_lift_pct": round(
            float(validation_stats["win_rate_pct"] - validation_baseline["win_rate_pct"]), 1
        )
        if validation_stats["n"]
        else "",
        "verdict": verdict(
            train_stats,
            validation_stats,
            train_baseline,
            validation_baseline,
            min_samples,
            min_avg_lift_pct,
            min_win_rate_lift_pct,
        ),
        "_signature": "|".join(matched["event_id"].astype(str).sort_values()),
    }


def discover_conditions(
    records: pd.DataFrame,
    direction: str,
    train_end: str,
    validation_end: str,
    min_samples: int,
    max_conditions: int,
    min_avg_lift_pct: float,
    min_win_rate_lift_pct: float,
) -> pd.DataFrame:
    target = records[(records["status"] == "ok") & (records["direction"] == direction)].copy()
    baseline = apply_no_overlap(target)
    train_cutoff = pd.Timestamp(train_end)
    validation_cutoff = pd.Timestamp(validation_end)
    train_baseline = stats(baseline[baseline["date"] <= train_cutoff])
    validation_baseline = stats(
        baseline[(baseline["date"] > train_cutoff) & (baseline["date"] <= validation_cutoff)]
    )
    predicates = build_predicates(target, direction)
    combinations: list[tuple[Predicate, ...]] = [(predicate,) for predicate in predicates]
    if max_conditions >= 2:
        combinations.extend(
            pair
            for pair in itertools.combinations(predicates, 2)
            if non_redundant_pair(*pair)
        )

    rows = [
        evaluate_condition(
            target,
            combination,
            train_cutoff,
            validation_cutoff,
            min_samples,
            train_baseline,
            validation_baseline,
            min_avg_lift_pct,
            min_win_rate_lift_pct,
        )
        for combination in combinations
    ]
    results = pd.DataFrame(rows)
    order = {"KEEP": 0, "CHECK": 1, "DROP": 2, "DROP_SAMPLE": 3}
    results["sort_key"] = results["verdict"].map(order).fillna(9)
    results = results.sort_values(
        [
            "sort_key",
            "condition_count",
            "validation_profit_factor",
            "validation_avg_return_pct",
            "validation_n",
        ],
        ascending=[True, True, False, False, False],
    )
    results = results.drop_duplicates("_signature", keep="first")
    return (
        results.drop(columns=["sort_key", "_signature"])
        .sort_values(
            [
                "verdict",
                "validation_profit_factor",
                "validation_avg_return_pct",
                "validation_n",
            ],
            key=lambda column: column.map(order) if column.name == "verdict" else column,
            ascending=[True, False, False, False],
        )
        .reset_index(drop=True)
    )


def assign_candidate_ids(results: pd.DataFrame, snapshot_date: str, limit: int) -> pd.DataFrame:
    candidates = results[results["verdict"].isin(["KEEP", "CHECK"])].head(limit).copy()
    candidates.insert(
        0,
        "candidate_id",
        [f"NEW-{snapshot_date.replace('-', '')}-EV{index:02d}" for index in range(1, len(candidates) + 1)],
    )
    candidates.insert(1, "status", "research_candidate")
    return candidates


def build_observation_strategies(
    candidates: pd.DataFrame,
    direction: str,
    hold_days: int,
    limit: int,
) -> pd.DataFrame:
    columns = [
        "priority",
        "hypothesis_id",
        "status",
        "use_type",
        "market_regime",
        "direction",
        "amount_tag",
        "flow_category",
        "dart_tag",
        "window_category",
        "min_chg_pct",
        "max_chg_pct",
        "action_hint",
        "suggested_response",
        "preferred_entry_mode",
        "preferred_hold_days",
        "tested_trades",
        "avg_score_return_pct",
        "hit_rate",
        "risk_note",
    ]
    rows: list[dict[str, Any]] = []
    for _, candidate in candidates.iterrows():
        parts = str(candidate["condition_key"]).split(" & ")
        parsed: dict[str, str] = {}
        supported = True
        for part in parts:
            if "<=" in part:
                column, value = part.split("<=", 1)
                if column not in OBSERVABLE_FEATURES:
                    supported = False
                    break
                parsed["max_chg_pct"] = value
            elif ">=" in part:
                column, value = part.split(">=", 1)
                if column not in OBSERVABLE_FEATURES:
                    supported = False
                    break
                parsed["min_chg_pct"] = value
            elif "=" in part:
                column, value = part.split("=", 1)
                if column not in OBSERVABLE_FEATURES:
                    supported = False
                    break
                parsed[column] = value
        if not supported:
            continue

        rows.append(
            {
                "priority": len(rows) + 1,
                "hypothesis_id": candidate["candidate_id"],
                "status": "research_candidate",
                "use_type": "신규조건 반등 감시 후보" if direction == "down" else "신규조건 매수 후보",
                "market_regime": parsed.get("market_regime", "*"),
                "direction": direction,
                "amount_tag": parsed.get("amount_tag", "*"),
                "flow_category": parsed.get("flow_category", "*"),
                "dart_tag": parsed.get("dart_tag", "*"),
                "window_category": parsed.get("window_category", "*"),
                "min_chg_pct": parsed.get("min_chg_pct", ""),
                "max_chg_pct": parsed.get("max_chg_pct", ""),
                "action_hint": "반등 관찰 후보" if direction == "down" else "진입 후보",
                "suggested_response": "연구 후보: 다음 거래일 시가 기준 가상 진입 후 성과만 관찰",
                "preferred_entry_mode": "next_open",
                "preferred_hold_days": hold_days,
                "tested_trades": int(candidate["validation_n"]),
                "avg_score_return_pct": float(candidate["validation_avg_return_pct"]),
                "hit_rate": float(candidate["validation_win_rate_pct"]) / 100,
                "risk_note": "train/validation 통과 연구 후보. 별도 관찰 20건 전 active 승격 금지",
            }
        )
        if len(rows) >= limit:
            break
    return pd.DataFrame(rows, columns=columns)


def fmt_pct(value: Any) -> str:
    if value == "" or pd.isna(value):
        return ""
    return f"{float(value):+.2f}%"


def build_markdown(
    candidates: pd.DataFrame,
    records: pd.DataFrame,
    args: argparse.Namespace,
) -> str:
    target = records[(records["status"] == "ok") & (records["direction"] == args.direction)]
    baseline = apply_no_overlap(target)
    train_cutoff = pd.Timestamp(args.train_end)
    validation_cutoff = pd.Timestamp(args.validation_end)
    train_baseline = stats(baseline[baseline["date"] <= train_cutoff])
    validation_baseline = stats(
        baseline[(baseline["date"] > train_cutoff) & (baseline["date"] <= validation_cutoff)]
    )
    lines = [
        "# 이벤트 전략 신규 조건 탐색",
        "",
        f"- 스냅샷 기준일: {args.snapshot_date}",
        f"- 방향: {args.direction}",
        f"- 실제 진입 기준: 다음 거래일 시가",
        f"- 보유 기간: {args.hold_days}거래일",
        f"- 거래비용: 편도 {args.fee_bps:.1f}bp",
        f"- Train: {records['date'].min().date()} ~ {args.train_end}",
        f"- Validation: {pd.Timestamp(args.train_end).date()} 이후 ~ {args.validation_end}",
        f"- 조건당 최소 표본: train/validation 각각 {args.min_samples}건",
        f"- 기준 대비 최소 평균수익률 개선: train/validation 각각 {args.min_avg_lift_pct:.2f}%p",
        f"- 기준 대비 최소 승률 개선: train/validation 각각 {args.min_win_rate_lift_pct:.1f}%p",
        (
            f"- Train 기준 성과: n={train_baseline['n']}, 평균 "
            f"{fmt_pct(train_baseline['avg_return_pct'])}, 중앙값 "
            f"{fmt_pct(train_baseline['median_return_pct'])}, 승률 "
            f"{float(train_baseline['win_rate_pct']):.1f}%"
        ),
        (
            f"- Validation 기준 성과: n={validation_baseline['n']}, 평균 "
            f"{fmt_pct(validation_baseline['avg_return_pct'])}, 중앙값 "
            f"{fmt_pct(validation_baseline['median_return_pct'])}, 승률 "
            f"{float(validation_baseline['win_rate_pct']):.1f}%"
        ),
        "- 동일 조건·동일 종목은 보유기간이 겹치는 신호를 제거했다.",
        "- 결과 라벨과 미래 수익률은 조건 변수로 사용하지 않았다.",
        "",
        "## 후보",
        "",
    ]
    if candidates.empty:
        lines.append("_KEEP/CHECK 조건 없음_")
    else:
        lines.extend(
            [
                "| candidate_id | verdict | description | train | validation |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for _, row in candidates.iterrows():
            train = (
                f"n={int(row['train_n'])}, 평균 {fmt_pct(row['train_avg_return_pct'])}, "
                f"중앙값 {fmt_pct(row['train_median_return_pct'])}, "
                f"승률 {float(row['train_win_rate_pct']):.1f}%, PF {float(row['train_profit_factor']):.2f}"
            )
            validation = (
                f"n={int(row['validation_n'])}, 평균 {fmt_pct(row['validation_avg_return_pct'])}, "
                f"중앙값 {fmt_pct(row['validation_median_return_pct'])}, "
                f"승률 {float(row['validation_win_rate_pct']):.1f}%, "
                f"PF {float(row['validation_profit_factor']):.2f}"
            )
            lines.append(
                f"| {row['candidate_id']} | {row['verdict']} | {row['description']} | "
                f"{train} | {validation} |"
            )

    lines.extend(
        [
            "",
            "## 승격 제한",
            "",
            "- 이 결과는 연구 후보이며 active 전략이 아니다.",
            "- 후보 정의를 고정한 뒤 별도 관찰 로그에서 실제 진입 가능 표본 20건 이상을 수집한다.",
            "- Validation 결과가 좋아도 공시 본문 성격과 시장·업종 추세가 아직 분리되지 않아 자동 매수에 사용하지 않는다.",
        "- 공시 호재/악재 세분화 필드는 현재 이벤트 원자료에 없어 `DART공시동반/주변공시부재`만 비교했다.",
        "- daily 신호 필드로 표현 가능한 후보만 별도 관찰 전략 CSV에 기록한다.",
        "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="이벤트 전략 전용 신규 조건 탐색")
    parser.add_argument("--events-csv", type=Path, default=EVENTS_CSV)
    parser.add_argument("--snapshot-date", default="2026-06-11")
    parser.add_argument("--direction", choices=["up", "down"], default="down")
    parser.add_argument("--hold-days", type=int, default=5)
    parser.add_argument("--fee-bps", type=float, default=15.0)
    parser.add_argument("--train-end", default="2023-12-31")
    parser.add_argument("--validation-end", default="2026-03-31")
    parser.add_argument("--min-samples", type=int, default=20)
    parser.add_argument("--max-conditions", type=int, choices=[1, 2], default=2)
    parser.add_argument("--min-avg-lift-pct", type=float, default=0.5)
    parser.add_argument("--min-win-rate-lift-pct", type=float, default=3.0)
    parser.add_argument("--candidate-limit", type=int, default=20)
    parser.add_argument("--observation-limit", type=int, default=5)
    parser.add_argument("--save-trades", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir or SNAPSHOT_DIR / args.snapshot_date
    output_dir.mkdir(parents=True, exist_ok=True)

    events = read_events(args.events_csv)
    records = build_trade_records(events, args.hold_days, args.fee_bps)
    results = discover_conditions(
        records,
        args.direction,
        args.train_end,
        args.validation_end,
        args.min_samples,
        args.max_conditions,
        args.min_avg_lift_pct,
        args.min_win_rate_lift_pct,
    )
    candidates = assign_candidate_ids(results, args.snapshot_date, args.candidate_limit)
    observation_strategies = build_observation_strategies(
        candidates,
        args.direction,
        args.hold_days,
        args.observation_limit,
    )

    prefix = f"이벤트_신규조건_탐색_{args.direction}_d{args.hold_days}"
    records_path = output_dir / f"{prefix}_거래.csv"
    results_path = output_dir / f"{prefix}_전체결과.csv"
    candidates_path = output_dir / f"{prefix}_후보.csv"
    observation_strategy_path = output_dir / f"{prefix}_관찰전략.csv"
    markdown_path = output_dir / f"{prefix}_후보.md"

    if args.save_trades:
        records.to_csv(records_path, index=False, encoding="utf-8-sig")
    results.to_csv(results_path, index=False, encoding="utf-8-sig")
    candidates.to_csv(candidates_path, index=False, encoding="utf-8-sig")
    observation_strategies.to_csv(observation_strategy_path, index=False, encoding="utf-8-sig")
    markdown_path.write_text(build_markdown(candidates, records, args), encoding="utf-8")

    print(f"events={len(events)}")
    print(f"trade_records={(records['status'] == 'ok').sum()}")
    print(f"conditions={len(results)}")
    print(f"keep={(results['verdict'] == 'KEEP').sum()}")
    print(f"check={(results['verdict'] == 'CHECK').sum()}")
    print(f"candidates={len(candidates)}")
    print(f"observation_strategies={len(observation_strategies)}")
    print(f"candidates_csv={candidates_path}")
    print(f"candidates_md={markdown_path}")


if __name__ == "__main__":
    main()
