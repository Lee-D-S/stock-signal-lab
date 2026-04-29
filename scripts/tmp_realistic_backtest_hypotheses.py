from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
REVIEW_DIR = BASE_DIR / "05_가설검토"
BACKTEST_DIR = BASE_DIR / "06_백테스트"
CACHE_DIR = ROOT / "data" / "ohlcv_cache"

REVIEW_CSV = REVIEW_DIR / "가설_이벤트_검토.csv"
SUMMARY_CSV = REVIEW_DIR / "가설_이벤트_요약.csv"

TRADES_CSV = BACKTEST_DIR / "가설_실전_백테스트_거래.csv"
SUMMARY_OUT_CSV = BACKTEST_DIR / "가설_실전_백테스트_요약.csv"
MD_OUT = BACKTEST_DIR / "가설_실전_백테스트.md"


def load_ohlcv(ticker: str) -> pd.DataFrame:
    path = CACHE_DIR / f"{ticker}.pkl"
    if not path.is_file():
        return pd.DataFrame()
    df = pd.read_pickle(path)
    if df.empty:
        return df
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def action_side(action_hint: str) -> str:
    if action_hint in {"진입 후보", "반등 관찰 후보"}:
        return "long"
    if action_hint in {"추격매수 회피 후보", "리스크 회피 후보"}:
        return "avoid_long"
    return "ignore"


def find_trade(
    event: pd.Series,
    ohlcv: pd.DataFrame,
    action_hint: str,
    entry_mode: str,
    hold_days: int,
    fee_bps: float,
) -> dict[str, Any]:
    if ohlcv.empty:
        return {"status": "skip", "skip_reason": "ohlcv_cache_missing"}

    event_date = pd.Timestamp(event["date"])
    idxs = ohlcv.index[ohlcv["date"] == event_date].tolist()
    if not idxs:
        return {"status": "skip", "skip_reason": "event_date_not_in_cache"}

    event_idx = idxs[0]
    entry_idx = event_idx + 1
    if entry_idx >= len(ohlcv):
        return {"status": "skip", "skip_reason": "no_next_trading_day"}

    exit_idx = entry_idx + hold_days
    if exit_idx >= len(ohlcv):
        return {"status": "skip", "skip_reason": f"no_d{hold_days}_exit_day"}

    entry_row = ohlcv.iloc[entry_idx]
    exit_row = ohlcv.iloc[exit_idx]
    entry_col = "open" if entry_mode == "next_open" else "close"
    entry_price = float(entry_row[entry_col])
    exit_price = float(exit_row["close"])
    if entry_price <= 0 or exit_price <= 0:
        return {"status": "skip", "skip_reason": "invalid_price"}

    raw_return = (exit_price / entry_price - 1) * 100
    round_trip_fee_pct = fee_bps * 2 / 100
    side = action_side(action_hint)
    if side == "long":
        score_return = raw_return - round_trip_fee_pct
    elif side == "avoid_long":
        score_return = -raw_return - round_trip_fee_pct
    else:
        return {"status": "skip", "skip_reason": "ignored_action"}

    return {
        "status": "ok",
        "skip_reason": "",
        "side": side,
        "entry_date": entry_row["date"].strftime("%Y-%m-%d"),
        "exit_date": exit_row["date"].strftime("%Y-%m-%d"),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "raw_return_pct": raw_return,
        "score_return_pct": score_return,
    }


def build_trades(entry_mode: str, hold_days: int, fee_bps: float, no_overlap: bool) -> pd.DataFrame:
    review = pd.read_csv(REVIEW_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    base_summary = pd.read_csv(SUMMARY_CSV, encoding="utf-8-sig")
    action_map = dict(zip(base_summary["hypothesis_id"], base_summary["action_hint"], strict=False))

    cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    active_until: dict[tuple[str, str], pd.Timestamp] = {}

    review = review.sort_values(["date", "hypothesis_id", "ticker"]).reset_index(drop=True)
    for _, event in review.iterrows():
        hid = event["hypothesis_id"]
        ticker = event["ticker"]
        action_hint = action_map.get(hid, "추가 검토")
        key = (str(hid), str(ticker))
        event_date = pd.Timestamp(event["date"])
        if no_overlap and key in active_until and event_date <= active_until[key]:
            rows.append({**event.to_dict(), "action_hint": action_hint, "status": "skip", "skip_reason": "overlap_same_hypothesis_ticker"})
            continue

        if ticker not in cache:
            cache[ticker] = load_ohlcv(ticker)
        result = find_trade(event, cache[ticker], action_hint, entry_mode, hold_days, fee_bps)
        row = {
            **event.to_dict(),
            "action_hint": action_hint,
            "entry_mode": entry_mode,
            "hold_days": hold_days,
            "fee_bps": fee_bps,
            **result,
        }
        if result.get("status") == "ok":
            active_until[key] = pd.Timestamp(result["exit_date"])
        rows.append(row)
    return pd.DataFrame(rows)


def summarize(trades: pd.DataFrame) -> pd.DataFrame:
    ok = trades[trades["status"] == "ok"].copy()
    if ok.empty:
        return pd.DataFrame()

    rows = []
    for hid, group in ok.groupby("hypothesis_id"):
        all_group = trades[trades["hypothesis_id"] == hid]
        scores = pd.to_numeric(group["score_return_pct"], errors="coerce")
        raw = pd.to_numeric(group["raw_return_pct"], errors="coerce")
        wins = scores > 0
        rows.append(
            {
                "hypothesis_id": hid,
                "market_regime": group["market_regime"].iloc[0],
                "direction": group["direction"].iloc[0],
                "dominant_followup": group["dominant_followup"].iloc[0],
                "action_hint": group["action_hint"].iloc[0],
                "total_events": len(all_group),
                "tested_trades": len(group),
                "skipped_events": len(all_group) - len(group),
                "company_count": group["ticker"].nunique(),
                "avg_raw_return_pct": raw.mean(),
                "avg_score_return_pct": scores.mean(),
                "median_score_return_pct": scores.median(),
                "hit_rate": wins.mean(),
                "worst_score_return_pct": scores.min(),
                "best_score_return_pct": scores.max(),
                "external_review_count": int(group["needs_external_review"].sum()),
            }
        )
    return pd.DataFrame(rows).sort_values(["avg_score_return_pct", "hit_rate"], ascending=[False, False]).reset_index(drop=True)


def fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):+.2f}%"


def fmt_rate(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.2%}"


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_데이터 없음_"
    view = df.copy()
    for col in view.columns:
        if col == "hit_rate":
            view[col] = view[col].map(fmt_rate)
        elif col.endswith("_pct"):
            view[col] = view[col].map(fmt_pct)
        else:
            view[col] = view[col].map(lambda x: "N/A" if pd.isna(x) else x)
    headers = [str(col) for col in view.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for _, row in view.iterrows():
        values = [str(row[col]).replace("|", "\\|") for col in view.columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def build_markdown(summary: pd.DataFrame, trades: pd.DataFrame, entry_mode: str, hold_days: int, fee_bps: float, no_overlap: bool) -> str:
    skip_counts = trades[trades["status"] != "ok"]["skip_reason"].value_counts().reset_index()
    skip_counts.columns = ["skip_reason", "count"]
    lines = [
        "# 국면별 후보 실거래형 백테스트",
        "",
        "## 조건",
        "",
        f"- 진입 기준: {entry_mode}",
        f"- 보유 기간: {hold_days}거래일",
        f"- 거래비용: 편도 {fee_bps:.1f}bp",
        f"- 동일 후보/동일 종목 중복 신호 제거: {'예' if no_overlap else '아니오'}",
        "- 진입 후보/반등 관찰 후보는 long 수익률로 평가했다.",
        "- 추격매수 회피 후보는 해당 신호를 따라 매수했을 때의 손실 회피 효과를 점수화했다.",
        "",
        "## 결과 요약",
        "",
        markdown_table(
            summary[
                [
                    "hypothesis_id",
                    "market_regime",
                    "action_hint",
                    "total_events",
                    "tested_trades",
                    "skipped_events",
                    "company_count",
                    "avg_raw_return_pct",
                    "avg_score_return_pct",
                    "hit_rate",
                    "worst_score_return_pct",
                    "best_score_return_pct",
                    "external_review_count",
                ]
            ]
        ),
        "",
        "## 제외 사유",
        "",
        markdown_table(skip_counts),
        "",
        "## 해석",
        "",
        "- 이 결과는 기존 OHLCV 캐시에 있는 이벤트만 대상으로 한다. 캐시가 2025년 말까지인 종목이 있어 2026년 일부 이벤트는 제외될 수 있다.",
        "- H01/H02/H03처럼 long 또는 반등 관찰 후보는 평균 점수가 플러스이고 적중률이 높을수록 좋다.",
        "- H04/H06처럼 회피 후보는 점수가 플러스일수록 추격매수를 피하는 효과가 있었다는 뜻이다.",
        "- 다음 단계에서는 캐시를 2026년까지 보강하거나 API로 부족 구간을 채운 뒤 같은 스크립트를 다시 실행해야 한다.",
        "",
    ]
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entry-mode", choices=["next_open", "next_close"], default="next_open")
    parser.add_argument("--hold-days", type=int, default=10)
    parser.add_argument("--fee-bps", type=float, default=15.0)
    parser.add_argument("--allow-overlap", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    no_overlap = not args.allow_overlap
    trades = build_trades(args.entry_mode, args.hold_days, args.fee_bps, no_overlap)
    summary = summarize(trades)
    trades.to_csv(TRADES_CSV, index=False, encoding="utf-8-sig")
    summary.to_csv(SUMMARY_OUT_CSV, index=False, encoding="utf-8-sig")
    MD_OUT.write_text(build_markdown(summary, trades, args.entry_mode, args.hold_days, args.fee_bps, no_overlap), encoding="utf-8")
    print(f"events={len(trades)} tested={(trades['status'] == 'ok').sum()} skipped={(trades['status'] != 'ok').sum()}")
    print(f"summary_rows={len(summary)}")
    print(f"trades_csv={TRADES_CSV}")
    print(f"summary_csv={SUMMARY_OUT_CSV}")
    print(f"md={MD_OUT}")


if __name__ == "__main__":
    main()
