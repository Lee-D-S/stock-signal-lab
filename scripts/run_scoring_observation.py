"""팩터 스코어링 결과 일별 관찰 기록 및 D+ 수익률 추적.

run_scoring.py --mode screen 이 저장한 CSV를 읽어
임계값 이상 종목을 08_관찰기록/스코어_관찰_로그.csv 에 누적하고
기존 행의 D+ 수익률을 업데이트한다.

Usage:
    python scripts/run_scoring_observation.py                    # 오늘 기준
    python scripts/run_scoring_observation.py --threshold 0.70
    python scripts/run_scoring_observation.py --date 2026-05-01  # 과거 재실행
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd

OBS_DIR  = ROOT / "ai 주가 변동 원인 분석" / "08_관찰기록"
LOG_CSV  = OBS_DIR / "스코어_관찰_로그.csv"
SCREEN_DIR = ROOT / "scripts" / "scoring" / "results"

LOG_COLS = [
    "signal_date", "ticker", "name", "score",
    "momentum_fill", "trend_fill", "value_fill", "fundamental_fill", "volatility_fill",
    "event_close",
    "d_plus_1_open_return_pct", "d_plus_1_close_return_pct",
    "d_plus_5_close_return_pct",
    "d_plus_10_close_return_pct",
    "d_plus_20_close_return_pct",
    "result_label", "review_note",
]

SUMMARY_CSV  = OBS_DIR / "스코어_관찰_일별요약.csv"
SUMMARY_COLS = ["run_date", "threshold", "pool_screened", "new_signals"]


def _append_summary(run_date: str, threshold: float, pool_screened: int, new_signals: int) -> None:
    OBS_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(SUMMARY_CSV, encoding="utf-8-sig") if SUMMARY_CSV.exists() else pd.DataFrame(columns=SUMMARY_COLS)
    df = df[df["run_date"].astype(str) != run_date]
    row = pd.DataFrame(
        [{"run_date": run_date, "threshold": threshold, "pool_screened": pool_screened, "new_signals": new_signals}],
        columns=SUMMARY_COLS,
    )
    pd.concat([df, row], ignore_index=True).to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    print(f"[score-obs] 일별 요약: {run_date} pool={pool_screened} new={new_signals}")


def _load_log(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path, encoding="utf-8-sig")
    return pd.DataFrame(columns=LOG_COLS)


def _return_pct(event_close: float, future: float | None) -> float | None:
    if future is None or event_close == 0:
        return None
    return round((future - event_close) / event_close * 100, 2)


async def _fetch_price_series(ticker: str) -> pd.DataFrame:
    from screener_lib.data import get_ohlcv
    df, _ = await get_ohlcv(ticker)
    return df


async def _update_returns(df: pd.DataFrame, today: date) -> pd.DataFrame:
    if df.empty:
        return df

    return_cols = [
        ("d_plus_1_open_return_pct",   1, "open"),
        ("d_plus_1_close_return_pct",  1, "close"),
        ("d_plus_5_close_return_pct",  5, "close"),
        ("d_plus_10_close_return_pct", 10, "close"),
        ("d_plus_20_close_return_pct", 20, "close"),
    ]

    if return_cols[0][0] not in df.columns:
        for col, _, _ in return_cols:
            df[col] = None

    needs_mask = df[return_cols[0][0]].isna()
    if not needs_mask.any():
        return df

    ohlcv_cache: dict[str, pd.DataFrame] = {}

    for idx in df[needs_mask].index:
        row         = df.loc[idx]
        ticker      = row["ticker"]
        sig_date    = pd.to_datetime(row["signal_date"]).date()
        event_close = row.get("event_close")
        if pd.isna(event_close) or event_close == 0:
            continue

        if ticker not in ohlcv_cache:
            ohlcv_cache[ticker] = await _fetch_price_series(ticker)

        ohlcv = ohlcv_cache[ticker]
        if ohlcv.empty:
            continue

        dates_list  = [d.date() if hasattr(d, "date") else d for d in ohlcv["date"].tolist()]
        closes_list = ohlcv["close"].tolist()
        opens_list  = ohlcv["open"].tolist() if "open" in ohlcv.columns else closes_list
        closes_by_d: dict[date, float] = dict(zip(dates_list, closes_list))
        opens_by_d:  dict[date, float] = dict(zip(dates_list, opens_list))

        trading_after = sorted(d for d in dates_list if d > sig_date)

        def _nth(n: int, kind: str) -> float | None:
            if len(trading_after) < n:
                return None
            d = trading_after[n - 1]
            if d > today:
                return None
            return opens_by_d.get(d) if kind == "open" else closes_by_d.get(d)

        for col, n, kind in return_cols:
            val = _nth(n, kind)
            if val is not None:
                df.at[idx, col] = _return_pct(event_close, val)

    return df


def _save(df: pd.DataFrame) -> None:
    OBS_DIR.mkdir(parents=True, exist_ok=True)
    save_cols = [c for c in LOG_COLS if c in df.columns]
    out = df[save_cols]
    out.to_csv(LOG_CSV, index=False, encoding="utf-8-sig")

    md_path = LOG_CSV.with_suffix(".md")
    with md_path.open("w", encoding="utf-8") as f:
        f.write("# 스코어 관찰 로그\n\n")
        f.write(out.to_markdown(index=False))
        f.write("\n")
    print(f"[score-obs] 저장: {LOG_CSV}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="팩터 스코어 관찰 기록/추적")
    parser.add_argument("--threshold", type=float, default=0.60)
    parser.add_argument("--date", default=None, help="기준일 YYYY-MM-DD (기본: 오늘). run_scoring.py --date 와 맞춰서 사용")
    args = parser.parse_args()

    today     = date.fromisoformat(args.date) if args.date else date.today()
    today_str = today.isoformat()

    screen_csv = SCREEN_DIR / f"screen_{today_str}.csv"
    if not screen_csv.exists():
        print(f"[score-obs] 오늘 스크린 파일 없음: {screen_csv}")
        print("  → run_scoring.py --mode screen 을 먼저 실행하세요.")
        return

    try:
        screen_df = pd.read_csv(screen_csv, encoding="utf-8-sig")
    except Exception:
        screen_df = pd.DataFrame()

    if screen_df.empty or "score" not in screen_df.columns:
        print("[score-obs] 스크린 파일이 비어 있음: D+ 추적만 실행")
        log_df = _load_log(LOG_CSV)
        log_df = await _update_returns(log_df, today)
        _save(log_df)
        _append_summary(today_str, args.threshold, 0, 0)
        return

    above = screen_df[screen_df["score"] >= args.threshold].copy()
    print(f"[score-obs] 임계값 {args.threshold:.0%} 이상: {len(above)}개")

    if above.empty:
        log_df = _load_log(LOG_CSV)
        log_df = await _update_returns(log_df, today)
        _save(log_df)
        _append_summary(today_str, args.threshold, len(screen_df), 0)
        return

    # 현재가 조회 (event_close 채우기)
    from screener_lib.data import get_ohlcv

    new_rows: list[dict] = []
    for _, r in above.iterrows():
        ticker = str(r["ticker"])
        ohlcv, _ = await get_ohlcv(ticker)
        event_close = float(ohlcv["close"].iloc[-1]) if not ohlcv.empty else None

        row: dict = {
            "signal_date":      today_str,
            "ticker":           ticker,
            "name":             r.get("name", ""),
            "score":            round(float(r["score"]), 4),
            "momentum_fill":    r.get("momentum_fill"),
            "trend_fill":       r.get("trend_fill"),
            "value_fill":       r.get("value_fill"),
            "fundamental_fill": r.get("fundamental_fill"),
            "volatility_fill":  r.get("volatility_fill"),
            "event_close":      event_close,
            "result_label":     None,
            "review_note":      None,
        }
        for col in ["d_plus_1_open_return_pct", "d_plus_1_close_return_pct",
                    "d_plus_5_close_return_pct", "d_plus_10_close_return_pct",
                    "d_plus_20_close_return_pct"]:
            row[col] = None
        new_rows.append(row)
        await asyncio.sleep(0.2)

    log_df = _load_log(LOG_CSV)

    deduped_count = 0
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        existing_keys = set(
            zip(log_df["signal_date"].astype(str), log_df["ticker"].astype(str))
        ) if not log_df.empty else set()
        deduped = new_df[
            ~new_df.apply(
                lambda r2: (str(r2["signal_date"]), str(r2["ticker"])) in existing_keys, axis=1
            )
        ]
        if not deduped.empty:
            log_df = pd.concat([log_df, deduped], ignore_index=True)
            deduped_count = len(deduped)
            print(f"[score-obs] 신규 추가: {deduped_count}개")

    log_df = await _update_returns(log_df, today)
    _save(log_df)
    _append_summary(today_str, args.threshold, len(screen_df), deduped_count)


if __name__ == "__main__":
    asyncio.run(main())
