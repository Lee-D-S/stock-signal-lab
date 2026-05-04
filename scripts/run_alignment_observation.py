"""단기/장기 이동평균선 정배열 일별 관찰 기록 및 D+ 수익률 추적.

Usage:
    python scripts/run_alignment_observation.py                        # 오늘 기준
    python scripts/run_alignment_observation.py --pool-size 500
    python scripts/run_alignment_observation.py --date 2026-04-30     # 과거 날짜 재실행

--date를 지정하면 OHLCV를 그 날짜까지만 잘라 계산한다 (과거 재현 가능).
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

OBS_DIR = ROOT / "ai 주가 변동 원인 분석" / "08_관찰기록"
SHORT_CSV = OBS_DIR / "단기정배열_관찰_로그.csv"
LONG_CSV  = OBS_DIR / "장기정배열_관찰_로그.csv"

SHORT_COLS = [
    "signal_date", "ticker", "name", "price", "change_rate_pct",
    "ma5", "ma20", "ma60", "ma120",
    "price_to_ma5_pct", "price_to_ma20_pct",
    "streak_days",
    "d_plus_1_open_return_pct", "d_plus_1_close_return_pct",
    "d_plus_5_close_return_pct",
    "d_plus_10_close_return_pct",
    "d_plus_20_close_return_pct",
    "result_label", "review_note",
]
LONG_COLS = [
    "signal_date", "ticker", "name", "price", "change_rate_pct",
    "ma60", "ma120", "ma240",
    "price_to_ma60_pct",
    "streak_days",
    "d_plus_1_open_return_pct", "d_plus_1_close_return_pct",
    "d_plus_5_close_return_pct",
    "d_plus_10_close_return_pct",
    "d_plus_20_close_return_pct",
    "result_label", "review_note",
]

SUMMARY_CSV  = OBS_DIR / "정배열_관찰_일별요약.csv"
SUMMARY_COLS = ["run_date", "pool_size", "short_new", "long_new"]


def _append_summary(run_date: str, pool_size: int, short_new: int, long_new: int) -> None:
    OBS_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(SUMMARY_CSV, encoding="utf-8-sig") if SUMMARY_CSV.exists() else pd.DataFrame(columns=SUMMARY_COLS)
    df = df[df["run_date"].astype(str) != run_date]
    row = pd.DataFrame(
        [{"run_date": run_date, "pool_size": pool_size, "short_new": short_new, "long_new": long_new}],
        columns=SUMMARY_COLS,
    )
    pd.concat([df, row], ignore_index=True).to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    print(f"[alignment-obs] 일별 요약: {run_date} pool={pool_size} short={short_new} long={long_new}")


def _sma(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def _count_streak(closes: list[float], kind: str) -> int:
    """연속 정배열 일수 계산."""
    if kind == "short":
        periods = [5, 20, 60, 120]
    else:
        periods = [60, 120, 240]

    min_p = max(periods)
    if len(closes) < min_p:
        return 0

    streak = 0
    # 최신일부터 거슬러 올라가며 카운트
    for end in range(len(closes), min_p - 1, -1):
        sub = closes[:end]
        mas = [_sma(sub, p) for p in periods]
        if any(v is None for v in mas):
            break
        if all(mas[i] > mas[i + 1] for i in range(len(mas) - 1)):
            streak += 1
        else:
            break
    return streak


def _pct(a: float, b: float) -> float | None:
    if b and b != 0:
        return round((a - b) / b * 100, 2)
    return None


def _return_pct(event_close: float, future: float | None) -> float | None:
    if future is None or event_close == 0:
        return None
    return round((future - event_close) / event_close * 100, 2)


async def _fetch_ohlcv_with_dates(ticker: str, target_date: date) -> tuple[list[date], list[float], list[float]]:
    """날짜 포함 OHLCV 조회. target_date 이하 데이터만 반환 (오래된 순).

    Returns:
        (dates, closes, opens)
    """
    from core.api.client import get_marketdata
    from datetime import timedelta

    date_to   = target_date.strftime("%Y%m%d")
    date_from = (target_date - timedelta(days=420)).strftime("%Y%m%d")
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": date_from,
                "FID_INPUT_DATE_2": date_to,
                "FID_PERIOD_DIV_CODE": "D",
            },
            tr_id="FHKST03010100",
        )
        rows = data.get("output2", [])
        dates_out, closes_out, opens_out = [], [], []
        for r in reversed(rows):  # API는 최신순 → 오래된 순으로 변환
            d_str = r.get("stck_bsop_date", "")
            if not d_str or not r.get("stck_clpr"):
                continue
            d = date(int(d_str[:4]), int(d_str[4:6]), int(d_str[6:]))
            if d > target_date:
                continue
            dates_out.append(d)
            closes_out.append(float(r["stck_clpr"]))
            opens_out.append(float(r.get("stck_oprc") or r["stck_clpr"]))
        return dates_out, closes_out, opens_out
    except Exception:
        return [], [], []


async def scan_ticker(
    ticker: str,
    name: str,
    change_rate: str,
    pool_date: date,
    target_date: date,
) -> dict | None:
    """종목 OHLCV 조회 후 단기/장기 정배열 여부와 D+ 수익률 데이터 반환.

    target_date 까지의 데이터만 사용해 정배열을 판단한다 (과거 재현 지원).
    """
    dates, closes, opens_list = await _fetch_ohlcv_with_dates(ticker, target_date)

    if len(closes) < 240:
        return None

    # 단기 정배열
    mas_s = {p: _sma(closes, p) for p in [5, 20, 60, 120]}
    short_aligned = all(v is not None for v in mas_s.values()) and (
        mas_s[5] > mas_s[20] > mas_s[60] > mas_s[120]
    )

    # 장기 정배열
    mas_l = {p: _sma(closes, p) for p in [60, 120, 240]}
    long_aligned = all(v is not None for v in mas_l.values()) and (
        mas_l[60] > mas_l[120] > mas_l[240]
    )

    if not (short_aligned or long_aligned):
        return None

    price      = float(closes[-1])
    event_date = dates[-1]  # target_date 이하 마지막 거래일

    closes_by_date: dict[date, float] = dict(zip(dates, closes))
    opens_by_date:  dict[date, float] = dict(zip(dates, opens_list))

    def _nth_trading(n: int) -> date | None:
        sorted_dates = sorted(d for d in dates if d > event_date)
        return sorted_dates[n - 1] if len(sorted_dates) >= n else None

    def _d_open(n: int) -> float | None:
        d = _nth_trading(n)
        return opens_by_date.get(d) if d else None

    def _d_close(n: int) -> float | None:
        d = _nth_trading(n)
        return closes_by_date.get(d) if d else None

    streak_s = _count_streak(closes, "short") if short_aligned else None
    streak_l = _count_streak(closes, "long")  if long_aligned  else None

    base = {
        "signal_date":             str(pool_date),
        "ticker":                  ticker,
        "name":                    name,
        "price":                   price,
        "change_rate_pct":         float(change_rate) if change_rate else None,
        "event_close":             price,
        "d_plus_1_open_return_pct":  _return_pct(price, _d_open(1)),
        "d_plus_1_close_return_pct": _return_pct(price, _d_close(1)),
        "d_plus_5_close_return_pct": _return_pct(price, _d_close(5)),
        "d_plus_10_close_return_pct":_return_pct(price, _d_close(10)),
        "d_plus_20_close_return_pct":_return_pct(price, _d_close(20)),
        "result_label":  None,
        "review_note":   None,
    }

    short_row = None
    if short_aligned:
        short_row = {
            **base,
            "ma5":   round(mas_s[5],   1),
            "ma20":  round(mas_s[20],  1),
            "ma60":  round(mas_s[60],  1),
            "ma120": round(mas_s[120], 1),
            "price_to_ma5_pct":  _pct(price, mas_s[5]),
            "price_to_ma20_pct": _pct(price, mas_s[20]),
            "streak_days": streak_s,
        }

    long_row = None
    if long_aligned:
        long_row = {
            **base,
            "ma60":  round(mas_l[60],  1),
            "ma120": round(mas_l[120], 1),
            "ma240": round(mas_l[240], 1),
            "price_to_ma60_pct": _pct(price, mas_l[60]),
            "streak_days": streak_l,
        }

    return {"short": short_row, "long": long_row}


def _load_log(path: Path, cols: list[str]) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path, encoding="utf-8-sig")
    return pd.DataFrame(columns=cols)


async def _update_returns(df: pd.DataFrame, today: date) -> pd.DataFrame:
    """미완성 D+ 수익률 행을 업데이트한다."""
    if df.empty:
        return df

    return_cols = [
        ("d_plus_1_open_return_pct",  1, "open"),
        ("d_plus_1_close_return_pct", 1, "close"),
        ("d_plus_5_close_return_pct", 5, "close"),
        ("d_plus_10_close_return_pct",10, "close"),
        ("d_plus_20_close_return_pct",20, "close"),
    ]

    needs_update_mask = df[return_cols[0][0]].isna()
    if not needs_update_mask.any():
        return df

    # D+ 추적은 항상 오늘까지의 전체 데이터 필요
    ohlcv_cache: dict[str, tuple[list[date], list[float], list[float]]] = {}

    for idx in df[needs_update_mask].index:
        row         = df.loc[idx]
        ticker      = row["ticker"]
        sig_date    = pd.to_datetime(row["signal_date"]).date()
        event_close = row.get("event_close") or row.get("price")

        if pd.isna(event_close) or event_close == 0:
            continue

        if ticker not in ohlcv_cache:
            ohlcv_cache[ticker] = await _fetch_ohlcv_with_dates(ticker, today)

        dates_list, closes_list, opens_list = ohlcv_cache[ticker]
        if not closes_list:
            continue

        closes_by_d: dict[date, float] = dict(zip(dates_list, closes_list))
        opens_by_d:  dict[date, float] = dict(zip(dates_list, opens_list))

        trading_after = sorted(d for d in dates_list if d > sig_date)

        def _nth(n: int, kind: str, _ta: list = trading_after, _today: date = today) -> float | None:
            if len(_ta) < n:
                return None
            d = _ta[n - 1]
            if d > _today:
                return None
            return opens_by_d.get(d) if kind == "open" else closes_by_d.get(d)

        for col, n, kind in return_cols:
            val = _nth(n, kind)
            if val is not None:
                df.at[idx, col] = round((val - event_close) / event_close * 100, 2)

    return df


def _save(df: pd.DataFrame, path: Path, cols: list[str]) -> None:
    OBS_DIR.mkdir(parents=True, exist_ok=True)
    save_cols = [c for c in cols if c in df.columns]
    out = df[save_cols]
    out.to_csv(path, index=False, encoding="utf-8-sig")

    md_path = path.with_suffix(".md")
    with md_path.open("w", encoding="utf-8") as f:
        f.write(f"# {path.stem}\n\n")
        f.write(out.to_markdown(index=False))
        f.write("\n")


async def main() -> None:
    parser = argparse.ArgumentParser(description="단기/장기 정배열 관찰 기록/추적")
    parser.add_argument("--pool-size", type=int, default=300, help="시총 상위 N개 (기본: 300)")
    parser.add_argument("--date", default=None, help="기준일 YYYY-MM-DD (기본: 오늘). 지정 시 그 날짜까지 OHLCV를 잘라 계산")
    parser.add_argument("--delay", type=float, default=0.35)
    args = parser.parse_args()

    today     = date.fromisoformat(args.date) if args.date else date.today()
    today_str = today.isoformat()

    from screener_lib.universe import get_stock_universe

    print(f"[alignment-obs] 유니버스 조회 중... (top={args.pool_size})")
    universe = await get_stock_universe(by="marcap")
    stocks   = universe[: args.pool_size]
    print(f"[alignment-obs] {len(stocks)}개 종목 스캔 중...")

    short_new: list[dict] = []
    long_new:  list[dict] = []

    for i, stock in enumerate(stocks, 1):
        ticker      = stock["ticker"]
        name        = stock.get("name", "")
        change_rate = stock.get("change_rate", "0")

        result = await scan_ticker(ticker, name, change_rate, today, target_date=today)
        if result:
            if result["short"]:
                short_new.append(result["short"])
            if result["long"]:
                long_new.append(result["long"])

        if i % 50 == 0:
            print(f"[alignment-obs] {i}/{len(stocks)} 완료  단기:{len(short_new)}  장기:{len(long_new)}")

        await asyncio.sleep(args.delay)

    print(f"[alignment-obs] 스캔 완료: 단기 {len(short_new)}개, 장기 {len(long_new)}개")
    _append_summary(today_str, len(stocks), len(short_new), len(long_new))

    for label, csv_path, cols, new_rows in [
        ("단기", SHORT_CSV, SHORT_COLS, short_new),
        ("장기", LONG_CSV,  LONG_COLS,  long_new),
    ]:
        log_df = _load_log(csv_path, cols)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            # 당일 동일 티커 중복 제거
            existing_keys = set(
                zip(log_df["signal_date"].astype(str), log_df["ticker"].astype(str))
            ) if not log_df.empty else set()
            deduped = new_df[
                ~new_df.apply(lambda r: (str(r["signal_date"]), str(r["ticker"])) in existing_keys, axis=1)
            ]
            if not deduped.empty:
                log_df = pd.concat([log_df, deduped], ignore_index=True)
                print(f"[alignment-obs] {label} 신규 추가: {len(deduped)}개")
            else:
                print(f"[alignment-obs] {label} 신규 없음 (당일 이미 기록됨)")

        # D+ 수익률 채우기
        log_df = await _update_returns(log_df, today)

        _save(log_df, csv_path, cols)
        print(f"[alignment-obs] {label} 저장 완료: {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
