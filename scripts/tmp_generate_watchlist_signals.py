from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from scripts.screener_lib.dart import get_corp_code_map  # noqa: E402
from tmp_quarterly_stock_analysis import (  # noqa: E402
    fetch_dart_disclosures,
    fetch_investor_range,
    fetch_ohlcv,
)


BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
STRATEGY_DIR = BASE_DIR / "07_전략신호"

STRATEGY_CSV = STRATEGY_DIR / "전략_조건_초안.csv"
WATCHLIST_CSV = STRATEGY_DIR / "관심종목_시그널_후보.csv"
WATCHLIST_MD = STRATEGY_DIR / "관심종목_시그널_후보.md"
SCAN_CSV = STRATEGY_DIR / "관심종목_시그널_스캔.csv"
UNIVERSE_CSV = STRATEGY_DIR / "거래대금_상위_유니버스.csv"

COMPANIES = [
    ("005930", "삼성전자", None),
    ("000660", "SK하이닉스", None),
    ("047040", "대우건설", None),
    ("006400", "삼성SDI", None),
    ("005490", "POSCO홀딩스", None),
    ("001440", "대한전선", None),
    ("001510", "SK증권", None),
    ("005935", "삼성전자우", "005930"),
    ("042700", "한미반도체", None),
    ("009150", "삼성전기", None),
    ("222080", "씨아이에스", None),
    ("066570", "LG전자", None),
    ("402340", "SK스퀘어", None),
    ("267260", "HD현대일렉트릭", None),
    ("034020", "두산에너빌리티", None),
    ("298380", "에이비엘바이오", None),
    ("322000", "HD현대에너지솔루션", None),
    ("000720", "현대건설", None),
    ("010170", "대한광통신", None),
    ("012450", "한화에어로스페이스", None),
    ("028050", "삼성E&A", None),
    ("298040", "효성중공업", None),
    ("086520", "에코프로", None),
    ("329180", "HD현대중공업", None),
    ("006360", "GS건설", None),
    ("241520", "DSC인베스트먼트", None),
    ("490470", "세미파이브", None),
]


def load_companies(universe_csv: Path | None = None) -> list[tuple[str, str, str | None]]:
    path = universe_csv or UNIVERSE_CSV
    if path.exists():
        universe = pd.read_csv(path, encoding="utf-8-sig", dtype={"ticker": str})
        if not universe.empty and {"ticker", "name"}.issubset(universe.columns):
            rows = []
            for _, row in universe.iterrows():
                ticker = str(row["ticker"]).zfill(6)
                name = str(row["name"]).strip()
                if ticker and name:
                    rows.append((ticker, name, None))
            if rows:
                return rows
    return COMPANIES


def quarter_code_for_date(date: pd.Timestamp) -> str:
    quarter = (date.month - 1) // 3 + 1
    return f"{date.year}_Q{quarter}"


def quarter_start_for_date(date: pd.Timestamp) -> pd.Timestamp:
    month = ((date.month - 1) // 3) * 3 + 1
    return pd.Timestamp(year=date.year, month=month, day=1)


def market_regime_for_date(date: pd.Timestamp) -> str:
    quarter = (date.month - 1) // 3 + 1
    ordinal = date.year * 4 + quarter
    if 2021 * 4 + 2 <= ordinal <= 2021 * 4 + 4:
        return "유동성/저금리 후반장"
    if 2022 * 4 + 1 <= ordinal <= 2023 * 4 + 1:
        return "금리 인상/긴축장"
    if 2023 * 4 + 2 <= ordinal <= 2023 * 4 + 4:
        return "반도체 반등장"
    if 2024 * 4 + 1 <= ordinal <= 2025 * 4 + 2:
        return "AI/전력기기 테마장"
    if 2025 * 4 + 3 <= ordinal <= 2026 * 4 + 2:
        return "변동성 장세"
    return "미분류"


def amount_tag(trade_amount: float, avg_amount: float) -> str:
    if avg_amount and trade_amount >= avg_amount * 2:
        return "거래대금급증"
    if avg_amount and trade_amount >= avg_amount:
        return "거래대금평균상회"
    return "거래대금약함"


def flow_category(investor: pd.DataFrame, event_date: pd.Timestamp) -> tuple[str, float | None, float | None, float | None]:
    if investor.empty:
        return "수급정보부족", None, None, None
    win = investor[(investor["date"] >= event_date - pd.Timedelta(days=5)) & (investor["date"] <= event_date)]
    if win.empty:
        return "수급정보부족", None, None, None
    foreign = win["foreign_qty"].sum(min_count=1)
    institution = win["institution_qty"].sum(min_count=1)
    individual = win["individual_qty"].sum(min_count=1)
    if pd.notna(foreign) and pd.notna(institution):
        if foreign > 0 and institution > 0:
            return "외국인기관동반매수", float(foreign), float(institution), float(individual)
        if foreign < 0 and institution < 0:
            return "외국인기관동반매도", float(foreign), float(institution), float(individual)
        return "수급엇갈림", float(foreign), float(institution), float(individual)
    return "수급정보부족", None, None, None


def dart_tag(disclosures: list[dict[str, Any]], event_date: pd.Timestamp) -> tuple[str, int, str]:
    names = []
    for disc in disclosures:
        dt = pd.to_datetime(disc.get("rcept_dt"), format="%Y%m%d", errors="coerce")
        if pd.notna(dt) and event_date - pd.Timedelta(days=5) <= dt <= event_date:
            names.append(f"{dt.strftime('%Y-%m-%d')} {disc.get('report_nm', '')}")
    return ("DART공시동반" if names else "주변공시부재", len(names), " / ".join(names[:3]))


def window_category(ohlcv: pd.DataFrame, investor: pd.DataFrame, event_date: pd.Timestamp, direction: str) -> str:
    prior_20 = ohlcv[(ohlcv["date"] < event_date) & (ohlcv["date"] >= event_date - pd.Timedelta(days=35))]
    prior_120 = ohlcv[(ohlcv["date"] < event_date) & (ohlcv["date"] >= event_date - pd.Timedelta(days=190))]
    prior_inv = investor[(investor["date"] < event_date) & (investor["date"] >= event_date - pd.Timedelta(days=35))]

    def ret(df: pd.DataFrame) -> float | None:
        if len(df) < 2 or not float(df.iloc[0]["close"]):
            return None
        return (float(df.iloc[-1]["close"]) / float(df.iloc[0]["close"]) - 1) * 100

    r20 = ret(prior_20)
    r120 = ret(prior_120)
    foreign_20 = prior_inv["foreign_qty"].sum(min_count=1) if not prior_inv.empty else None
    inst_20 = prior_inv["institution_qty"].sum(min_count=1) if not prior_inv.empty else None

    if direction == "up":
        if r20 is not None and r20 > 5:
            return "선반영"
        if r120 is not None and r120 > 10:
            return "누적배경"
        if foreign_20 is not None and inst_20 is not None and foreign_20 > 0 and inst_20 > 0:
            return "누적배경"
    else:
        if r20 is not None and r20 < -5:
            return "선반영"
        if r120 is not None and abs(r120) > 10:
            return "누적배경"
    return "직접반응"


def match_strategy(signal: dict[str, Any], strategies: pd.DataFrame) -> list[dict[str, Any]]:
    matches = []
    for _, strategy in strategies.iterrows():
        if signal["market_regime"] != strategy["market_regime"]:
            continue
        if signal["direction"] != strategy["direction"]:
            continue
        if signal["amount_tag"] != strategy["amount_tag"]:
            continue
        flow_match_quality = "exact"
        if signal["flow_category"] != strategy["flow_category"]:
            if signal["flow_category"] == "수급정보부족":
                flow_match_quality = "flow_check_required"
            else:
                continue
        if signal["dart_tag"] != strategy["dart_tag"]:
            continue
        if signal["window_category"] != strategy["window_category"]:
            continue
        matches.append(
            {
                "hypothesis_id": strategy["hypothesis_id"],
                "priority": int(strategy["priority"]),
                "use_type": strategy["use_type"],
                "suggested_response": strategy["suggested_response"],
                "preferred_entry_mode": strategy["preferred_entry_mode"],
                "preferred_hold_days": int(strategy["preferred_hold_days"]),
                "backtest_avg_score_pct": float(strategy["avg_score_return_pct"]),
                "backtest_hit_rate": float(strategy["hit_rate"]),
                "risk_note": strategy.get("risk_note", ""),
                "match_quality": flow_match_quality,
                "required_flow_category": strategy["flow_category"],
            }
        )
    return matches


def latest_row_for_date(ohlcv: pd.DataFrame, target_date: pd.Timestamp | None) -> pd.Series | None:
    if ohlcv.empty:
        return None
    df = ohlcv.sort_values("date").reset_index(drop=True)
    if target_date is not None:
        df = df[df["date"] <= target_date]
    if df.empty:
        return None
    return df.iloc[-1]


async def analyze_company(
    ticker: str,
    name: str,
    corp_ticker: str | None,
    corp_map: dict[str, str],
    strategies: pd.DataFrame,
    target_date: pd.Timestamp | None,
    lookback_days: int,
) -> list[dict[str, Any]]:
    end = target_date or pd.Timestamp.today().normalize()
    start = end - pd.Timedelta(days=lookback_days)
    try:
        ohlcv = await fetch_ohlcv(ticker, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        if ohlcv.empty:
            return []
        row = latest_row_for_date(ohlcv, target_date)
        if row is None or pd.isna(row.get("chg_pct")):
            return []
        event_date = pd.Timestamp(row["date"])
        q_start = quarter_start_for_date(event_date)
        investor_start = (event_date - pd.Timedelta(days=40)).strftime("%Y-%m-%d")
        investor_error = ""
        dart_error = ""
        try:
            investor = await fetch_investor_range(ticker, investor_start, event_date.strftime("%Y-%m-%d"))
        except Exception as exc:
            investor = pd.DataFrame(columns=["date", "foreign_qty", "institution_qty", "individual_qty"])
            investor_error = repr(exc)
        try:
            disclosures = await fetch_dart_disclosures(
                corp_map[corp_ticker or ticker],
                (event_date - pd.Timedelta(days=7)).strftime("%Y-%m-%d"),
                event_date.strftime("%Y-%m-%d"),
            )
        except Exception as exc:
            disclosures = []
            dart_error = repr(exc)
    except Exception as exc:
        return [
            {
                "ticker": ticker,
                "name": name,
                "status": "error",
                "error": repr(exc),
            }
        ]

    qdf = ohlcv[ohlcv["date"] >= q_start].copy()
    avg_amount = float(qdf["trade_amount"].mean()) if not qdf.empty else float(ohlcv["trade_amount"].mean())
    direction = "up" if float(row["chg_pct"]) > 0 else "down" if float(row["chg_pct"]) < 0 else "flat"
    if direction == "flat":
        return []

    flow, foreign, institution, individual = flow_category(investor, event_date)
    dtag, disclosure_count, disclosure_names = dart_tag(disclosures, event_date)
    wcat = "직접반응" if dtag == "DART공시동반" else window_category(ohlcv, investor, event_date, direction)
    signal = {
        "ticker": ticker,
        "name": name,
        "signal_date": event_date.strftime("%Y-%m-%d"),
        "quarter": quarter_code_for_date(event_date),
        "market_regime": market_regime_for_date(event_date),
        "direction": direction,
        "chg_pct": float(row["chg_pct"]),
        "close": float(row["close"]),
        "trade_amount": float(row["trade_amount"]),
        "avg_trade_amount": avg_amount,
        "amount_tag": amount_tag(float(row["trade_amount"]), avg_amount),
        "flow_category": flow,
        "foreign_5d": foreign,
        "institution_5d": institution,
        "individual_5d": individual,
        "dart_tag": dtag,
        "disclosure_count": disclosure_count,
        "disclosure_names": disclosure_names,
        "window_category": wcat,
        "investor_error": investor_error,
        "dart_error": dart_error,
        "status": "ok",
        "error": "",
    }
    matches = match_strategy(signal, strategies)
    if not matches:
        return [{**signal, "is_candidate": False, "no_match_reason": "strategy_condition_not_matched"}]
    return [{**signal, **match, "is_candidate": True, "no_match_reason": ""} for match in matches]


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_후보 없음_"
    view = df.copy()
    for col in ["chg_pct", "backtest_avg_score_pct"]:
        if col in view.columns:
            view[col] = view[col].map(lambda x: "N/A" if pd.isna(x) else f"{float(x):+.2f}%")
    if "backtest_hit_rate" in view.columns:
        view["backtest_hit_rate"] = view["backtest_hit_rate"].map(lambda x: "N/A" if pd.isna(x) else f"{float(x):.2%}")
    headers = [str(col) for col in view.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for _, row in view.iterrows():
        values = [str(row[col]).replace("|", "\\|") for col in view.columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def build_markdown(df: pd.DataFrame, target_date: str, title: str = "일별 전략 감시 후보") -> str:
    lines = [
        f"# {title}",
        "",
        f"- 기준일: {target_date}",
        f"- 후보 수: {len(df):,}건",
        "- 이 파일은 확정 매수 지시가 아니라 과거 패턴과 유사한 감시 후보 목록이다.",
        "- `match_quality=flow_check_required`는 수급 API 시간 제한 등으로 필수 수급 조건을 아직 확인하지 못했다는 뜻이다.",
        "- 이 경우 자동 매수 대상이 아니라 수급 재조회 후 확정해야 하는 관찰 후보로만 본다.",
        "",
        "## 후보",
        "",
        markdown_table(
            df[
                [
                    "priority",
                    "hypothesis_id",
                    "use_type",
                    "ticker",
                    "name",
                    "signal_date",
                    "market_regime",
                    "direction",
                    "chg_pct",
                    "amount_tag",
                    "flow_category",
                    "required_flow_category",
                    "match_quality",
                    "dart_tag",
                    "window_category",
                    "suggested_response",
                    "backtest_avg_score_pct",
                    "backtest_hit_rate",
                    "risk_note",
                ]
            ]
            if not df.empty
            else df
        ),
        "",
    ]
    return "\n".join(lines)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD. 생략하면 KIS가 반환하는 최신 거래일 기준")
    parser.add_argument("--lookback-days", type=int, default=220)
    parser.add_argument("--delay", type=float, default=0.35)
    parser.add_argument("--universe-csv", type=Path, default=UNIVERSE_CSV, help="거래대금 상위 유니버스 CSV")
    parser.add_argument("--strategy-csv", type=Path, default=STRATEGY_CSV, help="감시 조건 CSV. 기본값은 active 전략 조건")
    parser.add_argument("--scan-csv", type=Path, default=SCAN_CSV, help="전체 스캔 결과 CSV")
    parser.add_argument("--watchlist-csv", type=Path, default=WATCHLIST_CSV, help="후보 결과 CSV")
    parser.add_argument("--watchlist-md", type=Path, default=WATCHLIST_MD, help="후보 결과 Markdown")
    parser.add_argument("--title", default="일별 전략 감시 후보", help="Markdown 제목")
    args = parser.parse_args()

    target_date = pd.Timestamp(args.date) if args.date else None
    strategies = pd.read_csv(args.strategy_csv, encoding="utf-8-sig")
    corp_map = await get_corp_code_map()
    companies = load_companies(args.universe_csv)

    scan_rows = []
    errors = []
    for ticker, name, corp_ticker in companies:
        result = await analyze_company(ticker, name, corp_ticker, corp_map, strategies, target_date, args.lookback_days)
        for row in result:
            if row.get("status") == "error":
                errors.append(row)
            else:
                scan_rows.append(row)
        await asyncio.sleep(args.delay)

    scan_df = pd.DataFrame(scan_rows)
    args.scan_csv.parent.mkdir(parents=True, exist_ok=True)
    args.watchlist_csv.parent.mkdir(parents=True, exist_ok=True)
    args.watchlist_md.parent.mkdir(parents=True, exist_ok=True)
    scan_df.to_csv(args.scan_csv, index=False, encoding="utf-8-sig")
    df = scan_df[scan_df["is_candidate"] == True].copy() if not scan_df.empty and "is_candidate" in scan_df.columns else pd.DataFrame()
    if not df.empty:
        df = df.sort_values(["priority", "signal_date", "ticker"]).reset_index(drop=True)
    df.to_csv(args.watchlist_csv, index=False, encoding="utf-8-sig")
    out_date = args.date or ("latest" if df.empty else str(df["signal_date"].max()))
    args.watchlist_md.write_text(build_markdown(df, out_date, args.title), encoding="utf-8")

    if errors:
        error_path = STRATEGY_DIR / "관심종목_시그널_오류.csv"
        pd.DataFrame(errors).to_csv(error_path, index=False, encoding="utf-8-sig")
        print(f"errors={len(errors)} error_csv={error_path}")
    print(f"universe_source={args.universe_csv if args.universe_csv.exists() else 'COMPANIES'}")
    print(f"strategy_source={args.strategy_csv}")
    print(f"universe_count={len(companies)}")
    print(f"candidates={len(df)}")
    print(f"scanned={len(scan_df)} scan_csv={args.scan_csv}")
    print(f"watchlist_csv={args.watchlist_csv}")
    print(f"watchlist_md={args.watchlist_md}")


if __name__ == "__main__":
    asyncio.run(main())
