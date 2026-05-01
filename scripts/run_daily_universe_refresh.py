from __future__ import annotations

import argparse
import asyncio
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from screener_lib.universe import get_stock_universe  # noqa: E402


BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
COMPANY_DIR = BASE_DIR / "00_기업별분석"
PLAN_DIR = BASE_DIR / "01_기획"
STRATEGY_DIR = BASE_DIR / "07_전략신호"
UNIVERSE_CSV = STRATEGY_DIR / "거래대금_상위_유니버스.csv"
UNIVERSE_MD = STRATEGY_DIR / "거래대금_상위_유니버스.md"
NEW_COMPANY_MD = PLAN_DIR / "신규_기업_추가_대상.md"


def is_excluded_stock(ticker: str, name: str) -> tuple[bool, str]:
    normalized = name.upper().replace(" ", "")
    if not ticker.isdigit() or len(ticker) != 6:
        return True, "invalid_ticker"
    if "스팩" in name or "SPAC" in normalized:
        return True, "spac"
    if "ETF" in normalized or "ETN" in normalized:
        return True, "etf_etn"
    if name.endswith("우") or re.search(r"우[AB]?$", name):
        return True, "preferred_stock"
    return False, ""


def safe_company_dir_name(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', "_", name).strip()


def format_amount(value: Any) -> str:
    try:
        return f"{int(float(value)):,}"
    except (TypeError, ValueError):
        return ""


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_종목 없음_"
    view = df.copy()
    for col in ("trade_amount", "market_cap", "price"):
        if col in view.columns:
            view[col] = view[col].map(format_amount)
    headers = [str(col) for col in view.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for _, row in view.iterrows():
        values = [str(row[col]).replace("|", "\\|") for col in view.columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def write_new_company_report(df: pd.DataFrame, created_count: int) -> None:
    new_df = df[(df["universe_status"] == "new") | (df["report_status"] == "report_needed")].copy()
    lines = [
        "# 신규 기업 추가 대상",
        "",
        f"- 데이터/보고서 준비 필요 기업 수: {len(new_df):,}",
        f"- 이번 실행에서 생성한 폴더 수: {created_count:,}",
        "",
        "## 준비 필요 기업",
        "",
        markdown_table(
            new_df[
                [
                    "rank",
                    "ticker",
                    "name",
                    "trade_amount",
                    "company_folder",
                    "report_status",
                ]
            ]
            if not new_df.empty
            else new_df
        ),
        "",
        "## 후속 작업",
        "",
        "- DART 기업코드 매핑 확인",
        "- 과거 OHLCV 캐시 생성",
        "- 분기별 원인 후보 분석 보고서 생성",
        "- 데이터 준비가 끝난 뒤 전략 신호 스캔 품질 확인",
        "",
    ]
    NEW_COMPANY_MD.write_text("\n".join(lines), encoding="utf-8")


async def main() -> None:
    parser = argparse.ArgumentParser(description="거래대금 상위 유니버스 갱신")
    parser.add_argument("--top", type=int, default=30, help="최종 유니버스 종목 수")
    parser.add_argument("--pool-size", type=int, default=120, help="필터링 전 조회 후보 수")
    parser.add_argument("--date", help="기준일 메모용 YYYY-MM-DD. 생략하면 KIS 최신 거래일")
    parser.add_argument("--no-create-folders", action="store_true", help="신규 기업 폴더를 만들지 않음")
    args = parser.parse_args()

    STRATEGY_DIR.mkdir(parents=True, exist_ok=True)
    PLAN_DIR.mkdir(parents=True, exist_ok=True)
    COMPANY_DIR.mkdir(parents=True, exist_ok=True)

    stocks = await get_stock_universe("amount")
    rows: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    created_count = 0
    seen: set[str] = set()

    for stock in stocks[: args.pool_size]:
        ticker = str(stock.get("ticker", "")).zfill(6)
        name = str(stock.get("name", "")).strip()
        excluded_flag, excluded_reason = is_excluded_stock(ticker, name)
        if excluded_flag:
            excluded.append({**stock, "ticker": ticker, "name": name, "excluded_reason": excluded_reason})
            continue
        if ticker in seen:
            continue
        seen.add(ticker)

        folder_name = safe_company_dir_name(name)
        folder_path = COMPANY_DIR / folder_name
        folder_exists_before = folder_path.exists()
        status = "existing" if folder_exists_before else "new"
        report_status = "exists" if folder_exists_before and any(folder_path.glob("*.md")) else "report_needed"

        if not folder_exists_before and not args.no_create_folders:
            folder_path.mkdir(parents=True, exist_ok=True)
            created_count += 1

        rows.append(
            {
                "basis_date": args.date or "latest",
                "rank": len(rows) + 1,
                "ticker": ticker,
                "name": name,
                "price": stock.get("price", 0),
                "change_rate": stock.get("change_rate", ""),
                "market_cap": stock.get("market_cap", 0),
                "trade_amount": stock.get("trade_amount", 0),
                "universe_status": status,
                "company_folder_exists": bool(folder_exists_before or folder_path.exists()),
                "company_folder": str(folder_path.relative_to(ROOT)),
                "report_status": report_status,
            }
        )
        if len(rows) >= args.top:
            break

    df = pd.DataFrame(rows)
    df.to_csv(UNIVERSE_CSV, index=False, encoding="utf-8-sig")

    lines = [
        "# 거래대금 상위 유니버스",
        "",
        f"- 기준일: {args.date or 'KIS 최신 거래일'}",
        f"- 최종 종목 수: {len(df):,}",
        f"- 신규 기업 수: {int((df['universe_status'] == 'new').sum()) if not df.empty else 0:,}",
        f"- 제외 종목 수: {len(excluded):,}",
        "",
        markdown_table(
            df[
                [
                    "rank",
                    "ticker",
                    "name",
                    "price",
                    "change_rate",
                    "trade_amount",
                    "universe_status",
                    "report_status",
                ]
            ]
            if not df.empty
            else df
        ),
        "",
    ]
    UNIVERSE_MD.write_text("\n".join(lines), encoding="utf-8")
    write_new_company_report(df, created_count)

    print(f"universe_count={len(df)}")
    print(f"new_companies={int((df['universe_status'] == 'new').sum()) if not df.empty else 0}")
    print(f"created_folders={created_count}")
    print(f"excluded={len(excluded)}")
    print(f"universe_csv={UNIVERSE_CSV}")
    print(f"universe_md={UNIVERSE_MD}")
    print(f"new_company_md={NEW_COMPANY_MD}")


def run_async_entrypoint() -> None:
    if sys.platform != "win32":
        asyncio.run(main())
        return

    policies = [
        asyncio.WindowsProactorEventLoopPolicy,
        asyncio.WindowsSelectorEventLoopPolicy,
    ]
    last_error: BaseException | None = None
    for policy_factory in policies:
        try:
            asyncio.set_event_loop_policy(policy_factory())
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(main())
                return
            finally:
                loop.close()
        except BaseException as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error


if __name__ == "__main__":
    run_async_entrypoint()
