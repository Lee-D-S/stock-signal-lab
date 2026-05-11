from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv

load_dotenv()

from core.api.client import get_marketdata  # noqa: E402
from core.market_data import get_current_price  # noqa: E402
from screener_lib.data import get_kis_valuation, get_ohlcv  # noqa: E402


DEFAULT_INPUT = ROOT / "data" / "manual_portfolio_input.json"
DEFAULT_OUTPUT = ROOT / "data" / "portfolios" / "my_real_portfolio.json"

KNOWN_TICKERS = {
    "한화오션": "042660",
    "에코프로": "086520",
    "뷰노": "338220",
    "두산로보틱스": "454910",
}

DEFAULT_SECTORS = {
    "042660": "조선",
    "086520": "2차전지",
    "338220": "의료AI",
    "454910": "로봇",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="수동 입력 보유내역과 KIS 시장 데이터를 바탕으로 data/portfolios/my_real_portfolio.json을 생성합니다."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    raw = json.loads(args.input.read_text(encoding="utf-8"))
    holdings = raw.get("positions", [])
    positions: list[dict[str, Any]] = []
    warnings: list[str] = []

    for item in holdings:
        name = str(item.get("name", "")).strip()
        ticker = str(item.get("ticker") or KNOWN_TICKERS.get(name, "")).strip()
        if not ticker:
            warnings.append(f"{name or '<unknown>'}: 종목코드가 없습니다.")
            continue

        quantity = int(item.get("quantity", 0) or 0)
        avg_price = float(item.get("avg_price", 0) or 0)
        price_info: dict[str, Any] = {}
        valuation: dict[str, Any] | None = None
        stock_info: dict[str, Any] = {}
        trade_amount = 0

        try:
            price_info = await get_current_price(ticker)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"{ticker}: KIS 현재가 조회 실패: {type(exc).__name__}: {exc}")

        try:
            valuation = await get_kis_valuation(ticker)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"{ticker}: KIS 밸류에이션 조회 실패: {type(exc).__name__}: {exc}")

        try:
            stock_info = await get_stock_info(ticker)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"{ticker}: KIS 종목 정보 조회 실패: {type(exc).__name__}: {exc}")

        try:
            _, trade_amount = await get_ohlcv(ticker)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"{ticker}: KIS OHLCV 조회 실패: {type(exc).__name__}: {exc}")

        current_price = float(price_info.get("price") or item.get("current_price") or 0)
        market_value = quantity * current_price if current_price > 0 else 0
        invested_amount = quantity * avg_price
        unrealized_pl = market_value - invested_amount if market_value > 0 else None
        return_pct = (
            unrealized_pl / invested_amount
            if unrealized_pl is not None and invested_amount > 0
            else None
        )

        sector = sector_from_kis_stock_info(stock_info) or item.get("sector") or DEFAULT_SECTORS.get(ticker, "")

        positions.append(
            {
                "ticker": ticker,
                "name": name or price_info.get("name") or ticker,
                "quantity": quantity,
                "avg_price": avg_price,
                "current_price": current_price,
                "sector": sector,
                "sector_source": "kis_search_stock_info" if sector_from_kis_stock_info(stock_info) else "manual_fallback",
                "kis_stock_info": {
                    "market": stock_info.get("mket_id_cd", ""),
                    "security_group": stock_info.get("scty_grp_id_cd", ""),
                    "index_large": stock_info.get("idx_bztp_lcls_cd_name", ""),
                    "index_middle": stock_info.get("idx_bztp_mcls_cd_name", ""),
                    "index_small": stock_info.get("idx_bztp_scls_cd_name", ""),
                    "standard_industry": stock_info.get("std_idst_clsf_cd_name", ""),
                },
                "market_value": market_value,
                "invested_amount": invested_amount,
                "unrealized_pl": unrealized_pl,
                "return_pct": return_pct,
                "trade_amount": trade_amount,
                "valuation": valuation or {},
                "price_source": "kis_inquire_price" if price_info.get("price") else "manual_or_missing",
            }
        )

    snapshot = {
        "cash": float(raw.get("cash", 0) or 0),
        "positions": positions,
        "metadata": {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "input_file": str(args.input),
            "market_data_source": "KIS",
            "account_source": raw.get("account_source", "manual"),
            "warnings": warnings,
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"작성 완료: {args.output}")
    print(f"보유 종목 수={len(positions)}, 현금={snapshot['cash']:,.0f}")
    if warnings:
        print("경고:")
        for warning in warnings:
            print(f"- {warning}")


async def get_stock_info(ticker: str) -> dict[str, Any]:
    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/search-stock-info",
        params={"PRDT_TYPE_CD": "300", "PDNO": ticker},
        tr_id="CTPF1002R",
    )
    output = data.get("output") or {}
    if isinstance(output, list):
        return output[0] if output else {}
    return output


def sector_from_kis_stock_info(stock_info: dict[str, Any]) -> str:
    for key in (
        "idx_bztp_mcls_cd_name",
        "idx_bztp_lcls_cd_name",
        "std_idst_clsf_cd_name",
        "idx_bztp_scls_cd_name",
    ):
        value = str(stock_info.get(key, "") or "").strip()
        if value:
            return value
    return ""


if __name__ == "__main__":
    asyncio.run(main())
