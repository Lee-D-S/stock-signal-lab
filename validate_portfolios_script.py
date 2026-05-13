from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from core.api.client import get_marketdata  # noqa: E402


PORTFOLIO_DIR = ROOT / "data" / "portfolios"
TEST_INPUT_DIR = PORTFOLIO_DIR / "test_inputs"
REPORT_DIR = PORTFOLIO_DIR / "validation_reports"
PORTFOLIO_PRESETS = {
    "balanced": TEST_INPUT_DIR / "balanced.json",
    "cash-heavy": TEST_INPUT_DIR / "cash_heavy.json",
    "growth": TEST_INPUT_DIR / "growth.json",
    "risk-test": TEST_INPUT_DIR / "risk_test.json",
}

MOCK_FIELDS = ["cash", "positions[].ticker", "positions[].name", "positions[].quantity", "positions[].avg_price"]
LIVE_FIELDS = [
    "positions[].current_price",
    "positions[].valuation.per",
    "positions[].valuation.pbr",
    "positions[].valuation.eps",
    "positions[].sector",
    "positions[].trade_amount",
]
DERIVED_FIELDS = [
    "positions[].market_value",
    "positions[].invested_amount",
    "positions[].unrealized_pl",
    "positions[].return_pct",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="테스트 포트폴리오의 mock/live/derived 필드를 분리 검증합니다.")
    parser.add_argument("--preset", choices=sorted(PORTFOLIO_PRESETS), action="append")
    parser.add_argument("--write-report", action="store_true", help="검증 결과 Markdown/JSON 리포트를 저장합니다.")
    return parser.parse_args()


async def get_real_data(ticker: str) -> dict[str, Any]:
    try:
        price_data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
            tr_id="FHKST01010100",
        )
        info_data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/search-stock-info",
            params={"PRDT_TYPE_CD": "300", "PDNO": ticker},
            tr_id="CTPF1002R",
        )
        output = price_data.get("output") or {}
        info = info_data.get("output") or {}
        return {
            "ticker": ticker,
            "current_price": parse_float(output.get("stck_prpr")),
            "valuation": {
                "per": parse_float(output.get("per")),
                "pbr": parse_float(output.get("pbr")),
                "eps": parse_float(output.get("eps")),
                "bps": parse_float(output.get("bps")),
            },
            "sector": sector_from_stock_info(info),
            "raw_stock_info": info,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ticker": ticker, "error": f"{type(exc).__name__}: {exc}"}


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def sector_from_stock_info(info: dict[str, Any]) -> str:
    for key in ("idx_bztp_mcls_cd_name", "idx_bztp_lcls_cd_name", "std_idst_clsf_cd_name", "idx_bztp_scls_cd_name"):
        value = str(info.get(key, "") or "").strip()
        if value:
            return value
    return ""


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_mock_input(path: Path, data: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if "cash" not in data:
        issues.append("cash missing")
    if not isinstance(data.get("positions"), list):
        issues.append("positions missing or not list")
        return issues

    forbidden_live_keys = {"current_price", "valuation", "trade_amount", "market_value", "unrealized_pl", "return_pct"}
    for idx, pos in enumerate(data["positions"]):
        prefix = f"positions[{idx}]"
        for key in ("ticker", "name", "quantity", "avg_price"):
            if key not in pos:
                issues.append(f"{prefix}.{key} missing")
        for key in forbidden_live_keys:
            if key in pos:
                issues.append(f"{prefix}.{key} should not be in mock input")
    return issues


def validate_snapshot(snapshot: dict[str, Any], live_by_ticker: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pos in snapshot.get("positions", []):
        ticker = str(pos.get("ticker", "")).zfill(6)
        live = live_by_ticker.get(ticker, {})
        if live.get("error"):
            rows.append({"ticker": ticker, "name": pos.get("name", ""), "status": "error", "issue": live["error"]})
            continue
        rows.extend(compare_live_fields(pos, live))
        rows.extend(compare_derived_fields(pos))
    return rows


def compare_live_fields(pos: dict[str, Any], live: dict[str, Any]) -> list[dict[str, Any]]:
    ticker = str(pos.get("ticker", "")).zfill(6)
    name = pos.get("name", "")
    checks = [
        ("current_price", pos.get("current_price"), live.get("current_price"), 1.0),
        ("valuation.per", (pos.get("valuation") or {}).get("per"), (live.get("valuation") or {}).get("per"), 0.02),
        ("valuation.pbr", (pos.get("valuation") or {}).get("pbr"), (live.get("valuation") or {}).get("pbr"), 0.02),
        ("valuation.eps", (pos.get("valuation") or {}).get("eps"), (live.get("valuation") or {}).get("eps"), 1.0),
    ]
    rows = []
    for field, saved, actual, tolerance in checks:
        saved_num = parse_float(saved)
        actual_num = parse_float(actual)
        status = "match" if saved_num is not None and actual_num is not None and abs(saved_num - actual_num) <= tolerance else "mismatch"
        rows.append({"ticker": ticker, "name": name, "field": field, "saved": saved, "actual": actual, "status": status})

    saved_sector = str(pos.get("sector", "") or "")
    actual_sector = str(live.get("sector", "") or "")
    rows.append({
        "ticker": ticker,
        "name": name,
        "field": "sector",
        "saved": saved_sector,
        "actual": actual_sector,
        "status": "match" if saved_sector == actual_sector else "mismatch",
    })
    return rows


def compare_derived_fields(pos: dict[str, Any]) -> list[dict[str, Any]]:
    ticker = str(pos.get("ticker", "")).zfill(6)
    name = pos.get("name", "")
    qty = parse_float(pos.get("quantity")) or 0
    avg = parse_float(pos.get("avg_price")) or 0
    price = parse_float(pos.get("current_price")) or 0
    invested = qty * avg
    market_value = qty * price
    unrealized = market_value - invested
    return_pct = unrealized / invested if invested > 0 else None
    expected = {
        "market_value": market_value,
        "invested_amount": invested,
        "unrealized_pl": unrealized,
        "return_pct": return_pct,
    }
    rows = []
    for field, actual in expected.items():
        saved = parse_float(pos.get(field))
        tolerance = 0.000001 if field == "return_pct" else 1.0
        status = "match" if saved is not None and actual is not None and abs(saved - actual) <= tolerance else "mismatch"
        rows.append({"ticker": ticker, "name": name, "field": field, "saved": pos.get(field), "actual": actual, "status": status})
    return rows


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# 테스트 포트폴리오 검증 리포트",
        "",
        f"- 생성시각: {report['generated_at']}",
        f"- preset: {', '.join(report['presets'])}",
        "",
        "## 필드 구분",
        "",
        f"- mock_fields: {', '.join(MOCK_FIELDS)}",
        f"- live_fields: {', '.join(LIVE_FIELDS)}",
        f"- derived_fields: {', '.join(DERIVED_FIELDS)}",
        "",
    ]
    for item in report["results"]:
        lines.extend([
            f"## {item['preset']}",
            "",
            f"- mock_input: {item['mock_input']}",
            f"- snapshot: {item.get('snapshot', '(없음)')}",
            f"- mock_issues: {len(item['mock_issues'])}",
            f"- live/derived checks: {len(item['checks'])}",
            "",
        ])
        if item["mock_issues"]:
            lines.extend(f"- {issue}" for issue in item["mock_issues"])
            lines.append("")
        if item["checks"]:
            lines.extend([
                "| ticker | name | field | saved | actual | status |",
                "|---|---|---|---:|---:|---|",
            ])
            for row in item["checks"]:
                lines.append(
                    f"| {row.get('ticker', '')} | {row.get('name', '')} | {row.get('field', row.get('issue', ''))} | "
                    f"{row.get('saved', '')} | {row.get('actual', '')} | {row.get('status', '')} |"
                )
            lines.append("")
    return "\n".join(lines)


async def main() -> None:
    args = parse_args()
    presets = args.preset or list(PORTFOLIO_PRESETS)
    tickers: set[str] = set()
    mock_inputs = {}
    for preset in presets:
        path = PORTFOLIO_PRESETS[preset]
        data = load_json(path)
        mock_inputs[preset] = (path, data)
        for pos in data.get("positions", []):
            tickers.add(str(pos.get("ticker", "")).zfill(6))

    print(f"Fetching live data for {len(tickers)} tickers...")
    live_by_ticker = {}
    for ticker in sorted(tickers):
        live_by_ticker[ticker] = await get_real_data(ticker)
        await asyncio.sleep(0.25)

    report = {"generated_at": datetime.now().isoformat(timespec="seconds"), "presets": presets, "results": []}
    for preset in presets:
        input_path, mock_data = mock_inputs[preset]
        snapshot_path = PORTFOLIO_DIR / f"test_portfolio_{preset.replace('-', '_')}.json"
        result = {
            "preset": preset,
            "mock_input": str(input_path),
            "mock_issues": validate_mock_input(input_path, mock_data),
            "checks": [],
        }
        if snapshot_path.exists():
            result["snapshot"] = str(snapshot_path)
            result["checks"] = validate_snapshot(load_json(snapshot_path), live_by_ticker)
        report["results"].append(result)

    print(render_markdown(report))
    if args.write_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = REPORT_DIR / f"test_portfolio_validation_{stamp}.json"
        md_path = REPORT_DIR / f"test_portfolio_validation_{stamp}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(render_markdown(report), encoding="utf-8")
        print(f"report_json={json_path}")
        print(f"report_md={md_path}")


if __name__ == "__main__":
    asyncio.run(main())
