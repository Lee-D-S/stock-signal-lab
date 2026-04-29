from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp
import httpx
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import settings
from core.api.auth import get_real_access_token
from core.api.client import get_marketdata
from scripts.screener_lib.dart import DART_API_KEY, DART_BASE_URL, get_corp_code_map


TICKER = "034020"
NAME = "두산에너빌리티"
END = "2026-04-28"
MAIN_START = "2025-10-28"
BASE_START = "2025-04-28"
OUT = ROOT / "ai 주가 변동 원인 분석" / "두산에너빌리티_최근6개월_원인후보_실제분석.md"


def fmt_won(n: float | int | None) -> str:
    if n is None or pd.isna(n):
        return "N/A"
    n = float(n)
    eok = n / 100_000_000
    if abs(eok) >= 10_000:
        return f"{eok / 10_000:.2f}조"
    return f"{eok:,.0f}억"


def fmt_int(n: float | int | None) -> str:
    if n is None or pd.isna(n):
        return "N/A"
    return f"{int(round(float(n))):,}"


def fmt_pct(n: float | int | None) -> str:
    if n is None or pd.isna(n):
        return "N/A"
    return f"{float(n):+.2f}%"


async def kis_get(path: str, params: dict[str, Any], tr_id: str) -> dict[str, Any]:
    token = await get_real_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "appkey": settings.kis_real_app_key,
        "appsecret": settings.kis_real_app_secret,
        "tr_id": tr_id,
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=20) as http:
        resp = await http.get(
            f"https://openapi.koreainvestment.com:9443{path}",
            headers=headers,
            params=params,
        )
    resp.raise_for_status()
    data = resp.json()
    if data.get("rt_cd", "0") != "0":
        raise RuntimeError(f"KIS API error {tr_id}: {data.get('msg1')}")
    return data


async def fetch_ohlcv(start: str, end: str) -> pd.DataFrame:
    async def fetch_chunk(s: pd.Timestamp, e: pd.Timestamp) -> list[dict[str, Any]]:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": TICKER,
                "FID_INPUT_DATE_1": s.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": e.strftime("%Y%m%d"),
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
            tr_id="FHKST03010100",
        )
        return data.get("output2", [])

    rows: list[dict[str, Any]] = []
    cur = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    while cur <= end_ts:
        chunk_end = min(cur + pd.Timedelta(days=80), end_ts)
        rows.extend(await fetch_chunk(cur, chunk_end))
        cur = chunk_end + pd.Timedelta(days=1)
        await asyncio.sleep(0.35)
    records = []
    for r in rows:
        if not r.get("stck_bsop_date"):
            continue
        close = float(r.get("stck_clpr") or 0)
        records.append(
            {
                "date": pd.to_datetime(r.get("stck_bsop_date")),
                "open": float(r.get("stck_oprc") or 0),
                "high": float(r.get("stck_hgpr") or 0),
                "low": float(r.get("stck_lwpr") or 0),
                "close": close,
                "volume": float(r.get("acml_vol") or 0),
                "trade_amount": float(r.get("acml_tr_pbmn") or 0),
                "api_chg_pct": pd.to_numeric(r.get("prdy_ctrt"), errors="coerce"),
            }
        )
    df = pd.DataFrame(records)
    if df.empty:
        return df
    df = df.drop_duplicates("date").sort_values("date").reset_index(drop=True)
    df["chg_pct"] = df["close"].pct_change() * 100
    df["trade_amount_ma20"] = df["trade_amount"].rolling(20, min_periods=5).mean()
    df["amount_vs_ma20"] = df["trade_amount"] / df["trade_amount_ma20"]
    return df


def parse_investor_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    out = []
    for r in rows:
        date = r.get("stck_bsop_date") or r.get("bsop_date") or r.get("trad_dt")
        if not date:
            continue
        out.append(
            {
                "date": pd.to_datetime(str(date), format="%Y%m%d", errors="coerce"),
                "foreign_qty": pd.to_numeric(r.get("frgn_ntby_qty"), errors="coerce"),
                "institution_qty": pd.to_numeric(
                    r.get("orgn_ntby_qty") or r.get("inst_ntby_qty"),
                    errors="coerce",
                ),
                "individual_qty": pd.to_numeric(
                    r.get("prsn_ntby_qty") or r.get("indv_ntby_qty"),
                    errors="coerce",
                ),
            }
        )
    df = pd.DataFrame(out)
    if df.empty:
        return df
    return df.dropna(subset=["date"]).drop_duplicates("date").sort_values("date").reset_index(drop=True)


async def fetch_investor_daily_chunk(end_yyyymmdd: str) -> pd.DataFrame:
    data = await kis_get(
        "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": TICKER,
            "FID_INPUT_DATE_1": end_yyyymmdd,
            "FID_ORG_ADJ_PRC": "",
            "FID_ETC_CLS_CODE": "",
        },
        "FHPTJ04160001",
    )
    rows = data.get("output2") or data.get("output1") or data.get("output") or []
    if isinstance(rows, dict):
        rows = [rows]
    return parse_investor_rows(rows)


async def fetch_investor_range(start: str, end: str) -> pd.DataFrame:
    start_ts = pd.Timestamp(start)
    cursor = pd.Timestamp(end)
    frames = []
    for _ in range(8):
        chunk = await fetch_investor_daily_chunk(cursor.strftime("%Y%m%d"))
        if chunk.empty:
            break
        frames.append(chunk)
        min_date = chunk["date"].min()
        if min_date <= start_ts:
            break
        cursor = min_date - pd.Timedelta(days=1)
        await asyncio.sleep(0.35)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames).drop_duplicates("date").sort_values("date").reset_index(drop=True)
    return df[(df["date"] >= start_ts) & (df["date"] <= pd.Timestamp(end))].reset_index(drop=True)


async def fetch_investor_current() -> pd.DataFrame:
    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/inquire-investor",
        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": TICKER},
        tr_id="FHKST01010900",
    )
    return parse_investor_rows(data.get("output", []) or [])


async def fetch_price_snapshot() -> dict[str, Any]:
    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": TICKER},
        tr_id="FHKST01010100",
    )
    return data.get("output", {}) or {}


async def fetch_short_sale(start: str, end: str) -> pd.DataFrame:
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/daily-short-sale",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": TICKER,
                "FID_INPUT_DATE_1": start.replace("-", ""),
                "FID_INPUT_DATE_2": end.replace("-", ""),
            },
            tr_id="FHPST04830000",
        )
    except Exception:
        return pd.DataFrame()
    records = []
    for r in data.get("output2", []) or []:
        date = r.get("stck_bsop_date") or r.get("bsop_date")
        if not date:
            continue
        records.append(
            {
                "date": pd.to_datetime(str(date), format="%Y%m%d", errors="coerce"),
                "short_qty": pd.to_numeric(r.get("ssts_cntg_qty"), errors="coerce"),
                "short_amount": pd.to_numeric(r.get("ssts_tr_pbmn"), errors="coerce"),
                "short_ratio": pd.to_numeric(r.get("ssts_tr_pbmn_rate"), errors="coerce"),
            }
        )
    return pd.DataFrame(records).dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


async def dart_get(endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
    if not DART_API_KEY:
        return {"status": "NO_KEY"}
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{DART_BASE_URL}/{endpoint}",
            params={"crtfc_key": DART_API_KEY, **params},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            return await resp.json(content_type=None)


async def fetch_dart_disclosures(corp_code: str, start: str, end: str) -> list[dict[str, Any]]:
    data = await dart_get(
        "list.json",
        {
            "corp_code": corp_code,
            "bgn_de": start.replace("-", ""),
            "end_de": end.replace("-", ""),
            "page_count": 100,
        },
    )
    if data.get("status") != "000":
        return []
    return data.get("list", [])


def parse_financial_list(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    chosen_fs = ""
    for row in rows:
        account = (row.get("account_nm") or "").strip()
        field = None
        if "\ub9e4\ucd9c\uc561" in account or "\uc218\uc775" in account:
            field = "revenue"
        elif "\uc601\uc5c5\uc774\uc775" in account:
            field = "op_income"
        elif "\ub2f9\uae30\uc21c\uc774\uc775" in account:
            field = "net_income"
        elif account == "\uc790\uc0b0\ucd1d\uacc4":
            field = "total_assets"
        elif account == "\ubd80\ucc44\ucd1d\uacc4":
            field = "total_debt"
        elif account == "\uc790\ubcf8\ucd1d\uacc4":
            field = "equity"
        if not field:
            continue
        fs_div = row.get("fs_div", "")
        if fs_div == "OFS" and chosen_fs == "CFS":
            continue
        val = str(row.get("thstrm_amount") or "").replace(",", "")
        try:
            out[field] = int(val)
            chosen_fs = fs_div or chosen_fs
        except ValueError:
            pass
    rev, op, net = out.get("revenue"), out.get("op_income"), out.get("net_income")
    debt, equity, assets = out.get("total_debt"), out.get("equity"), out.get("total_assets")
    if rev and op:
        out["op_margin"] = op / rev * 100
    if rev and net:
        out["net_margin"] = net / rev * 100
    if equity and debt:
        out["debt_ratio"] = debt / equity * 100
    if equity and net:
        out["roe"] = net / equity * 100
    if assets and net:
        out["roa"] = net / assets * 100
    return out


async def fetch_financials(corp_code: str) -> dict[str, dict[str, Any]]:
    labels = {
        ("2024", "11011"): "2024 사업보고서",
        ("2025", "11014"): "2025 3분기보고서",
        ("2025", "11011"): "2025 사업보고서",
        ("2026", "11013"): "2026 1분기보고서",
    }
    out = {}
    for (year, code), label in labels.items():
        data = await dart_get(
            "fnlttMultiAcnt.json",
            {"corp_code": corp_code, "bsns_year": year, "reprt_code": code},
        )
        if data.get("status") == "000":
            parsed = parse_financial_list(data.get("list", []))
            if parsed:
                out[label] = parsed
        await asyncio.sleep(0.25)
    return out


async def fetch_dart_structured(corp_code: str, start: str, end: str) -> dict[str, list[dict[str, Any]]]:
    endpoints = {
        "자기주식취득결정": "tsstkAqDecsn.json",
        "대량보유상황보고": "majorstock.json",
        "단일판매공급계약": "singleSellContract.json",
    }
    out: dict[str, list[dict[str, Any]]] = {}
    for label, endpoint in endpoints.items():
        data = await dart_get(
            endpoint,
            {
                "corp_code": corp_code,
                "bgn_de": start.replace("-", ""),
                "end_de": end.replace("-", ""),
            },
        )
        if data.get("status") == "000":
            filtered = []
            for r in data.get("list", []):
                raw_dt = r.get("rcept_dt") or r.get("rcept_de") or r.get("cntrct_cncls_de") or ""
                dt = pd.to_datetime(str(raw_dt).replace("-", ""), format="%Y%m%d", errors="coerce")
                if pd.isna(dt) or (pd.Timestamp(start) <= dt <= pd.Timestamp(end)):
                    filtered.append(r)
            out[label] = filtered
        await asyncio.sleep(0.25)
    return out


def top_events(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    main = df[(df["date"] >= pd.Timestamp(MAIN_START)) & (df["date"] <= pd.Timestamp(END))].copy()
    up = main[main["chg_pct"] > 0].sort_values(["chg_pct", "trade_amount"], ascending=[False, False]).head(8)
    down = main[main["chg_pct"] < 0].sort_values(["chg_pct", "trade_amount"], ascending=[True, False]).head(8)
    return up, down


def event_context(row: pd.Series, investor: pd.DataFrame, disclosures: list[dict[str, Any]]) -> dict[str, Any]:
    d = row["date"]
    win = investor[(investor["date"] >= d - pd.Timedelta(days=5)) & (investor["date"] <= d + pd.Timedelta(days=5))]
    near = []
    for disc in disclosures:
        dt = pd.to_datetime(disc.get("rcept_dt"), format="%Y%m%d", errors="coerce")
        if pd.notna(dt) and abs((dt - d).days) <= 5:
            near.append(f"{dt.strftime('%Y-%m-%d')} {disc.get('report_nm')}")
    return {
        "foreign_11d": None if win.empty else win["foreign_qty"].sum(min_count=1),
        "institution_11d": None if win.empty else win["institution_qty"].sum(min_count=1),
        "individual_11d": None if win.empty else win["individual_qty"].sum(min_count=1),
        "disclosures": near,
    }


def rank_reasons(
    ohlcv: pd.DataFrame,
    investor: pd.DataFrame,
    disclosures: list[dict[str, Any]],
    financials: dict[str, dict[str, Any]],
    structured: dict[str, list[dict[str, Any]]],
) -> tuple[list[str], list[str]]:
    main = ohlcv[(ohlcv["date"] >= pd.Timestamp(MAIN_START)) & (ohlcv["date"] <= pd.Timestamp(END))]
    inv_main = investor[(investor["date"] >= pd.Timestamp(MAIN_START)) & (investor["date"] <= pd.Timestamp(END))]
    foreign_sum = inv_main["foreign_qty"].sum(min_count=1) if not inv_main.empty else None
    inst_sum = inv_main["institution_qty"].sum(min_count=1) if not inv_main.empty else None
    indiv_sum = inv_main["individual_qty"].sum(min_count=1) if not inv_main.empty else None
    avg_amount = main["trade_amount"].mean()
    max_amount = main["trade_amount"].max()
    latest_fin = next((v for k, v in financials.items() if "2025 사업" in k), None) or next(iter(financials.values()), {})
    op_margin = latest_fin.get("op_margin")
    debt_ratio = latest_fin.get("debt_ratio")
    net_margin = latest_fin.get("net_margin")
    up = [
        f"1. 외국인/기관 수급 유입: 최근 6개월 조회 가능 구간 누적 외국인 {fmt_int(foreign_sum)}주, 기관 {fmt_int(inst_sum)}주, 개인 {fmt_int(indiv_sum)}주. 상승일 전후 수급이 같은 방향이면 가장 높은 우선순위.",
        f"2. 고거래대금 동반 가격 재평가: 최근 6개월 평균 거래대금 {fmt_won(avg_amount)}, 최대 거래대금 {fmt_won(max_amount)}. 평균 대비 2배 이상 거래대금이 붙은 상승일은 단순 저유동성 반등보다 설명력이 높음.",
        "3. DART 이벤트: 자기주식 취득 결정, 단일판매ㆍ공급계약, IR/실적 관련 공시가 급등락일 전후 5거래일 안에 있으면 보조 원인 후보.",
        f"4. 재무 체력 확인: 최신 DART 주요계정 기준 매출 {fmt_won(latest_fin.get('revenue'))}, 영업이익 {fmt_won(latest_fin.get('op_income'))}, 영업이익률 {op_margin:.2f}%로 흑자 체력은 확인되지만, 단독 급등 원인으로는 약함." if op_margin is not None else f"4. 재무 체력 확인: 최신 DART 주요계정 기준 매출 {fmt_won(latest_fin.get('revenue'))}, 영업이익 {fmt_won(latest_fin.get('op_income'))}. 단독 급등 원인으로는 약함.",
    ]
    down = [
        "1. 이벤트/수급 급등 후 차익실현: 거래대금이 큰 상승 직후 하락하고 외국인/기관이 순매도로 돌아선 날은 악재보다 매물 출회 후보가 우선.",
        "2. 외국인/기관 동반 순매도: 하락일 전후 5거래일 외국인과 기관 합산이 음수이면 수급성 하락 후보.",
        f"3. 높은 변동성 민감도: 최근 6개월 최대 거래대금이 {fmt_won(max_amount)}까지 확대되어, 수급 이탈 시 낙폭도 커질 수 있음.",
        f"4. 재무 부담 요인: 최신 DART 기준 부채비율 {debt_ratio:.2f}%, 순이익률 {net_margin:.2f}%는 강한 주가 상승을 펀더멘털만으로 설명하기 어렵게 만드는 보조 하락/조정 후보." if debt_ratio is not None and net_margin is not None else "4. 재무 부담 요인: 부채비율 또는 순이익률 수치가 제한적이면, 재무 수치만으로 강한 주가 상승을 설명하기 어렵다는 보조 후보로만 둠.",
    ]
    if structured.get("자기주식취득결정"):
        up.insert(0, "0. 구조화 DART 자기주식 취득 결정 데이터가 확인됨. 공시일 주변 상승은 자사주 매입 기대가 최상위 후보.")
    return up, down


def make_markdown(
    ohlcv: pd.DataFrame,
    investor: pd.DataFrame,
    current_inv: pd.DataFrame,
    short_df: pd.DataFrame,
    snapshot: dict[str, Any],
    disclosures: list[dict[str, Any]],
    financials: dict[str, dict[str, Any]],
    structured: dict[str, list[dict[str, Any]]],
) -> str:
    main = ohlcv[(ohlcv["date"] >= pd.Timestamp(MAIN_START)) & (ohlcv["date"] <= pd.Timestamp(END))].copy()
    start_row, end_row = main.iloc[0], main.iloc[-1]
    ret = (end_row["close"] / start_row["close"] - 1) * 100
    high = main.loc[main["close"].idxmax()]
    low = main.loc[main["close"].idxmin()]
    up, down = top_events(ohlcv)
    up_reasons, down_reasons = rank_reasons(ohlcv, investor, disclosures, financials, structured)

    lines = [
        f"# {NAME}(034020) 최근 6개월 주가 변동 원인 후보 분석",
        "",
        "## 분석 전제",
        "",
        f"- 작성일: 2026-04-28",
        f"- 메인 기간: {MAIN_START}~{END}",
        f"- 기준선: {BASE_START}~{END}",
        f"- KIS 일봉 실제 수집 범위: {ohlcv['date'].min().strftime('%Y-%m-%d')}~{ohlcv['date'].max().strftime('%Y-%m-%d')} ({len(ohlcv)}거래일)",
        "- 외부 뉴스, 정책, 금리, 환율, 테마 요인은 제외",
        "- 결론은 확정 원인이 아니라 DART/KIS 수치로 설명 가능한 상승/하락 원인 후보",
        "",
        "## 사용 데이터",
        "",
        "- KIS MCP 확인 API: 국내주식기간별시세 `inquire_daily_itemchartprice`, 주식현재가 투자자 `inquire_investor`, 주식현재가 시세 `inquire_price`",
        "- KIS 직접 조회: 일봉 OHLCV/거래대금, 투자자 일별 순매수, 현재 PER/PBR/EPS/BPS, 공매도 일별 추이",
        "- DART OpenAPI: 공시검색 `list`, 주요계정 `fnlttMultiAcnt`, 자기주식취득결정/대량보유/단일판매공급계약 구조화 API 시도",
        "",
        "## 기간 주가 요약",
        "",
        "| 항목 | 수치 |",
        "|---|---:|",
        f"| 시작 종가 | {fmt_int(start_row['close'])}원 ({start_row['date'].strftime('%Y-%m-%d')}) |",
        f"| 종료 종가 | {fmt_int(end_row['close'])}원 ({end_row['date'].strftime('%Y-%m-%d')}) |",
        f"| 기간 수익률 | {ret:+.2f}% |",
        f"| 기간 고점 | {fmt_int(high['close'])}원 ({high['date'].strftime('%Y-%m-%d')}) |",
        f"| 기간 저점 | {fmt_int(low['close'])}원 ({low['date'].strftime('%Y-%m-%d')}) |",
        f"| 평균 거래대금 | {fmt_won(main['trade_amount'].mean())} |",
        f"| 최대 거래대금 | {fmt_won(main['trade_amount'].max())} |",
        "",
        "## 주요 상승일",
        "",
        "| 날짜 | 종가 | 등락률 | 거래대금 | 전후 5거래일 외국인 | 전후 5거래일 기관 | 주변 DART 공시 |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for _, r in up.iterrows():
        ctx = event_context(r, investor, disclosures)
        disc = "<br>".join(ctx["disclosures"][:3]) if ctx["disclosures"] else "-"
        lines.append(
            f"| {r['date'].strftime('%Y-%m-%d')} | {fmt_int(r['close'])}원 | {fmt_pct(r['chg_pct'])} | {fmt_won(r['trade_amount'])} | {fmt_int(ctx['foreign_11d'])}주 | {fmt_int(ctx['institution_11d'])}주 | {disc} |"
        )
    lines.extend([
        "",
        "## 주요 하락일",
        "",
        "| 날짜 | 종가 | 등락률 | 거래대금 | 전후 5거래일 외국인 | 전후 5거래일 기관 | 주변 DART 공시 |",
        "|---|---:|---:|---:|---:|---:|---|",
    ])
    for _, r in down.iterrows():
        ctx = event_context(r, investor, disclosures)
        disc = "<br>".join(ctx["disclosures"][:3]) if ctx["disclosures"] else "-"
        lines.append(
            f"| {r['date'].strftime('%Y-%m-%d')} | {fmt_int(r['close'])}원 | {fmt_pct(r['chg_pct'])} | {fmt_won(r['trade_amount'])} | {fmt_int(ctx['foreign_11d'])}주 | {fmt_int(ctx['institution_11d'])}주 | {disc} |"
        )

    inv_main = investor[(investor["date"] >= pd.Timestamp(MAIN_START)) & (investor["date"] <= pd.Timestamp(END))]
    if inv_main.empty and not current_inv.empty:
        inv_main = current_inv
    lines.extend([
        "",
        "## KIS 수급 요약",
        "",
        "| 구분 | 누적 순매수 |",
        "|---|---:|",
        f"| 외국인 | {fmt_int(inv_main['foreign_qty'].sum(min_count=1) if not inv_main.empty else None)}주 |",
        f"| 기관 | {fmt_int(inv_main['institution_qty'].sum(min_count=1) if not inv_main.empty else None)}주 |",
        f"| 개인 | {fmt_int(inv_main['individual_qty'].sum(min_count=1) if not inv_main.empty else None)}주 |",
        "",
    ])

    lines.extend([
        "## KIS 현재 밸류에이션/가격 지표",
        "",
        "| 항목 | 수치 |",
        "|---|---:|",
        f"| 현재가 | {fmt_int(pd.to_numeric(snapshot.get('stck_prpr'), errors='coerce'))}원 |",
        f"| 시가총액 | {fmt_won(pd.to_numeric(snapshot.get('hts_avls'), errors='coerce') * 100_000_000 if snapshot.get('hts_avls') else None)} |",
        f"| PER | {snapshot.get('per') or snapshot.get('hts_per') or 'N/A'} |",
        f"| PBR | {snapshot.get('pbr') or 'N/A'} |",
        f"| EPS | {snapshot.get('eps') or 'N/A'} |",
        f"| BPS | {snapshot.get('bps') or 'N/A'} |",
        f"| 52주 최고가 | {fmt_int(pd.to_numeric(snapshot.get('w52_hgpr'), errors='coerce'))}원 |",
        f"| 52주 최저가 | {fmt_int(pd.to_numeric(snapshot.get('w52_lwpr'), errors='coerce'))}원 |",
        "",
    ])
    if not short_df.empty:
        ss = short_df[(short_df["date"] >= pd.Timestamp(MAIN_START)) & (short_df["date"] <= pd.Timestamp(END))]
        if not ss.empty:
            lines.extend([
                "## KIS 공매도 요약",
                "",
                "| 항목 | 수치 |",
                "|---|---:|",
                f"| 공매도 거래대금 합계 | {fmt_won(ss['short_amount'].sum(min_count=1))} |",
                f"| 일평균 공매도 거래대금 | {fmt_won(ss['short_amount'].mean())} |",
                f"| 최대 공매도 거래대금 | {fmt_won(ss['short_amount'].max())} |",
                "",
            ])

    lines.extend([
        "## DART 공시 요약",
        "",
        "| 날짜 | 공시명 |",
        "|---|---|",
    ])
    for d in disclosures[:30]:
        dt = pd.to_datetime(d.get("rcept_dt"), format="%Y%m%d", errors="coerce")
        lines.append(f"| {dt.strftime('%Y-%m-%d') if pd.notna(dt) else d.get('rcept_dt')} | {d.get('report_nm')} |")

    lines.extend(["", "## DART 주요 재무 수치", "", "| 기준 | 매출액 | 영업이익 | 순이익 | 영업이익률 | 부채비율 | ROE |", "|---|---:|---:|---:|---:|---:|---:|"])
    for label, f in financials.items():
        op_margin = f"{f['op_margin']:.2f}%" if f.get("op_margin") is not None else "N/A"
        debt_ratio = f"{f['debt_ratio']:.2f}%" if f.get("debt_ratio") is not None else "N/A"
        roe = f"{f['roe']:.2f}%" if f.get("roe") is not None else "N/A"
        lines.append(
            f"| {label} | {fmt_won(f.get('revenue'))} | {fmt_won(f.get('op_income'))} | {fmt_won(f.get('net_income'))} | {op_margin} | {debt_ratio} | {roe} |"
        )

    lines.extend(["", "## DART 구조화 이벤트 조회 결과", ""])
    for label, rows in structured.items():
        lines.append(f"- {label}: {len(rows)}건")
        for r in rows[:3]:
            summary_keys = ["rcept_no", "rcept_dt", "aqpln_stk_ostk", "aqpln_prc", "aqexpd_bgd", "aqexpd_edd", "cntrct_cncls_de", "cntrct_amount", "hd_stock_qota_rt"]
            vals = [f"{k}={r.get(k)}" for k in summary_keys if r.get(k)]
            if vals:
                lines.append(f"  - {'; '.join(vals)}")

    lines.extend(["", "## 상승 원인 후보 우선순위", ""])
    lines.extend(up_reasons)
    lines.extend(["", "## 하락 원인 후보 우선순위", ""])
    lines.extend(down_reasons)
    lines.extend([
        "",
        "## 종합 판단",
        "",
        "최근 6개월 두산에너빌리티의 상승/하락은 DART 재무 수치 하나로 설명하기보다, 고거래대금이 붙은 가격 재평가 구간과 외국인/기관 수급 변화, 그리고 공시 이벤트가 겹친 구간을 우선 후보로 보는 것이 덜 무리하다.",
        "",
        "상승 후보는 외국인/기관 매수와 자사주/계약/IR 등 DART 이벤트가 같은 방향으로 붙는 날을 가장 높게 본다. 하락 후보는 악재성 공시가 명확하지 않은 경우, 고거래대금 급등 후 차익실현과 외국인/기관 순매도 전환을 우선한다.",
        "",
        "주의할 점은 외부 요인을 제외했기 때문에 원전, 전력기기, 정책, 글로벌 피어 밸류에이션 같은 설명력 있는 변수가 빠져 있다는 점이다. 따라서 이 보고서는 내부 수치 기반의 원인 후보 정리로만 사용해야 한다.",
        "",
    ])
    return "\n".join(lines)


async def main() -> None:
    print("KIS/DART 데이터 수집 시작")
    corp_map = await get_corp_code_map()
    corp_code = corp_map[TICKER]
    print(f"corp_code={corp_code}")
    ohlcv, investor, current_inv, short_df, snapshot, disclosures, financials, structured = await asyncio.gather(
        fetch_ohlcv(BASE_START, END),
        fetch_investor_range(MAIN_START, END),
        fetch_investor_current(),
        fetch_short_sale(MAIN_START, END),
        fetch_price_snapshot(),
        fetch_dart_disclosures(corp_code, MAIN_START, END),
        fetch_financials(corp_code),
        fetch_dart_structured(corp_code, MAIN_START, END),
    )
    print(f"ohlcv={len(ohlcv)} investor={len(investor)} current_inv={len(current_inv)} short={len(short_df)} disclosures={len(disclosures)} financials={len(financials)}")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    md = make_markdown(ohlcv, investor, current_inv, short_df, snapshot, disclosures, financials, structured)
    OUT.write_text(md, encoding="utf-8")
    print(str(OUT))


if __name__ == "__main__":
    asyncio.run(main())
