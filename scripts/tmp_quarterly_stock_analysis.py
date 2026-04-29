from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import pandas as pd

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import aiohttp  # noqa: E402
import httpx  # noqa: E402

from config import settings  # noqa: E402
from core.api.auth import get_real_access_token  # noqa: E402
from core.api.client import get_marketdata  # noqa: E402
from scripts.screener_lib.dart import DART_API_KEY, DART_BASE_URL, get_corp_code_map  # noqa: E402

import google.generativeai as genai  # noqa: E402 — NewsSentimentStrategy 등 기존 코드용

try:
    from google import genai as genai_new  # type: ignore[attr-defined]  # noqa: E402 — Search grounding용 신 SDK
    from google.genai.types import GenerateContentConfig, GoogleSearch, Tool  # noqa: E402
except Exception:  # pragma: no cover - optional dependency
    genai_new = None
    GenerateContentConfig = GoogleSearch = Tool = None


OUT_DIR = ROOT / "ai 주가 변동 원인 분석" / "00_기업별분석"
GEMINI_CACHE_PATH = ROOT / "data" / "gemini_external_cache.json"
GEMINI_SEARCH_MODEL = "gemini-2.5-flash"
GEMINI_CACHE_VERSION = "external-v2"
PERIODS = [
    ("2026_Q1", "2026년 1분기", "2026-01-01", "2026-03-31"),
    ("2025_Q4", "2025년 4분기", "2025-10-01", "2025-12-31"),
    ("2025_Q3", "2025년 3분기", "2025-07-01", "2025-09-30"),
    ("2025_Q2", "2025년 2분기", "2025-04-01", "2025-06-30"),
    ("2025_Q1", "2025년 1분기", "2025-01-01", "2025-03-31"),
    ("2024_Q4", "2024년 4분기", "2024-10-01", "2024-12-31"),
    ("2024_Q3", "2024년 3분기", "2024-07-01", "2024-09-30"),
    ("2024_Q2", "2024년 2분기", "2024-04-01", "2024-06-30"),
    ("2024_Q1", "2024년 1분기", "2024-01-01", "2024-03-31"),
    ("2023_Q4", "2023년 4분기", "2023-10-01", "2023-12-31"),
    ("2023_Q3", "2023년 3분기", "2023-07-01", "2023-09-30"),
    ("2023_Q2", "2023년 2분기", "2023-04-01", "2023-06-30"),
    ("2023_Q1", "2023년 1분기", "2023-01-01", "2023-03-31"),
    ("2022_Q4", "2022년 4분기", "2022-10-01", "2022-12-31"),
    ("2022_Q3", "2022년 3분기", "2022-07-01", "2022-09-30"),
    ("2022_Q2", "2022년 2분기", "2022-04-01", "2022-06-30"),
    ("2022_Q1", "2022년 1분기", "2022-01-01", "2022-03-31"),
    ("2021_Q4", "2021년 4분기", "2021-10-01", "2021-12-31"),
    ("2021_Q3", "2021년 3분기", "2021-07-01", "2021-09-30"),
    ("2021_Q2", "2021년 2분기", "2021-04-01", "2021-06-30"),
]


def fmt_won(n: float | int | None) -> str:
    if n is None or pd.isna(n):
        return "N/A"
    eok = float(n) / 100_000_000
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
        resp = await http.get(f"https://openapi.koreainvestment.com:9443{path}", headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    if data.get("rt_cd", "0") != "0":
        raise RuntimeError(f"KIS API error {tr_id}: {data.get('msg1')}")
    return data


async def fetch_ohlcv(ticker: str, start: str, end: str) -> pd.DataFrame:
    async def fetch_chunk(s: pd.Timestamp, e: pd.Timestamp) -> list[dict[str, Any]]:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": s.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": e.strftime("%Y%m%d"),
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
            tr_id="FHKST03010100",
        )
        return data.get("output2", [])

    rows: list[dict[str, Any]] = []
    cur, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    while cur <= end_ts:
        chunk_end = min(cur + pd.Timedelta(days=80), end_ts)
        rows.extend(await fetch_chunk(cur, chunk_end))
        cur = chunk_end + pd.Timedelta(days=1)
        await asyncio.sleep(0.35)

    records = []
    for r in rows:
        if r.get("stck_bsop_date"):
            records.append({
                "date": pd.to_datetime(r.get("stck_bsop_date")),
                "open": float(r.get("stck_oprc") or 0),
                "high": float(r.get("stck_hgpr") or 0),
                "low": float(r.get("stck_lwpr") or 0),
                "close": float(r.get("stck_clpr") or 0),
                "volume": float(r.get("acml_vol") or 0),
                "trade_amount": float(r.get("acml_tr_pbmn") or 0),
            })
    df = pd.DataFrame(records)
    if df.empty:
        return df
    df = df.drop_duplicates("date").sort_values("date").reset_index(drop=True)
    df["chg_pct"] = df["close"].pct_change() * 100
    return df


def parse_investor_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    records = []
    for r in rows:
        date = r.get("stck_bsop_date") or r.get("bsop_date") or r.get("trad_dt")
        if date:
            records.append({
                "date": pd.to_datetime(str(date), format="%Y%m%d", errors="coerce"),
                "foreign_qty": pd.to_numeric(r.get("frgn_ntby_qty"), errors="coerce"),
                "institution_qty": pd.to_numeric(r.get("orgn_ntby_qty") or r.get("inst_ntby_qty"), errors="coerce"),
                "individual_qty": pd.to_numeric(r.get("prsn_ntby_qty") or r.get("indv_ntby_qty"), errors="coerce"),
            })
    df = pd.DataFrame(records)
    if df.empty:
        return df
    return df.dropna(subset=["date"]).drop_duplicates("date").sort_values("date").reset_index(drop=True)


async def fetch_investor_chunk(ticker: str, end_yyyymmdd: str) -> pd.DataFrame:
    data = await kis_get(
        "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_INPUT_DATE_1": end_yyyymmdd,
            "FID_ORG_ADJ_PRC": "",
            "FID_ETC_CLS_CODE": "",
        },
        "FHPTJ04160001",
    )
    rows = data.get("output2") or data.get("output1") or data.get("output") or []
    return parse_investor_rows([rows] if isinstance(rows, dict) else rows)


async def fetch_investor_range(ticker: str, start: str, end: str) -> pd.DataFrame:
    frames = []
    start_ts, cursor = pd.Timestamp(start), pd.Timestamp(end)
    for _ in range(5):
        chunk = await fetch_investor_chunk(ticker, cursor.strftime("%Y%m%d"))
        if chunk.empty:
            break
        frames.append(chunk)
        min_date = chunk["date"].min()
        if min_date <= start_ts:
            break
        cursor = min_date - pd.Timedelta(days=1)
        await asyncio.sleep(0.35)
    if not frames:
        return pd.DataFrame(columns=["date", "foreign_qty", "institution_qty", "individual_qty"])
    df = pd.concat(frames).drop_duplicates("date").sort_values("date").reset_index(drop=True)
    return df[(df["date"] >= start_ts) & (df["date"] <= pd.Timestamp(end))].reset_index(drop=True)


async def fetch_price_snapshot(ticker: str) -> dict[str, Any]:
    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
        tr_id="FHKST01010100",
    )
    return data.get("output", {}) or {}


async def fetch_short_sale(ticker: str, start: str, end: str) -> pd.DataFrame:
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/daily-short-sale",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": start.replace("-", ""),
                "FID_INPUT_DATE_2": end.replace("-", ""),
            },
            tr_id="FHPST04830000",
        )
    except Exception:
        return pd.DataFrame()
    rows = []
    for r in data.get("output2", []) or []:
        date = r.get("stck_bsop_date") or r.get("bsop_date")
        if date:
            rows.append({
                "date": pd.to_datetime(str(date), format="%Y%m%d", errors="coerce"),
                "short_amount": pd.to_numeric(r.get("ssts_tr_pbmn"), errors="coerce"),
            })
    return pd.DataFrame(rows).dropna(subset=["date"]).sort_values("date").reset_index(drop=True) if rows else pd.DataFrame()


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
    data = await dart_get("list.json", {
        "corp_code": corp_code,
        "bgn_de": start.replace("-", ""),
        "end_de": end.replace("-", ""),
        "page_count": 100,
    })
    return data.get("list", []) if data.get("status") == "000" else []


def parse_financial_list(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    chosen_fs = ""
    for row in rows:
        account = (row.get("account_nm") or "").strip()
        field = None
        if "매출액" in account or "수익" in account:
            field = "revenue"
        elif "영업이익" in account:
            field = "op_income"
        elif "당기순이익" in account:
            field = "net_income"
        elif account == "자산총계":
            field = "total_assets"
        elif account == "부채총계":
            field = "total_debt"
        elif account == "자본총계":
            field = "equity"
        if not field:
            continue
        fs_div = row.get("fs_div", "")
        if fs_div == "OFS" and chosen_fs == "CFS":
            continue
        try:
            out[field] = int(str(row.get("thstrm_amount") or "").replace(",", ""))
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
    report_codes = {
        "11013": "1분기보고서",
        "11012": "반기보고서",
        "11014": "3분기보고서",
        "11011": "사업보고서",
    }
    out = {}
    for year in range(2020, 2027):
        for report_code, report_name in report_codes.items():
            label = f"{year} {report_name}"
            data = await dart_get("fnlttMultiAcnt.json", {"corp_code": corp_code, "bsns_year": str(year), "reprt_code": report_code})
            if data.get("status") == "000":
                parsed = parse_financial_list(data.get("list", []))
                if parsed:
                    out[label] = parsed
            await asyncio.sleep(0.12)
    return out


async def fetch_dart_structured(corp_code: str, start: str, end: str) -> dict[str, list[dict[str, Any]]]:
    endpoints = {
        "자기주식취득결정": "tsstkAqDecsn.json",
        "대량보유상황보고": "majorstock.json",
        "단일판매공급계약": "singleSellContract.json",
    }
    out: dict[str, list[dict[str, Any]]] = {}
    for label, endpoint in endpoints.items():
        data = await dart_get(endpoint, {"corp_code": corp_code, "bgn_de": start.replace("-", ""), "end_de": end.replace("-", "")})
        if data.get("status") == "000":
            rows = []
            for r in data.get("list", []):
                raw_dt = r.get("rcept_dt") or r.get("rcept_de") or r.get("cntrct_cncls_de") or ""
                dt = pd.to_datetime(str(raw_dt).replace("-", ""), format="%Y%m%d", errors="coerce")
                if pd.isna(dt) or (pd.Timestamp(start) <= dt <= pd.Timestamp(end)):
                    rows.append(r)
            out[label] = rows
        await asyncio.sleep(0.25)
    return out


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


def filter_ohlcv(df: pd.DataFrame, start: str | pd.Timestamp, end: str | pd.Timestamp) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return pd.DataFrame()
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    return df[(df["date"] >= start_ts) & (df["date"] <= end_ts)].copy().reset_index(drop=True)


def filter_investor(df: pd.DataFrame, start: str | pd.Timestamp, end: str | pd.Timestamp) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return pd.DataFrame()
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    return df[(df["date"] >= start_ts) & (df["date"] <= end_ts)].copy().reset_index(drop=True)


def filter_disclosures(disclosures: list[dict[str, Any]], start: str | pd.Timestamp, end: str | pd.Timestamp) -> list[dict[str, Any]]:
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    out = []
    for disc in disclosures:
        dt = pd.to_datetime(disc.get("rcept_dt"), format="%Y%m%d", errors="coerce")
        if pd.notna(dt) and start_ts <= dt <= end_ts:
            out.append(disc)
    return out


def signed(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def future_returns(qdf: pd.DataFrame, row: pd.Series) -> dict[str, float | None]:
    idx = qdf.index[qdf["date"] == row["date"]]
    if len(idx) == 0 or not row.get("close"):
        return {f"d{n}": None for n in (1, 3, 5, 10)}
    i = int(idx[0])
    out: dict[str, float | None] = {}
    for n in (1, 3, 5, 10):
        j = i + n
        out[f"d{n}"] = None if j >= len(qdf) else (qdf.iloc[j]["close"] / row["close"] - 1) * 100
    return out


def follow_judgment(direction: str, fut: dict[str, float | None]) -> str:
    d5 = fut.get("d5")
    d3 = fut.get("d3")
    ref = d5 if d5 is not None else d3
    if ref is None:
        return "후속 거래일 부족"
    if direction == "up":
        if ref >= 3:
            return "상승 지속"
        if ref >= 0:
            return "상승 유지"
        if ref <= -3:
            return "상승 실패"
        return "단기 되돌림"
    if ref <= -3:
        return "하락 지속"
    if ref <= 0:
        return "하락 유지"
    if ref >= 3:
        return "하락 후 반등"
    return "단기 반등"


def event_tags(direction: str, row: pd.Series, ctx: dict[str, Any], avg_amount: float) -> list[str]:
    tags: list[str] = []
    amount = signed(row.get("trade_amount")) or 0
    foreign = signed(ctx.get("foreign_11d"))
    institution = signed(ctx.get("institution_11d"))
    individual = signed(ctx.get("individual_11d"))
    if avg_amount and amount >= avg_amount * 2:
        tags.append("거래대금급증")
    elif avg_amount and amount >= avg_amount:
        tags.append("거래대금평균상회")
    else:
        tags.append("거래대금약함")
    if foreign is not None and institution is not None:
        if foreign > 0 and institution > 0:
            tags.append("외국인기관동반매수")
        elif foreign < 0 and institution < 0:
            tags.append("외국인기관동반매도")
        else:
            tags.append("외국인기관수급엇갈림")
    if individual is not None:
        tags.append("개인매수" if individual > 0 else "개인매도")
    tags.append("DART공시동반" if ctx["disclosures"] else "주변공시부재")
    tags.append("상승이벤트" if direction == "up" else "하락이벤트")
    return tags


def event_priority(direction: str, row: pd.Series, ctx: dict[str, Any], avg_amount: float, fut: dict[str, float | None]) -> list[str]:
    amount = signed(row.get("trade_amount")) or 0
    foreign = signed(ctx.get("foreign_11d"))
    institution = signed(ctx.get("institution_11d"))
    disclosure_text = "주변 DART 공시 동반" if ctx["disclosures"] else "주변 DART 공시 부재"
    amount_text = "분기 평균 대비 거래대금 급증" if avg_amount and amount >= avg_amount * 2 else "거래대금 확인"
    follow = follow_judgment(direction, fut)
    if direction == "up":
        flow_text = "외국인/기관 동반 순매수" if foreign is not None and institution is not None and foreign > 0 and institution > 0 else "외국인/기관 수급 확인"
        return [
            f"거래대금 후보: {amount_text}",
            f"수급 후보: {flow_text}",
            f"공시 후보: {disclosure_text}",
            f"후속 흐름 후보: {follow}",
        ]
    flow_text = "외국인/기관 동반 순매도" if foreign is not None and institution is not None and foreign < 0 and institution < 0 else "외국인/기관 수급 확인"
    return [
        f"거래대금 후보: {amount_text}",
        f"수급 이탈 후보: {flow_text}",
        f"공시/매물 후보: {disclosure_text}",
        f"후속 흐름 후보: {follow}",
    ]


def window_summary(
    label: str,
    ohlcv: pd.DataFrame,
    investor: pd.DataFrame,
    disclosures: list[dict[str, Any]],
    avg_amount: float,
) -> dict[str, Any]:
    if ohlcv.empty or "date" not in ohlcv.columns:
        start_dt = end_dt = None
        ret = None
        amount_avg = amount_max = None
    else:
        q = ohlcv.sort_values("date").reset_index(drop=True)
        start_dt, end_dt = q["date"].min(), q["date"].max()
        ret = None if len(q) < 2 or not q.iloc[0].get("close") else (q.iloc[-1]["close"] / q.iloc[0]["close"] - 1) * 100
        amount_avg = q["trade_amount"].mean()
        amount_max = q["trade_amount"].max()

    foreign = investor["foreign_qty"].sum(min_count=1) if not investor.empty and "foreign_qty" in investor.columns else None
    institution = investor["institution_qty"].sum(min_count=1) if not investor.empty and "institution_qty" in investor.columns else None
    individual = investor["individual_qty"].sum(min_count=1) if not investor.empty and "individual_qty" in investor.columns else None
    ratio = None if not avg_amount or amount_avg is None or pd.isna(amount_avg) else amount_avg / avg_amount
    return {
        "label": label,
        "start": start_dt,
        "end": end_dt,
        "trading_days": 0 if ohlcv.empty else len(ohlcv),
        "return": ret,
        "avg_amount": amount_avg,
        "max_amount": amount_max,
        "avg_amount_ratio": ratio,
        "foreign": foreign,
        "institution": institution,
        "individual": individual,
        "disclosures": disclosures,
    }


def event_windows(
    row: pd.Series,
    all_ohlcv: pd.DataFrame,
    all_investor: pd.DataFrame,
    all_disclosures: list[dict[str, Any]],
    period_start: str,
    avg_amount: float,
) -> dict[str, dict[str, Any]]:
    d = pd.Timestamp(row["date"])
    period_start_ts = pd.Timestamp(period_start)

    pre20_ohlcv = all_ohlcv[all_ohlcv["date"] < d].sort_values("date").tail(20) if not all_ohlcv.empty and "date" in all_ohlcv.columns else pd.DataFrame()
    if not pre20_ohlcv.empty:
        pre20_start, pre20_end = pre20_ohlcv["date"].min(), pre20_ohlcv["date"].max()
    else:
        pre20_start, pre20_end = d - pd.Timedelta(days=40), d - pd.Timedelta(days=1)

    prev_q_start = period_start_ts - pd.DateOffset(months=3)
    prev_q_end = period_start_ts - pd.Timedelta(days=1)
    six_m_start = d - pd.DateOffset(months=6)
    six_m_end = d - pd.Timedelta(days=1)

    return {
        "pre20": window_summary(
            "선반영 창(D-20~D-1)",
            pre20_ohlcv,
            filter_investor(all_investor, pre20_start, pre20_end),
            filter_disclosures(all_disclosures, pre20_start, pre20_end),
            avg_amount,
        ),
        "prev_quarter": window_summary(
            "분기 선행 창(직전 분기)",
            filter_ohlcv(all_ohlcv, prev_q_start, prev_q_end),
            filter_investor(all_investor, prev_q_start, prev_q_end),
            filter_disclosures(all_disclosures, prev_q_start, prev_q_end),
            avg_amount,
        ),
        "six_month": window_summary(
            "장기 배경 창(직전 6개월)",
            filter_ohlcv(all_ohlcv, six_m_start, six_m_end),
            filter_investor(all_investor, six_m_start, six_m_end),
            filter_disclosures(all_disclosures, six_m_start, six_m_end),
            avg_amount,
        ),
    }


def _flow_matches(direction: str, foreign: Any, institution: Any) -> bool:
    foreign_v = signed(foreign)
    institution_v = signed(institution)
    if foreign_v is None or institution_v is None:
        return False
    if direction == "up":
        return foreign_v > 0 and institution_v > 0
    return foreign_v < 0 and institution_v < 0


def _return_matches(direction: str, value: Any) -> bool:
    ret = signed(value)
    if ret is None:
        return False
    return ret > 3 if direction == "up" else ret < -3


def classify_windows(direction: str, row: pd.Series, ctx: dict[str, Any], windows: dict[str, dict[str, Any]], avg_amount: float) -> tuple[list[str], list[str]]:
    classes: list[str] = []
    reasons: list[str] = []
    amount = signed(row.get("trade_amount")) or 0
    direct = amount >= avg_amount * 1.5 if avg_amount else False
    direct = direct or _flow_matches(direction, ctx.get("foreign_11d"), ctx.get("institution_11d")) or bool(ctx.get("disclosures"))
    if direct:
        classes.append("직접 반응형")
        reasons.append("D-5~D+5에서 거래대금, 수급 또는 공시가 확인된다.")

    pre20 = windows["pre20"]
    if _return_matches(direction, pre20.get("return")) or _flow_matches(direction, pre20.get("foreign"), pre20.get("institution")):
        classes.append("선반영형")
        reasons.append("D-20~D-1에 같은 방향의 가격 또는 외국인/기관 누적 수급이 확인된다.")

    prev_q = windows["prev_quarter"]
    if _return_matches(direction, prev_q.get("return")) or _flow_matches(direction, prev_q.get("foreign"), prev_q.get("institution")) or len(prev_q.get("disclosures", [])) >= 3:
        classes.append("누적 배경형")
        reasons.append("직전 분기 가격, 수급 또는 공시 누적이 배경 후보로 확인된다.")

    six_m = windows["six_month"]
    if _return_matches(direction, six_m.get("return")) or _flow_matches(direction, six_m.get("foreign"), six_m.get("institution")) or len(six_m.get("disclosures", [])) >= 8:
        if "누적 배경형" not in classes:
            classes.append("누적 배경형")
        reasons.append("직전 6개월 장기 흐름 또는 반복 공시가 배경 후보로 확인된다.")

    if not classes:
        classes.append("설명 부족형")
        reasons.append("직접 반응, 선반영, 누적 배경 창 모두에서 강한 내부 근거가 약하다.")
    return classes, reasons


def fmt_window_summary(summary: dict[str, Any]) -> str:
    if summary["trading_days"] == 0:
        return f"- {summary['label']}: KIS 일봉 데이터 없음, 공시 {len(summary['disclosures'])}건"
    start = summary["start"].strftime("%Y-%m-%d") if pd.notna(summary["start"]) else "N/A"
    end = summary["end"].strftime("%Y-%m-%d") if pd.notna(summary["end"]) else "N/A"
    ratio = "N/A" if summary["avg_amount_ratio"] is None or pd.isna(summary["avg_amount_ratio"]) else f"{summary['avg_amount_ratio']:.2f}배"
    return (
        f"- {summary['label']}: {start}~{end}, {summary['trading_days']}거래일, "
        f"수익률 {fmt_pct(summary['return'])}, 평균 거래대금 {fmt_won(summary['avg_amount'])}({ratio}), "
        f"외국인 {fmt_int(summary['foreign'])}주, 기관 {fmt_int(summary['institution'])}주, "
        f"개인 {fmt_int(summary['individual'])}주, 공시 {len(summary['disclosures'])}건"
    )


import hashlib
import json


def _gemini_cache_key(ticker: str, date: pd.Timestamp, direction: str, trigger: str) -> str:
    raw = f"{GEMINI_CACHE_VERSION}_{GEMINI_SEARCH_MODEL}_{ticker}_{date.strftime('%Y%m%d')}_{direction}_{trigger}"
    return hashlib.md5(raw.encode()).hexdigest()


def _load_gemini_cache() -> dict[str, Any]:
    if GEMINI_CACHE_PATH.exists():
        try:
            return json.loads(GEMINI_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_gemini_cache(cache: dict[str, Any]) -> None:
    GEMINI_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    GEMINI_CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def needs_gemini(
    direction: str,
    row: pd.Series,
    ctx: dict[str, Any],
    fin: dict[str, Any],
    avg_amount: float,
    avg_volume: float,
) -> tuple[bool, str]:
    """Gemini 검색 트리거 여부 판단.

    조건 A: 거래대금·수급·공시 모두 신호 약함 (설명 공백)
    조건 B: KIS/DART 수치 방향 ↔ 실제 주가 방향 불일치 (역행)
    """
    amount = signed(row.get("trade_amount")) or 0
    foreign = signed(ctx.get("foreign_11d"))
    institution = signed(ctx.get("institution_11d"))
    has_flow = foreign is not None and institution is not None
    combined = (foreign or 0.0) + (institution or 0.0)
    chg_pct = float(row.get("chg_pct") or 0)

    # 조건 A: 전 항목 신호 약함
    weak_volume = avg_amount > 0 and amount < avg_amount
    no_disclosure = len(ctx.get("disclosures", [])) == 0
    unclear_flow = (not has_flow) or combined == 0 or (
        has_flow and (foreign > 0) != (institution > 0)  # 외국인·기관 방향 엇갈림
    )
    if weak_volume and no_disclosure and unclear_flow:
        return True, "A"

    # 조건 B: 수치 방향 ↔ 실제 주가 역행
    # avg_volume의 3%를 최소 의미있는 수급 규모로 간주 (1주라도 > 0이면 트리거되는 문제 방지)
    flow_threshold = max(avg_volume * 0.03, 1000.0) if avg_volume > 0 else 1000.0
    if has_flow and abs(combined) >= flow_threshold:
        if combined > 0 and chg_pct < -2.0:   # 수급 매수 우세인데 실제 하락
            return True, "B"
        if combined < 0 and chg_pct > 2.0:    # 수급 매도 우세인데 실제 상승
            return True, "B"

    return False, ""


async def query_gemini_external(
    ticker: str,
    name: str,
    date: pd.Timestamp,
    direction: str,
    chg_pct: float,
    trigger: str,
    ctx: dict[str, Any],
) -> str:
    """google-genai 신 SDK + Google Search grounding으로 외부 요인 후보를 검색한다.

    결과는 GEMINI_CACHE_PATH에 캐시하여 동일 (ticker, date, direction, trigger) 조합 재호출을 방지한다.
    """
    if not settings.gemini_api_key:
        return "_GEMINI_API_KEY 미설정 — 외부 요인 검색 불가_"
    if genai_new is None or Tool is None or GenerateContentConfig is None or GoogleSearch is None:
        return "_google-genai 미설치 — Google Search grounding 기반 외부 요인 검색 불가_"

    cache_key = _gemini_cache_key(ticker, date, direction, trigger)
    cache = _load_gemini_cache()
    if cache_key in cache:
        return cache[cache_key]

    date_str = date.strftime("%Y년 %m월 %d일")
    dir_str = "상승" if direction == "up" else "하락"
    foreign = signed(ctx.get("foreign_11d"))
    institution = signed(ctx.get("institution_11d"))
    combined = (foreign or 0.0) + (institution or 0.0)

    if trigger == "A":
        situation = (
            "KIS 거래대금, 투자자 수급, DART 공시 모두 뚜렷한 신호가 없었음에도 "
            f"주가가 {chg_pct:+.1f}% 변동했다."
        )
    elif combined > 0:
        situation = (
            f"외국인+기관 수급이 {combined:+,.0f}주 순매수로 상승 신호였으나, "
            f"실제 주가는 {chg_pct:.1f}% 하락했다. 내부 수치와 반대로 움직인 상황이다."
        )
    else:
        situation = (
            f"외국인+기관 수급이 {combined:+,.0f}주 순매도로 하락 신호였으나, "
            f"실제 주가는 {chg_pct:+.1f}% 상승했다. 내부 수치와 반대로 움직인 상황이다."
        )

    prompt = (
        f"한국 주식 분석 질문입니다.\n\n"
        f"종목: {name}({ticker})\n"
        f"날짜: {date_str}\n"
        f"주가 변동: {chg_pct:+.1f}% ({dir_str})\n"
        f"상황: {situation}\n\n"
        f"이 날짜 전후로 위 종목의 주가 {dir_str}을 설명할 수 있는 외부 요인"
        f"(뉴스, 정책, 업종 이슈, 글로벌 이벤트, 테마 등)을 Google Search로 검색하여 설명해주세요.\n"
        f"내부 수치(재무, 수급)가 아닌 외부 원인 후보에 집중해주세요.\n\n"
        f"각 항목을 아래 형식으로 3~5개 작성해주세요:\n"
        f"- 요인: (외부 요인 설명)\n"
        f"  근거 날짜: YYYY-MM-DD\n"
        f"  출처명: (신문사/기관명)\n"
        f"  URL: (기사 링크 또는 N/A)\n"
        f"  관련성: 높음/중간/낮음\n"
        f"  신뢰도: 높음/중간/낮음\n"
    )

    client = genai_new.Client(api_key=settings.gemini_api_key)
    response = None
    last_exc: Exception | None = None
    retry_waits = [20, 60, 120]
    for attempt in range(4):
        try:
            response = await client.aio.models.generate_content(
                model=GEMINI_SEARCH_MODEL,
                contents=prompt,
                config=GenerateContentConfig(
                    tools=[Tool(google_search=GoogleSearch())],
                ),
            )
            break
        except Exception as exc:
            last_exc = exc
            msg = str(exc)
            retryable = "429" in msg or "RESOURCE_EXHAUSTED" in msg or "rate limit" in msg.lower()
            if not retryable or attempt == 3:
                return f"_Gemini 호출 실패: {exc}_"
            await asyncio.sleep(retry_waits[attempt])

    try:
        if response is None:
            return f"_Gemini 호출 실패: {last_exc}_"
        text = response.text.strip() if response.text else ""

        # grounding 출처 URL이 있으면 하단에 첨부
        sources: list[str] = []
        try:
            meta = response.candidates[0].grounding_metadata
            if meta and meta.grounding_chunks:
                for chunk in meta.grounding_chunks:
                    if hasattr(chunk, "web") and chunk.web:
                        sources.append(f"- {chunk.web.title}: {chunk.web.uri}")
        except Exception:
            pass

        if sources:
            text += "\n\n**검색 출처:**\n" + "\n".join(sources)

        cache[cache_key] = text
        _save_gemini_cache(cache)
        return text

    except Exception as exc:
        return f"_Gemini 호출 실패: {exc}_"


async def append_event_detail(
    lines: list[str],
    event_records: list[dict[str, Any]],
    direction: str,
    events: pd.DataFrame,
    qdf: pd.DataFrame,
    investor: pd.DataFrame,
    disclosures: list[dict[str, Any]],
    all_ohlcv: pd.DataFrame,
    all_investor: pd.DataFrame,
    all_disclosures: list[dict[str, Any]],
    period_start: str,
    avg_amount: float,
    ticker: str,
    name: str,
    quarter: str,
    market_regime: str,
    fin: dict[str, Any],
    avg_volume: float,
) -> None:
    label = "상승" if direction == "up" else "하락"
    lines.extend(["", f"## 이벤트별 {label} 원인 후보 판정", ""])
    if events.empty:
        lines.extend([
            "- 해당 분기에서 선별된 이벤트가 없다.",
            "- 원인 창 분류: 가격 이벤트 없음",
            "- 선행 시그널 여부: 산정 불가",
            "- 멀티 윈도우 요약: 이벤트 기준일이 없어 산정하지 않는다.",
            "",
            "#### 외부 자료 확인 필요 — 사유: 가격 이벤트 없음",
            "",
            "- KIS 가격 이벤트가 없어 상승/하락 원인을 내부 수치로 특정할 수 없다.",
            "- Gemini/GPT 기반 외부 요인 검색은 API 한도 문제로 현재 비활성화되어 있다.",
            "- 추후 외부 자료 확인 시 상장 일정, 보호예수, 뉴스, 정책, 업황, 금리, 환율, 글로벌 피어 주가를 별도 검토해야 한다.",
        ])
        return
    for _, r in events.iterrows():
        ctx = event_context(r, investor, disclosures)
        fut = future_returns(qdf, r)
        windows = event_windows(r, all_ohlcv, all_investor, all_disclosures, period_start, avg_amount)
        window_classes, window_reasons = classify_windows(direction, r, ctx, windows, avg_amount)
        priorities = event_priority(direction, r, ctx, avg_amount, fut)
        tags = event_tags(direction, r, ctx, avg_amount)
        follow = follow_judgment(direction, fut)
        leading_signal = "있음" if any(c in window_classes for c in ["선반영형", "누적 배경형"]) else "약함"
        call_gemini, trigger = needs_gemini(direction, r, ctx, fin, avg_amount, avg_volume)
        needs_external_review = call_gemini or "설명 부족형" in window_classes
        amount_tags = {"거래대금급증", "거래대금평균상회", "거래대금약함"}
        dart_tags = {"DART공시동반", "주변공시부재"}
        direction_tags = {"상승이벤트", "하락이벤트"}
        event_records.append({
            "event_id": f"{ticker}_{quarter}_{r['date'].strftime('%Y-%m-%d')}_{direction}",
            "ticker": ticker,
            "name": name,
            "quarter": quarter,
            "date": r["date"].strftime("%Y-%m-%d"),
            "direction": direction,
            "chg_pct": clean_json_value(r.get("chg_pct")),
            "trade_amount": clean_json_value(r.get("trade_amount")),
            "amount_tag": next((tag for tag in tags if tag in amount_tags), ""),
            "flow_tags": [tag for tag in tags if tag not in amount_tags and tag not in dart_tags and tag not in direction_tags],
            "dart_tag": next((tag for tag in tags if tag in dart_tags), ""),
            "window_types": window_classes,
            "leading_signal": leading_signal,
            "followup": follow.replace(" ", ""),
            "needs_external_review": needs_external_review,
            "market_regime": market_regime,
            "external_review_trigger": trigger if call_gemini else ("window_explanation_weak" if "설명 부족형" in window_classes else ""),
        })
        lines.extend([
            f"### {r['date'].strftime('%Y-%m-%d')} {label} 이벤트",
            "",
            f"- 당일 등락률: {fmt_pct(r['chg_pct'])}",
            f"- 당일 거래대금: {fmt_won(r['trade_amount'])}",
            f"- 후속 수익률: D+1 {fmt_pct(fut['d1'])}, D+3 {fmt_pct(fut['d3'])}, D+5 {fmt_pct(fut['d5'])}, D+10 {fmt_pct(fut['d10'])}",
            f"- 후속 판정: {follow}",
            f"- 시그널 태그: {', '.join(tags)}",
            f"- 원인 창 분류: {', '.join(window_classes)}",
            f"- 선행 시그널 여부: {leading_signal}",
            "- 멀티 윈도우 요약:",
            fmt_window_summary(windows["pre20"]),
            fmt_window_summary(windows["prev_quarter"]),
            fmt_window_summary(windows["six_month"]),
            "- 창 분류 근거:",
        ])
        for reason in window_reasons:
            lines.append(f"  - {reason}")
        lines.extend([
            "- 원인 후보 우선순위:",
        ])
        for i, item in enumerate(priorities, 1):
            lines.append(f"  {i}. {item}")

        if needs_external_review:
            trigger_label = (
                "수치 신호 약함 — 거래대금·수급·공시 모두 불충분" if trigger == "A"
                else "수치 방향 ↔ 실제 주가 방향 불일치" if trigger == "B"
                else "멀티 윈도우 내부 데이터 설명 부족"
            )
            lines.extend([
                "",
                f"#### 외부 자료 확인 필요 — 사유: {trigger_label}",
                "",
                "- 현재 보고서는 KIS 시세/수급 및 DART 공시/재무 데이터를 우선 사용한다.",
                "- 위 내부 데이터만으로 상승/하락 원인 후보의 설명력이 약하거나 방향이 엇갈린다.",
                "- Gemini/GPT 기반 외부 요인 검색은 API 한도 문제로 현재 비활성화되어 있다.",
                "- 추후 외부 자료 확인 시 뉴스, 정책, 업황, 전쟁/지정학, 금리, 환율, 글로벌 피어 주가를 별도 검토해야 한다.",
            ])

        # TODO: 외부 요인 검색 — Gemini/GPT API 한도 문제로 임시 비활성화
        # 활성화하려면 아래 주석을 해제하고 API 플랜 확인 후 사용
        # if call_gemini:
        #     trigger_label = (
        #         "수치 신호 약함 — 거래대금·수급·공시 모두 불충분" if trigger == "A"
        #         else "수치 방향 ↔ 실제 주가 방향 불일치"
        #     )
        #     lines.extend(["", f"#### 외부 요인 후보 (Gemini 검색) — 트리거: {trigger_label}", ""])
        #     print(f"  [Gemini] {name} {r['date'].strftime('%Y-%m-%d')} {label} 이벤트 외부 요인 검색 중... (트리거={trigger})")
        #     gemini_text = await query_gemini_external(
        #         ticker, name, r["date"], direction, float(r.get("chg_pct") or 0), trigger, ctx
        #     )
        #     lines.extend(gemini_text.splitlines())

        lines.append("")


def period_note(code: str) -> str:
    year = int(code[:4])
    quarter = int(code[-1])
    if quarter == 1:
        return f"{year - 1} 사업보고서 제출 전후 수치와 {year}년 1분기 수급/시세 반응을 연결한다."
    if quarter == 2:
        return f"{year} 반기보고서와 해당 분기 수급/시세 반응을 연결한다."
    if quarter == 3:
        return f"{year} 3분기보고서와 해당 분기 수급/시세 반응을 연결한다."
    return f"연말 구간이므로 {year} 사업보고서는 사후 확인용 수치로 사용하고, 해당 분기 수급/시세 반응을 우선한다."


def selected_financial(financials: dict[str, dict[str, Any]], code: str) -> dict[str, Any]:
    year = int(code[:4])
    quarter = int(code[-1])
    if quarter == 1:
        preferred = [f"{year - 1} 사업보고서", f"{year - 1} 3분기보고서", f"{year - 2} 사업보고서"]
    elif quarter == 2:
        preferred = [f"{year} 반기보고서", f"{year} 1분기보고서", f"{year - 1} 사업보고서"]
    elif quarter == 3:
        preferred = [f"{year} 3분기보고서", f"{year} 반기보고서", f"{year - 1} 사업보고서"]
    else:
        preferred = [f"{year} 사업보고서", f"{year} 3분기보고서", f"{year} 반기보고서", f"{year - 1} 사업보고서"]
    for label in preferred:
        if label in financials:
            return financials[label]
    return next(iter(financials.values()), {})


def market_regime_for_quarter(code: str) -> str:
    year = int(code[:4])
    quarter = int(code[-1])
    ordinal = year * 4 + quarter
    if 2021 * 4 + 2 <= ordinal <= 2021 * 4 + 4:
        return "유동성/저금리 후반장"
    if 2022 * 4 + 1 <= ordinal <= 2023 * 4 + 1:
        return "금리 인상/긴축장"
    if 2023 * 4 + 2 <= ordinal <= 2023 * 4 + 4:
        return "반도체 반등장"
    if 2024 * 4 + 1 <= ordinal <= 2025 * 4 + 2:
        return "AI/전력기기 테마장"
    if 2025 * 4 + 3 <= ordinal <= 2026 * 4 + 1:
        return "변동성 장세"
    return "미분류"


def clean_json_value(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


async def make_report(
    ticker: str,
    name: str,
    code: str,
    title: str,
    start: str,
    end: str,
    ohlcv: pd.DataFrame,
    investor: pd.DataFrame,
    short_df: pd.DataFrame,
    snapshot: dict[str, Any],
    disclosures: list[dict[str, Any]],
    financials: dict[str, dict[str, Any]],
    structured: dict[str, list[dict[str, Any]]],
) -> tuple[str, list[dict[str, Any]]]:
    all_ohlcv = ohlcv
    all_investor = investor
    all_disclosures = disclosures
    ohlcv = filter_ohlcv(all_ohlcv, start, end)
    investor = filter_investor(all_investor, start, end)
    disclosures = filter_disclosures(all_disclosures, start, end)
    event_records: list[dict[str, Any]] = []

    if ohlcv.empty or "date" not in ohlcv.columns:
        fin = selected_financial(financials, code)
        disclosure_names = " / ".join([d.get("report_nm", "") for d in disclosures[:5]]) or "해당 분기 주요 공시 제한적"
        lines = [
            f"# {name}({ticker}) {title} 주가 변동 원인 후보 분석",
            "",
            "## 분석 전제",
            "",
            f"- 분석 기간: {start}~{end}",
            "- 실제 KIS 거래일 범위: KIS 일봉 데이터 없음",
            f"- DART 연결 기준: {period_note(code)}",
            "- 외부 뉴스, 정책, 금리, 환율, 테마 요인은 제외 (외부 요인 검색 기능 현재 비활성화)",
            "- 결론은 확정 원인이 아니라 DART/KIS 수치로 설명 가능한 원인 후보",
            "",
            "## 기간 주가 요약",
            "",
            "| 항목 | 수치 |",
            "|---|---:|",
            "| 시작 종가 | N/A |",
            "| 종료 종가 | N/A |",
            "| 기간 수익률 | N/A |",
            "| 평균 거래대금 | N/A |",
            "| 최대 거래대금 | N/A |",
            "",
            "## 주요 상승일",
            "",
            "| 날짜 | 종가 | 등락률 | 거래대금 | 전후 5거래일 외국인 | 전후 5거래일 기관 | 주변 DART 공시 |",
            "|---|---:|---:|---:|---:|---:|---|",
            "| - | N/A | N/A | N/A | N/A | N/A | KIS 일봉 데이터 없음 |",
            "",
            "## 주요 하락일",
            "",
            "| 날짜 | 종가 | 등락률 | 거래대금 | 전후 5거래일 외국인 | 전후 5거래일 기관 | 주변 DART 공시 |",
            "|---|---:|---:|---:|---:|---:|---|",
            "| - | N/A | N/A | N/A | N/A | N/A | KIS 일봉 데이터 없음 |",
            "",
            "## 이벤트별 상승 원인 후보 판정",
            "",
            "- KIS 일봉 데이터가 없어 날짜별 상승 이벤트를 산정하지 않는다.",
            "- 원인 창 분류: 가격 이벤트 없음",
            "- 선행 시그널 여부: 산정 불가",
            "- 멀티 윈도우 요약: 이벤트 기준일이 없어 산정하지 않는다.",
            "",
            "#### 외부 자료 확인 필요 — 사유: KIS 일봉 데이터 없음",
            "",
            "- KIS 가격 이벤트가 없어 상승 원인을 내부 수치로 특정할 수 없다.",
            "- Gemini/GPT 기반 외부 요인 검색은 API 한도 문제로 현재 비활성화되어 있다.",
            "- 추후 외부 자료 확인 시 상장 일정, 보호예수, 뉴스, 정책, 업황, 금리, 환율, 글로벌 피어 주가를 별도 검토해야 한다.",
            "",
            "## 이벤트별 하락 원인 후보 판정",
            "",
            "- KIS 일봉 데이터가 없어 날짜별 하락 이벤트를 산정하지 않는다.",
            "- 원인 창 분류: 가격 이벤트 없음",
            "- 선행 시그널 여부: 산정 불가",
            "- 멀티 윈도우 요약: 이벤트 기준일이 없어 산정하지 않는다.",
            "",
            "#### 외부 자료 확인 필요 — 사유: KIS 일봉 데이터 없음",
            "",
            "- KIS 가격 이벤트가 없어 하락 원인을 내부 수치로 특정할 수 없다.",
            "- Gemini/GPT 기반 외부 요인 검색은 API 한도 문제로 현재 비활성화되어 있다.",
            "- 추후 외부 자료 확인 시 상장 일정, 보호예수, 뉴스, 정책, 업황, 금리, 환율, 글로벌 피어 주가를 별도 검토해야 한다.",
            "",
            "## DART 공시 요약",
            "",
            "| 날짜 | 공시명 |",
            "|---|---|",
        ]
        for d in disclosures[:25]:
            dt = pd.to_datetime(d.get("rcept_dt"), format="%Y%m%d", errors="coerce")
            lines.append(f"| {dt.strftime('%Y-%m-%d') if pd.notna(dt) else d.get('rcept_dt')} | {d.get('report_nm')} |")
        lines.extend([
            "",
            "## DART 주요 재무 수치",
            "",
            "| 기준 | 매출액 | 영업이익 | 순이익 | 영업이익률 | 부채비율 | ROE |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ])
        for label, f in financials.items():
            op_margin = f"{f['op_margin']:.2f}%" if f.get("op_margin") is not None else "N/A"
            debt_ratio = f"{f['debt_ratio']:.2f}%" if f.get("debt_ratio") is not None else "N/A"
            roe = f"{f['roe']:.2f}%" if f.get("roe") is not None else "N/A"
            lines.append(f"| {label} | {fmt_won(f.get('revenue'))} | {fmt_won(f.get('op_income'))} | {fmt_won(f.get('net_income'))} | {op_margin} | {debt_ratio} | {roe} |")
        lines.extend([
            "",
            "## 상승 원인 후보 우선순위",
            "",
            "1. KIS 일봉 데이터 부재: 가격 이벤트를 특정할 수 없어 상승 후보 산정 불가. 외부 요인 검색도 현재 수행하지 않는다.",
            f"2. DART 이벤트 후보: {disclosure_names}. 가격 반응을 확인할 수 없으므로 보조 후보로만 본다.",
            f"3. 재무 체력 후보: 매출 {fmt_won(fin.get('revenue'))}, 영업이익 {fmt_won(fin.get('op_income'))}, 순이익 {fmt_won(fin.get('net_income'))}. 단독 원인보다 배경 체력으로 본다.",
            "",
            "## 하락 원인 후보 우선순위",
            "",
            "1. KIS 일봉 데이터 부재: 가격 이벤트를 특정할 수 없어 하락 후보 산정 불가. 외부 요인 검색도 현재 수행하지 않는다.",
            "2. 공시 기반 후보 제한: DART 공시가 있더라도 가격·거래대금·수급 확인이 불가능하면 원인 후보 우선순위를 낮춘다.",
            "3. 내부 수치 한계: 내부 수치만으로는 원인 확정이 어렵다.",
            "",
            "## 종합 판단",
            "",
            "이 분기는 KIS 일봉 데이터가 없어 주가 변동 원인 후보를 정량 산정할 수 없고, 날짜별 이벤트를 특정할 수 없다. DART 공시와 재무 수치는 보조 정보로만 참고하며, 실제 원인 확인에는 외부 자료 검토가 필요하다.",
            "",
        ])
        return "\n".join(lines), event_records

    qdf = ohlcv.sort_values("date").reset_index(drop=True)
    if qdf.empty:
        return f"# {name}({ticker}) {title} 분석\n\nKIS 일봉 데이터가 비어 있어 분석하지 못했다.\n", event_records

    start_row, end_row = qdf.iloc[0], qdf.iloc[-1]
    high, low = qdf.loc[qdf["close"].idxmax()], qdf.loc[qdf["close"].idxmin()]
    ret = (end_row["close"] / start_row["close"] - 1) * 100
    up = qdf[qdf["chg_pct"] > 0].sort_values(["chg_pct", "trade_amount"], ascending=[False, False]).head(6)
    down = qdf[qdf["chg_pct"] < 0].sort_values(["chg_pct", "trade_amount"], ascending=[True, False]).head(6)
    fin = selected_financial(financials, code)
    disclosure_names = " / ".join([d.get("report_nm", "") for d in disclosures[:5]]) or "해당 분기 주요 공시 제한적"

    lines = [
        f"# {name}({ticker}) {title} 주가 변동 원인 후보 분석",
        "",
        "## 분석 전제",
        "",
        f"- 분석 기간: {start}~{end}",
        f"- 실제 KIS 거래일 범위: {qdf['date'].min().strftime('%Y-%m-%d')}~{qdf['date'].max().strftime('%Y-%m-%d')} ({len(qdf)}거래일)",
        f"- DART 연결 기준: {period_note(code)}",
        "- 외부 뉴스, 정책, 금리, 환율, 테마 요인은 제외 (외부 요인 검색 기능 현재 비활성화)",
        "- 결론은 확정 원인이 아니라 DART/KIS 수치로 설명 가능한 원인 후보",
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
        f"| 평균 거래대금 | {fmt_won(qdf['trade_amount'].mean())} |",
        f"| 최대 거래대금 | {fmt_won(qdf['trade_amount'].max())} |",
        "",
        "## 주요 상승일",
        "",
        "| 날짜 | 종가 | 등락률 | 거래대금 | 전후 5거래일 외국인 | 전후 5거래일 기관 | 주변 DART 공시 |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for _, r in up.iterrows():
        ctx = event_context(r, investor, disclosures)
        disc = "<br>".join(ctx["disclosures"][:3]) if ctx["disclosures"] else "-"
        lines.append(f"| {r['date'].strftime('%Y-%m-%d')} | {fmt_int(r['close'])}원 | {fmt_pct(r['chg_pct'])} | {fmt_won(r['trade_amount'])} | {fmt_int(ctx['foreign_11d'])}주 | {fmt_int(ctx['institution_11d'])}주 | {disc} |")
    lines.extend(["", "## 주요 하락일", "", "| 날짜 | 종가 | 등락률 | 거래대금 | 전후 5거래일 외국인 | 전후 5거래일 기관 | 주변 DART 공시 |", "|---|---:|---:|---:|---:|---:|---|"])
    for _, r in down.iterrows():
        ctx = event_context(r, investor, disclosures)
        disc = "<br>".join(ctx["disclosures"][:3]) if ctx["disclosures"] else "-"
        lines.append(f"| {r['date'].strftime('%Y-%m-%d')} | {fmt_int(r['close'])}원 | {fmt_pct(r['chg_pct'])} | {fmt_won(r['trade_amount'])} | {fmt_int(ctx['foreign_11d'])}주 | {fmt_int(ctx['institution_11d'])}주 | {disc} |")

    avg_amount = qdf["trade_amount"].mean()
    avg_volume = float(qdf["volume"].mean()) if "volume" in qdf.columns else 0.0
    market_regime = market_regime_for_quarter(code)
    await append_event_detail(lines, event_records, "up", up, qdf, investor, disclosures, all_ohlcv, all_investor, all_disclosures, start, avg_amount, ticker, name, code, market_regime, fin, avg_volume)
    await append_event_detail(lines, event_records, "down", down, qdf, investor, disclosures, all_ohlcv, all_investor, all_disclosures, start, avg_amount, ticker, name, code, market_regime, fin, avg_volume)

    lines.extend([
        "",
        "## KIS 수급 요약",
        "",
        "| 구분 | 분기 누적 순매수 |",
        "|---|---:|",
        f"| 외국인 | {fmt_int(investor['foreign_qty'].sum(min_count=1) if not investor.empty else None)}주 |",
        f"| 기관 | {fmt_int(investor['institution_qty'].sum(min_count=1) if not investor.empty else None)}주 |",
        f"| 개인 | {fmt_int(investor['individual_qty'].sum(min_count=1) if not investor.empty else None)}주 |",
        "",
        "## KIS 현재 참고 지표",
        "",
        "| 항목 | 수치 |",
        "|---|---:|",
        f"| 현재가 | {fmt_int(pd.to_numeric(snapshot.get('stck_prpr'), errors='coerce'))}원 |",
        f"| PER | {snapshot.get('per') or snapshot.get('hts_per') or 'N/A'} |",
        f"| PBR | {snapshot.get('pbr') or 'N/A'} |",
        f"| EPS | {snapshot.get('eps') or 'N/A'} |",
        f"| BPS | {snapshot.get('bps') or 'N/A'} |",
        "",
    ])
    if not short_df.empty:
        lines.extend(["## KIS 공매도 요약", "", "| 항목 | 수치 |", "|---|---:|", f"| 공매도 거래대금 합계 | {fmt_won(short_df['short_amount'].sum(min_count=1))} |", f"| 일평균 공매도 거래대금 | {fmt_won(short_df['short_amount'].mean())} |", f"| 최대 공매도 거래대금 | {fmt_won(short_df['short_amount'].max())} |", ""])

    lines.extend(["## DART 공시 요약", "", "| 날짜 | 공시명 |", "|---|---|"])
    for d in disclosures[:25]:
        dt = pd.to_datetime(d.get("rcept_dt"), format="%Y%m%d", errors="coerce")
        lines.append(f"| {dt.strftime('%Y-%m-%d') if pd.notna(dt) else d.get('rcept_dt')} | {d.get('report_nm')} |")

    lines.extend(["", "## DART 주요 재무 수치", "", "| 기준 | 매출액 | 영업이익 | 순이익 | 영업이익률 | 부채비율 | ROE |", "|---|---:|---:|---:|---:|---:|---:|"])
    for label, f in financials.items():
        op_margin = f"{f['op_margin']:.2f}%" if f.get("op_margin") is not None else "N/A"
        debt_ratio = f"{f['debt_ratio']:.2f}%" if f.get("debt_ratio") is not None else "N/A"
        roe = f"{f['roe']:.2f}%" if f.get("roe") is not None else "N/A"
        lines.append(f"| {label} | {fmt_won(f.get('revenue'))} | {fmt_won(f.get('op_income'))} | {fmt_won(f.get('net_income'))} | {op_margin} | {debt_ratio} | {roe} |")

    lines.extend(["", "## DART 구조화 이벤트 조회 결과", ""])
    for label, rows in structured.items():
        lines.append(f"- {label}: {len(rows)}건")
        for r in rows[:3]:
            keys = ["rcept_no", "rcept_dt", "aqpln_stk_ostk", "aqexpd_bgd", "aqexpd_edd", "cntrct_cncls_de", "cntrct_amount", "hd_stock_qota_rt"]
            vals = [f"{k}={r.get(k)}" for k in keys if r.get(k)]
            if vals:
                lines.append(f"  - {'; '.join(vals)}")

    max_amount = qdf["trade_amount"].max()
    debt_ratio = fin.get("debt_ratio")
    net_margin = fin.get("net_margin")
    lines.extend([
        "",
        "## 상승 원인 후보 우선순위",
        "",
        f"1. 수급 유입 후보: 분기 누적 외국인 {fmt_int(investor['foreign_qty'].sum(min_count=1) if not investor.empty else None)}주, 기관 {fmt_int(investor['institution_qty'].sum(min_count=1) if not investor.empty else None)}주. 상승일 전후 외국인/기관 합산이 양수이면 우선순위를 높인다.",
        f"2. 거래대금 재평가 후보: 분기 평균 거래대금 {fmt_won(avg_amount)}, 최대 거래대금 {fmt_won(max_amount)}. 평균 대비 큰 거래대금이 붙은 상승일은 설명력이 높다.",
        f"3. DART 이벤트 후보: {disclosure_names}. 급등일 전후 5거래일 안에 공시가 있으면 보조 원인으로 본다.",
        f"4. 재무 체력 후보: 기준 재무 수치상 매출 {fmt_won(fin.get('revenue'))}, 영업이익 {fmt_won(fin.get('op_income'))}, 순이익 {fmt_won(fin.get('net_income'))}. 단독 원인보다는 배경 체력으로 본다.",
        "",
        "## 하락 원인 후보 우선순위",
        "",
        "1. 수급 이탈 후보: 하락일 전후 외국인/기관 합산 순매도가 확인되면 수급성 하락 후보로 우선 분류한다.",
        "2. 고거래대금 이후 차익실현 후보: 직전 상승 구간에서 거래대금이 크게 붙은 뒤 하락하면 악재보다 매물 출회 가능성을 먼저 본다.",
        "3. 공시 부재 하락 후보: 하락일 주변 DART 악재성 공시가 없으면 내부 수치만으로는 원인 확정이 어렵고, 수급/변동성 후보로 낮춰 분류한다.",
        f"4. 재무 부담 후보: 기준 재무 수치의 부채비율 {debt_ratio:.2f}%, 순이익률 {net_margin:.2f}%는 조정 민감도를 높이는 보조 후보로 본다." if debt_ratio is not None and net_margin is not None else "4. 재무 부담 후보: 부채비율 또는 순이익률 수치가 제한적이면 재무 수치는 보조 후보로만 둔다.",
        "",
        "## 종합 판단",
        "",
        "이 분기의 주가 변동은 DART 재무 수치 하나로 확정하기보다, 거래대금이 동반된 가격 변동일과 외국인/기관 수급, 그리고 주변 공시 이벤트가 함께 맞는지를 우선순위로 봐야 한다.",
        "",
    ])
    return "\n".join(lines), event_records


async def build_period(ticker: str, name: str, code: str, title: str, start: str, end: str, corp_code: str, financials: dict[str, dict[str, Any]], snapshot: dict[str, Any]) -> Path:
    print(f"{name} {title} 수집 중: {start}~{end}")
    lookback_start = (pd.Timestamp(start) - pd.DateOffset(months=6) - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
    ohlcv, investor, short_df, disclosures, structured = await asyncio.gather(
        fetch_ohlcv(ticker, lookback_start, end),
        fetch_investor_range(ticker, lookback_start, end),
        fetch_short_sale(ticker, start, end),
        fetch_dart_disclosures(corp_code, lookback_start, end),
        fetch_dart_structured(corp_code, start, end),
    )
    md, event_records = await make_report(ticker, name, code, title, start, end, ohlcv, investor, short_df, snapshot, disclosures, financials, structured)
    company_dir = OUT_DIR / name
    company_dir.mkdir(parents=True, exist_ok=True)
    path = company_dir / f"{name}_{code}_원인후보_실제분석.md"
    path.write_text(md, encoding="utf-8")
    events_path = company_dir / f"{name}_{code}_events.jsonl"
    events_path.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in event_records) + ("\n" if event_records else ""),
        encoding="utf-8",
    )
    print(f"  저장: {path.name}, {events_path.name} (events={len(event_records)}, ohlcv={len(ohlcv)}, investor={len(investor)}, disclosures={len(disclosures)})")
    return path


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    corp_map = await get_corp_code_map()
    corp_code = corp_map[args.ticker]
    print(f"{args.name} corp_code={corp_code}")
    financials, snapshot = await asyncio.gather(fetch_financials(corp_code), fetch_price_snapshot(args.ticker))
    paths = []
    for code, title, start, end in PERIODS:
        paths.append(await build_period(args.ticker, args.name, code, title, start, end, corp_code, financials, snapshot))
        await asyncio.sleep(0.5)
    print("생성 완료")
    for p in paths:
        print(p)


if __name__ == "__main__":
    asyncio.run(main())
