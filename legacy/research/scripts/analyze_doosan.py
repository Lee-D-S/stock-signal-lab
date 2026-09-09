"""두산에너빌리티(034020) 3~4월 주가 원인 분석.

수집 데이터:
  1. KIS — 일별 OHLCV (inquire_daily_itemchartprice)
  2. KIS — 종목별 투자자매매동향 (investor_trade_by_stock_daily)
  3. KIS — 공매도 일별추이 (daily_short_sale)
  4. DART — 기간 내 공시 목록 (list.json)
  5. DART — 최신 재무 (fnlttMultiAcnt.json)
"""

from __future__ import annotations

import asyncio
import io
import sys
from pathlib import Path

# Windows CP949 환경에서 UTF-8 출력 강제
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import httpx
import pandas as pd

from core.api.auth import get_real_access_token
from config import settings
from screener_lib.dart import DART_API_KEY, DART_BASE_URL, get_corp_code_map

TICKER      = "034020"
START_DATE  = "20260301"
END_DATE    = "20260427"
KIS_REAL    = "https://openapi.koreainvestment.com:9443"

# ── KIS 공통 요청 ─────────────────────────────────────────────────────────────

async def _kis_get(path: str, params: dict, tr_id: str) -> dict:
    token = await get_real_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "appkey":    settings.kis_real_app_key,
        "appsecret": settings.kis_real_app_secret,
        "tr_id":     tr_id,
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=15) as http:
        resp = await http.get(f"{KIS_REAL}{path}", headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()


# ── 1. OHLCV ─────────────────────────────────────────────────────────────────

async def fetch_ohlcv() -> pd.DataFrame:
    data = await _kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD":         TICKER,
            "FID_INPUT_DATE_1":       START_DATE,
            "FID_INPUT_DATE_2":       END_DATE,
            "FID_PERIOD_DIV_CODE":    "D",
            "FID_ORG_ADJ_PRC":        "0",
        },
        "FHKST03010100",
    )
    rows = data.get("output2", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.rename(columns={
        "stck_bsop_date": "date",
        "stck_oprc": "open",
        "stck_hgpr": "high",
        "stck_lwpr": "low",
        "stck_clpr": "close",
        "acml_vol":  "volume",
        "prdy_ctrt": "chg_pct",
    })
    for col in ["open","high","low","close","volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "chg_pct" in df.columns:
        df["chg_pct"] = pd.to_numeric(df["chg_pct"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


# ── 2. 투자자 매매동향 ─────────────────────────────────────────────────────────

async def fetch_investor() -> pd.DataFrame:
    """종목별 투자자매매동향 — fid_input_date_1 기준 최근 30거래일."""
    data = await _kis_get(
        "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD":         TICKER,
            "FID_INPUT_DATE_1":       END_DATE,
            "FID_ORG_ADJ_PRC":        "",
            "FID_ETC_CLS_CODE":       "",
        },
        "FHPTJ04160001",
    )
    # output1 = 요약(현재가), output2 = 일별 투자자 상세
    rows = data.get("output2") or data.get("output1") or data.get("output", [])
    if not rows:
        return pd.DataFrame()
    if isinstance(rows, dict):
        rows = [rows]
    df = pd.DataFrame(rows)
    # 날짜 컬럼 찾기
    date_col = next((c for c in df.columns if "date" in c.lower() or "bsop" in c.lower()), None)
    if date_col:
        df = df.rename(columns={date_col: "date"})
        df["date"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d", errors="coerce")
        df = df[df["date"] >= pd.Timestamp(START_DATE)]
    return df.sort_values("date").reset_index(drop=True) if "date" in df.columns else df


# ── 3. 공매도 ─────────────────────────────────────────────────────────────────

async def fetch_short_sale() -> pd.DataFrame:
    data = await _kis_get(
        "/uapi/domestic-stock/v1/quotations/daily-short-sale",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD":         TICKER,
            "FID_INPUT_DATE_1":       START_DATE,
            "FID_INPUT_DATE_2":       END_DATE,
        },
        "FHPST04830000",
    )
    rows = data.get("output2", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    date_col = next((c for c in df.columns if "date" in c.lower() or "bsop" in c.lower()), None)
    if date_col:
        df = df.rename(columns={date_col: "date"})
        df["date"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d", errors="coerce")
    return df.sort_values("date").reset_index(drop=True) if "date" in df.columns else df


# ── 4. DART 공시 목록 ─────────────────────────────────────────────────────────

async def fetch_dart_disclosures(corp_code: str) -> list[dict]:
    if not DART_API_KEY:
        return []
    async with httpx.AsyncClient(timeout=15) as http:
        resp = await http.get(
            f"{DART_BASE_URL}/list.json",
            params={
                "crtfc_key": DART_API_KEY,
                "corp_code": corp_code,
                "bgn_de":    START_DATE,
                "end_de":    END_DATE,
                "page_count": 40,
            },
        )
    data = resp.json()
    if data.get("status") != "000":
        return []
    return data.get("list", [])


# ── 5. DART 재무 ──────────────────────────────────────────────────────────────

async def fetch_dart_financials(corp_code: str) -> dict:
    if not DART_API_KEY:
        return {}
    # 2024년 사업보고서 (2026.4.1 이후 확정)
    async with httpx.AsyncClient(timeout=15) as http:
        resp = await http.get(
            f"{DART_BASE_URL}/fnlttMultiAcnt.json",
            params={
                "crtfc_key":  DART_API_KEY,
                "corp_code":  corp_code,
                "bsns_year":  "2024",
                "reprt_code": "11011",
            },
        )
    data = resp.json()
    if data.get("status") != "000":
        return {}

    acct_map = {
        "매출액":       "revenue",
        "수익(매출액)":  "revenue",
        "영업이익":     "op_income",
        "당기순이익":   "net_income",
        "자산총계":     "total_assets",
        "부채총계":     "total_debt",
        "자본총계":     "equity",
    }
    raw: dict = {}
    for row in data.get("list", []):
        nm  = (row.get("account_nm") or "").strip()
        fsd = row.get("fs_div", "OFS")
        if nm not in acct_map:
            continue
        if fsd == "OFS" and raw.get("_fs") == "CFS":
            continue
        try:
            val = int((row.get("thstrm_amount") or "").replace(",", ""))
            raw[acct_map[nm]] = val
            raw["_fs"] = fsd
        except ValueError:
            pass

    raw.pop("_fs", None)
    rev = raw.get("revenue")
    op  = raw.get("op_income")
    net = raw.get("net_income")
    eq  = raw.get("equity")
    dbt = raw.get("total_debt")
    ast = raw.get("total_assets")
    if rev and op:     raw["op_margin"]  = round(op/rev*100, 1)
    if eq  and dbt:    raw["debt_ratio"] = round(dbt/eq*100, 1)
    if eq  and net:    raw["roe"]        = round(net/eq*100, 1)
    if ast and net:    raw["roa"]        = round(net/ast*100, 1)
    return raw


# ── 분석 출력 ─────────────────────────────────────────────────────────────────

def _fmt(n) -> str:
    if n is None:
        return "N/A"
    return f"{n:,.0f}" if abs(n) >= 1000 else str(n)


def print_analysis(
    ohlcv: pd.DataFrame,
    investor: pd.DataFrame,
    short_df: pd.DataFrame,
    disclosures: list[dict],
    financials: dict,
) -> None:

    print("\n" + "="*70)
    print("  두산에너빌리티(034020)  3~4월 주가 원인 분석")
    print(f"  기간: {START_DATE} ~ {END_DATE}")
    print("="*70)

    # ── 가격 흐름 ──────────────────────────────────────────────────────────────
    if not ohlcv.empty and "close" in ohlcv.columns:
        s_px  = ohlcv["close"].iloc[0]
        e_px  = ohlcv["close"].iloc[-1]
        hi_px = ohlcv["close"].max()
        lo_px = ohlcv["close"].min()
        chg   = (e_px - s_px) / s_px * 100

        hi_dt = ohlcv.loc[ohlcv["close"].idxmax(), "date"].strftime("%m/%d")
        lo_dt = ohlcv.loc[ohlcv["close"].idxmin(), "date"].strftime("%m/%d")

        print(f"\n[1] 가격 흐름")
        print(f"    기초 종가 : {_fmt(s_px)} 원")
        print(f"    기말 종가 : {_fmt(e_px)} 원  ({chg:+.1f}%)")
        print(f"    구간 고점 : {_fmt(hi_px)} 원 ({hi_dt})")
        print(f"    구간 저점 : {_fmt(lo_px)} 원 ({lo_dt})")

        if "volume" in ohlcv.columns:
            avg_vol  = ohlcv["volume"].mean()
            max_vol  = ohlcv["volume"].max()
            max_v_dt = ohlcv.loc[ohlcv["volume"].idxmax(), "date"].strftime("%m/%d")
            print(f"    평균 거래량: {_fmt(avg_vol)}주  / 최대: {_fmt(max_vol)}주 ({max_v_dt})")

        # 급등/급락일 5%+ 하이라이트
        if "chg_pct" in ohlcv.columns:
            big = ohlcv[ohlcv["chg_pct"].abs() >= 4.0].copy()
            if not big.empty:
                print(f"\n    ▶ 주요 급변일 (±4% 이상)")
                for _, r in big.iterrows():
                    print(f"      {r['date'].strftime('%m/%d')}  {r['chg_pct']:+.1f}%  종가 {_fmt(r['close'])}")

    # ── 투자자 매매동향 ────────────────────────────────────────────────────────
    print(f"\n[2] 투자자 매매동향 (조회 컬럼 목록)")
    if not investor.empty:
        print(f"    컬럼: {list(investor.columns)}")
        # 외국인/기관 순매수 컬럼 탐색
        for keyword, label in [
            ("frgn", "외국인"),
            ("orgn", "기관"),
            ("indv", "개인"),
        ]:
            net_cols = [c for c in investor.columns if keyword in c.lower() and "netby" in c.lower()]
            if not net_cols:
                net_cols = [c for c in investor.columns if keyword in c.lower()]
            if net_cols:
                col = net_cols[0]
                try:
                    total = pd.to_numeric(investor[col], errors="coerce").sum()
                    print(f"    {label} 누적 순매수: {_fmt(total)}주  (col={col})")
                except Exception:
                    pass
        print(f"\n    전체 데이터 (최근 5행):")
        print(investor.tail(5).to_string(index=False))
    else:
        print("    데이터 없음 또는 API 미지원")

    # ── 공매도 ────────────────────────────────────────────────────────────────
    print(f"\n[3] 공매도 추이")
    if not short_df.empty:
        print(f"    컬럼: {list(short_df.columns)}")
        print(short_df.to_string(index=False))
    else:
        print("    데이터 없음 또는 API 미지원")

    # ── DART 공시 목록 ────────────────────────────────────────────────────────
    print(f"\n[4] DART 공시 목록 ({len(disclosures)}건)")
    if disclosures:
        for d in disclosures:
            rcept_dt = d.get("rcept_dt", "")[:8]
            dt_str   = f"{rcept_dt[:4]}/{rcept_dt[4:6]}/{rcept_dt[6:8]}" if len(rcept_dt)==8 else rcept_dt
            rpt_nm   = d.get("report_nm", "")
            print(f"    {dt_str}  {rpt_nm}")
    else:
        print("    없음 (DART_API_KEY 미설정 또는 해당 기간 공시 없음)")

    # ── DART 재무 (2024 사업보고서) ───────────────────────────────────────────
    print(f"\n[5] DART 재무 (2024 사업보고서 - 2026.4.1부터 공식 사용 가능)")
    if financials:
        labels = {
            "revenue":     "매출액",
            "op_income":   "영업이익",
            "net_income":  "당기순이익",
            "total_assets":"자산총계",
            "total_debt":  "부채총계",
            "equity":      "자본총계",
            "op_margin":   "영업이익률(%)",
            "debt_ratio":  "부채비율(%)",
            "roe":         "ROE(%)",
            "roa":         "ROA(%)",
        }
        for k, lbl in labels.items():
            v = financials.get(k)
            if v is None:
                continue
            if k in ("op_margin","debt_ratio","roe","roa"):
                print(f"    {lbl:15s}: {v:+.1f}%")
            else:
                print(f"    {lbl:15s}: {_fmt(v)} 원")
    else:
        print("    없음 (API 오류 또는 키 미설정)")


# ── main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    print("두산에너빌리티 데이터 수집 중...")

    # corp_code 조회
    corp_map   = await get_corp_code_map()
    corp_code  = corp_map.get(TICKER, "")
    print(f"  corp_code: {corp_code or '조회 실패'}")

    results = await asyncio.gather(
        fetch_ohlcv(),
        fetch_investor(),
        fetch_short_sale(),
        fetch_dart_disclosures(corp_code) if corp_code else asyncio.sleep(0),
        fetch_dart_financials(corp_code)  if corp_code else asyncio.sleep(0),
        return_exceptions=True,
    )

    def _safe(r, default):
        if isinstance(r, Exception):
            print(f"  [경고] {type(r).__name__}: {r}")
            return default
        return r

    ohlcv       = _safe(results[0], pd.DataFrame())
    investor    = _safe(results[1], pd.DataFrame())
    short_df    = _safe(results[2], pd.DataFrame())
    disclosures = _safe(results[3], []) if corp_code else []
    financials  = _safe(results[4], {}) if corp_code else {}

    print_analysis(ohlcv, investor, short_df, disclosures, financials)


if __name__ == "__main__":
    asyncio.run(main())
