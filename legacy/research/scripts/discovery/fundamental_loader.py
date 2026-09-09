"""DART 재무 히스토리 로더.

다연도 DART 재무 데이터를 수집하고, point-in-time 조인으로
records DataFrame에 펀더멘털 컬럼을 추가한다.

캐시: data/dart_fundamental_cache/fund_{year}.parquet

Look-ahead bias 방지 원칙
    FY(Y) 사업보고서는 Y+1년 4월 1일 이후 records에만 조인한다.
    (사업보고서 법정 제출 기한 = 결산일로부터 90일)
    pd.merge_asof(direction="backward") 로 날짜 기준 직전 공시 데이터 사용.

Usage:
    from discovery.fundamental_loader import load_dart_history, enrich_records_with_fundamentals

    fund_df = await load_dart_history(tickers, start_year=2019, end_year=2024)
    enriched = enrich_records_with_fundamentals(records, fund_df)
    # enriched에 roe, roa, op_margin, debt_ratio 컬럼 추가됨
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from screener_lib.dart import DART_API_KEY, DART_BASE_URL, get_corp_code_map  # noqa: E402

try:
    import aiohttp as _aiohttp
except ImportError:
    _aiohttp = None  # type: ignore

FUND_CACHE_DIR = ROOT / "data" / "dart_fundamental_cache"
_REPRT_CODE    = "11011"  # 사업보고서
_BATCH_SIZE    = 100
_API_DELAY     = 0.35

_TARGET_ACCOUNTS: dict[str, str] = {
    "매출액":       "revenue",
    "수익(매출액)":  "revenue",
    "영업이익":     "op_income",
    "당기순이익":   "net_income",
    "자산총계":     "total_assets",
    "부채총계":     "total_debt",
    "자본총계":     "equity",
}

FUND_COLS = ["roe", "roa", "op_margin", "debt_ratio"]


# ── 내부 헬퍼 ─────────────────────────────────────────────────────────────────

def _cache_paths(year: int) -> tuple[Path, Path]:
    return (
        FUND_CACHE_DIR / f"fund_{year}.parquet",
        FUND_CACHE_DIR / f"fund_{year}.pkl",
    )


def _read_cache(year: int) -> pd.DataFrame | None:
    parquet_path, pickle_path = _cache_paths(year)
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except ImportError:
            pass
    if pickle_path.exists():
        return pd.read_pickle(pickle_path)
    return None


def _write_cache(df: pd.DataFrame, year: int) -> None:
    parquet_path, pickle_path = _cache_paths(year)
    try:
        df.to_parquet(parquet_path, index=False)
    except ImportError:
        df.to_pickle(pickle_path)

def _available_from(year: int) -> pd.Timestamp:
    """FY(year) 사업보고서 공시 가능 최조 시점 (보수적 추정: year+1년 4월 1일)."""
    return pd.Timestamp(f"{year + 1}-04-01")


def _derive(raw: dict) -> dict:
    """계정 원재료에서 파생 비율 지표 계산."""
    rev    = raw.get("revenue")
    op     = raw.get("op_income")
    net    = raw.get("net_income")
    eq     = raw.get("equity")
    dbt    = raw.get("total_debt")
    assets = raw.get("total_assets")

    if rev and op:     raw["op_margin"]  = round(op  / rev    * 100, 2)
    if eq  and dbt:    raw["debt_ratio"] = round(dbt / eq     * 100, 2)
    if eq  and net:    raw["roe"]        = round(net / eq     * 100, 2)
    if assets and net: raw["roa"]        = round(net / assets * 100, 2)
    return raw


async def _fetch_one_year(
    year: int,
    corp_codes: dict[str, str],    # ticker → corp_code
    ticker_by_corp: dict[str, str],
) -> list[dict]:
    """DART fnlttMultiAcnt API로 한 해 재무 데이터 수집."""
    if not DART_API_KEY or _aiohttp is None:
        return []

    raw: dict[str, dict] = {}

    async with _aiohttp.ClientSession() as session:
        codes = list(corp_codes.values())
        for i in range(0, len(codes), _BATCH_SIZE):
            batch = codes[i : i + _BATCH_SIZE]
            try:
                async with session.get(
                    f"{DART_BASE_URL}/fnlttMultiAcnt.json",
                    params={
                        "crtfc_key":  DART_API_KEY,
                        "corp_code":  ",".join(batch),
                        "bsns_year":  str(year),
                        "reprt_code": _REPRT_CODE,
                    },
                    timeout=_aiohttp.ClientTimeout(total=30),
                ) as resp:
                    data = await resp.json(content_type=None)
            except Exception:
                continue

            if data.get("status") != "000":
                continue

            for row in data.get("list", []):
                corp_code  = row.get("corp_code", "")
                account_nm = (row.get("account_nm") or "").strip()
                fs_div     = row.get("fs_div", "OFS")
                ticker     = ticker_by_corp.get(corp_code)

                if not ticker or account_nm not in _TARGET_ACCOUNTS:
                    continue

                entry = raw.setdefault(ticker, {})
                # 연결재무제표(CFS) 우선, 이미 CFS면 별도(OFS) 무시
                if fs_div == "OFS" and entry.get("_fs_div") == "CFS":
                    continue

                amount_str = (row.get("thstrm_amount") or "").replace(",", "")
                try:
                    entry[_TARGET_ACCOUNTS[account_nm]] = int(amount_str)
                    entry["_fs_div"] = fs_div
                except ValueError:
                    continue

            await asyncio.sleep(_API_DELAY)

    rows_out = []
    for ticker, d in raw.items():
        d.pop("_fs_div", None)
        d = _derive(d)
        rows_out.append({
            "ticker":         ticker,
            "year":           year,
            "available_from": _available_from(year),
            **{c: d.get(c) for c in FUND_COLS},
        })
    return rows_out


# ── 공개 API ──────────────────────────────────────────────────────────────────

async def load_dart_history(
    tickers: list[str],
    start_year: int,
    end_year: int,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """tickers의 start_year~end_year 연도별 DART 재무 데이터 로드.

    캐시 히트 시 API 미호출. 캐시에 없는 종목만 보충 조회.

    Returns:
        DataFrame(ticker, year, available_from, roe, roa, op_margin, debt_ratio)
        DART_API_KEY 미설정 또는 오류 시 빈 DataFrame 반환.
    """
    if not DART_API_KEY:
        print("[fund_loader] DART_API_KEY 미설정 — 재무 데이터 건너뜀")
        return pd.DataFrame()

    FUND_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    corp_map       = await get_corp_code_map()
    corp_codes     = {t: corp_map[t] for t in tickers if t in corp_map}
    ticker_by_corp = {v: k for k, v in corp_codes.items()}

    all_rows: list[dict] = []

    for year in range(start_year, end_year + 1):
        cached = None if force_refresh else _read_cache(year)

        if cached is not None:
            cached_tickers = set(cached["ticker"])
            missing        = [t for t in tickers if t in corp_codes and t not in cached_tickers]

            if not missing:
                all_rows.extend(cached.to_dict("records"))
                print(f"[fund_loader] {year}년 캐시 히트 ({len(cached)}개 종목)")
                continue

            print(f"[fund_loader] {year}년 {len(missing)}개 종목 추가 조회 중...")
            extra_corp    = {t: corp_codes[t] for t in missing}
            extra_by_corp = {v: k for k, v in extra_corp.items()}
            new_rows = await _fetch_one_year(year, extra_corp, extra_by_corp)
            if new_rows:
                merged_df = (
                    pd.concat([cached, pd.DataFrame(new_rows)])
                    .drop_duplicates(["ticker", "year"])
                )
                _write_cache(merged_df, year)
                all_rows.extend(merged_df.to_dict("records"))
            else:
                all_rows.extend(cached.to_dict("records"))
        else:
            print(f"[fund_loader] {year}년 DART 조회 중 ({len(corp_codes)}개 종목)...")
            rows = await _fetch_one_year(year, corp_codes, ticker_by_corp)
            if rows:
                df = pd.DataFrame(rows)
                _write_cache(df, year)
                all_rows.extend(rows)
                print(f"[fund_loader] {year}년 완료 ({len(rows)}개 종목)")
            else:
                print(f"[fund_loader] {year}년 데이터 없음")

    if not all_rows:
        return pd.DataFrame()

    fund_df = pd.DataFrame(all_rows)
    fund_df["available_from"] = pd.to_datetime(fund_df["available_from"])
    return fund_df


def enrich_records_with_fundamentals(
    records: pd.DataFrame,
    fund_df: pd.DataFrame,
) -> pd.DataFrame:
    """records의 각 (ticker, date) 행에 point-in-time 재무 데이터를 조인한다.

    available_from <= date인 가장 최근 연도 데이터를 사용 (look-ahead bias 없음).
    재무 데이터가 없는 행은 해당 컬럼이 NaN.

    Args:
        records:  (ticker, date, ...) DataFrame
        fund_df:  load_dart_history() 반환값

    Returns:
        records에 roe, roa, op_margin, debt_ratio 컬럼이 추가된 DataFrame
        (원본 행 순서와 달라질 수 있음 — date 기준 정렬됨)
    """
    if fund_df.empty:
        return records

    fund_cols_avail = [c for c in FUND_COLS if c in fund_df.columns]
    if not fund_cols_avail:
        return records

    records = records.copy()
    records["date"] = pd.to_datetime(records["date"])

    fund_sub = (
        fund_df[["ticker", "available_from"] + fund_cols_avail]
        .copy()
        .sort_values("available_from")
    )

    # merge_asof: 날짜 기준 직전 공시 데이터를 ticker별로 조인
    merged = pd.merge_asof(
        records.sort_values("date"),
        fund_sub,
        by="ticker",
        left_on="date",
        right_on="available_from",
        direction="backward",
    ).drop(columns=["available_from"], errors="ignore")

    return merged
