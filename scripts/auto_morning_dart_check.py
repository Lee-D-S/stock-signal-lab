"""평일 08:50 자동 실행 — 관찰 종목 DART 공시 확인 후 텔레그램 알림."""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402

DART_API_KEY = settings.dart_api_key
DART_BASE_URL = "https://opendart.fss.or.kr/api"
CORP_CODE_CACHE = ROOT / "data" / "dart_corp_codes.json"
OBS_DIR = ROOT / "ai 주가 변동 원인 분석" / "08_관찰기록"
OBS_UTF8_CSV = OBS_DIR / "관찰_로그(이상).csv"
OBS_CP949_CSV = OBS_DIR / "관찰_로그.csv"
OBS_MD = OBS_DIR / "관찰_로그.md"

POSITIVE_DISCLOSURE_KEYWORDS = [
    "단일판매",
    "공급계약",
    "수주",
    "신규시설투자",
    "투자판단",
    "자기주식취득",
    "현금ㆍ현물배당",
    "현금·현물배당",
    "무상증자",
    "특허권취득",
    "영업(잠정)실적",
    "매출액또는손익구조",
]

NEGATIVE_DISCLOSURE_KEYWORDS = [
    "유상증자",
    "전환사채",
    "신주인수권부사채",
    "감자",
    "횡령",
    "배임",
    "소송",
    "불성실공시",
    "관리종목",
    "거래정지",
    "상장폐지",
    "감사의견",
    "영업정지",
    "회생절차",
]

NEUTRAL_DISCLOSURE_KEYWORDS = [
    "주주총회",
    "기업설명회",
    "임원",
    "최대주주",
    "주식등의대량보유",
]


def safe_print(message: str) -> None:
    encoding = sys.stdout.encoding or "utf-8"
    print(message.encode(encoding, errors="replace").decode(encoding, errors="replace"))


def load_watched_tickers() -> list[tuple[str, str]]:
    """관찰 로그에서 아직 결과 라벨이 비어 있는 종목 (ticker, name) 반환."""
    if OBS_UTF8_CSV.exists():
        df = pd.read_csv(OBS_UTF8_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    elif OBS_CP949_CSV.exists():
        df = pd.read_csv(OBS_CP949_CSV, encoding="cp949", dtype={"ticker": str})
    else:
        return []
    if df.empty or not {"ticker", "name", "result_label"}.issubset(df.columns):
        return []
    active = df[df["result_label"].isna() | (df["result_label"] == "")]
    return list(zip(active["ticker"].str.zfill(6), active["name"]))


def load_observation_frame() -> pd.DataFrame:
    if OBS_UTF8_CSV.exists():
        return pd.read_csv(OBS_UTF8_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    if OBS_CP949_CSV.exists():
        return pd.read_csv(OBS_CP949_CSV, encoding="cp949", dtype={"ticker": str})
    return pd.DataFrame()


def disclosure_note(disclosures: list[dict]) -> str:
    names = []
    for disclosure in disclosures[:3]:
        rcept_dt = str(disclosure.get("rcept_dt", "")).strip()
        report_nm = str(disclosure.get("report_nm", "")).strip()
        if rcept_dt and report_nm:
            names.append(f"{rcept_dt} {report_nm}")
        elif report_nm:
            names.append(report_nm)
    return "오전 DART 공시: " + " / ".join(names)


def classify_disclosure_title(title: str) -> tuple[str, str]:
    normalized = title.replace(" ", "")
    if any(keyword.replace(" ", "") in normalized for keyword in NEGATIVE_DISCLOSURE_KEYWORDS):
        return "악재성/리스크", "negative"
    if any(keyword.replace(" ", "") in normalized for keyword in POSITIVE_DISCLOSURE_KEYWORDS):
        return "호재성/보강", "positive"
    if any(keyword.replace(" ", "") in normalized for keyword in NEUTRAL_DISCLOSURE_KEYWORDS):
        return "중립/확인", "neutral"
    return "미분류/확인필요", "unknown"


def summarize_disclosure_interpretation(disclosures: list[dict]) -> tuple[str, str]:
    labels: list[str] = []
    stances: list[str] = []
    for disclosure in disclosures[:3]:
        report_nm = str(disclosure.get("report_nm", "")).strip()
        label, stance = classify_disclosure_title(report_nm)
        labels.append(label)
        stances.append(stance)

    if "negative" in stances:
        stance = "negative"
        label = "악재성/리스크"
    elif "positive" in stances:
        stance = "positive"
        label = "호재성/보강"
    elif "neutral" in stances:
        stance = "neutral"
        label = "중립/확인"
    else:
        stance = "unknown"
        label = "미분류/확인필요"
    return label, stance


def interpretation_for_observation(row: pd.Series, stance: str) -> str:
    use_type = "" if pd.isna(row.get("use_type")) else str(row.get("use_type"))
    direction = "" if pd.isna(row.get("event_direction")) else str(row.get("event_direction"))

    if stance == "positive":
        if "회피" in use_type:
            return "공시 해석: 회피 조건 약화 가능"
        if direction == "down" or "반등" in use_type:
            return "공시 해석: 반등 후보 보강"
        return "공시 해석: 기존 후보 보강"
    if stance == "negative":
        if "회피" in use_type:
            return "공시 해석: 회피 조건 보강"
        return "공시 해석: 기존 후보 약화/리스크"
    if stance == "neutral":
        return "공시 해석: 방향성 판단 보류"
    return "공시 해석: 제목만으로 판단 불가"


def update_observation_notes(disclosures_by_ticker: dict[str, list[dict]]) -> int:
    if not disclosures_by_ticker:
        return 0
    df = load_observation_frame()
    if df.empty or not {"ticker", "result_label", "review_note"}.issubset(df.columns):
        return 0

    changed = 0
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    active_mask = df["result_label"].isna() | (df["result_label"].astype(str) == "")
    for ticker, disclosures in disclosures_by_ticker.items():
        row_mask = active_mask & (df["ticker"] == ticker)
        for idx in df[row_mask].index:
            label, stance = summarize_disclosure_interpretation(disclosures)
            interpretation = interpretation_for_observation(df.loc[idx], stance)
            note = f"{disclosure_note(disclosures)} [{label}; {interpretation}]"
            old_note = "" if pd.isna(df.at[idx, "review_note"]) else str(df.at[idx, "review_note"])
            if note in old_note:
                continue
            df.at[idx, "review_note"] = f"{old_note}; {note}" if old_note else note
            changed += 1

    if changed:
        df.to_csv(OBS_UTF8_CSV, index=False, encoding="utf-8-sig")
        df.to_csv(OBS_CP949_CSV, index=False, encoding="cp949", errors="replace")
    return changed


def append_markdown_notes(today: datetime, disclosures_by_name: dict[str, list[dict]]) -> None:
    if not disclosures_by_name:
        return
    date_text = today.strftime("%Y-%m-%d")
    heading = f"## {date_text} 오전 DART 공시"
    lines = [heading, ""]
    for name, disclosures in disclosures_by_name.items():
        label, _stance = summarize_disclosure_interpretation(disclosures)
        note = disclosure_note(disclosures)
        lines.append(f"- {name}: {note} [{label}]")
    lines.append("")
    section = "\n".join(lines)

    if OBS_MD.exists():
        text = OBS_MD.read_text(encoding="utf-8-sig")
        if heading in text:
            return
        OBS_MD.write_text(text.rstrip() + "\n\n" + section, encoding="utf-8")
    else:
        OBS_MD.write_text("# 일별 후보 관찰 로그\n\n" + section, encoding="utf-8")


def load_corp_codes() -> dict[str, str]:
    """data/dart_corp_codes.json → {stock_code: corp_code}"""
    if not CORP_CODE_CACHE.exists():
        return {}
    with open(CORP_CODE_CACHE, encoding="utf-8") as f:
        return json.load(f)


async def fetch_disclosures(session: aiohttp.ClientSession, corp_code: str, bgn_de: str, end_de: str) -> list[dict]:
    url = f"{DART_BASE_URL}/list.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": corp_code,
        "bgn_de": bgn_de,
        "end_de": end_de,
        "sort": "date",
        "sort_mth": "desc",
        "page_no": "1",
        "page_count": "10",
    }
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        data = await resp.json(content_type=None)
    if data.get("status") != "000":
        return []
    return data.get("list", [])


async def send_telegram(message: str) -> None:
    token = settings.telegram_bot_token
    chat_id = settings.telegram_chat_id
    if not token or not chat_id:
        safe_print("[telegram] 설정 없음, 출력만 합니다.")
        safe_print(message)
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                print(f"[telegram] 전송 실패: {resp.status}")


async def main() -> None:
    today = datetime.now()
    # 오늘 포함 최근 2 거래일 범위
    bgn = (today - timedelta(days=3)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    tickers = load_watched_tickers()
    if not tickers:
        print("관찰 종목 없음")
        return

    corp_codes = load_corp_codes()
    found: list[str] = []
    disclosures_by_ticker: dict[str, list[dict]] = {}
    disclosures_by_name: dict[str, list[dict]] = {}

    async with aiohttp.ClientSession() as session:
        for ticker, name in tickers:
            corp_code = corp_codes.get(ticker)
            if not corp_code:
                print(f"[{name}] corp_code 없음 (ticker={ticker})")
                continue
            try:
                disclosures = await fetch_disclosures(session, corp_code, bgn, end)
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                print(f"[{name}] DART 조회 실패: {type(exc).__name__}: {exc}")
                continue
            if disclosures:
                label, _stance = summarize_disclosure_interpretation(disclosures)
                lines = [f"📋 <b>{name} ({ticker})</b> 공시 {len(disclosures)}건"]
                lines.append(f"  • 해석: {label}")
                for d in disclosures[:3]:
                    lines.append(f"  • [{d.get('rcept_dt','')}] {d.get('report_nm','')}")
                found.append("\n".join(lines))
                disclosures_by_ticker[ticker] = disclosures
                disclosures_by_name[f"{name} ({ticker})"] = disclosures
            else:
                print(f"[{name}] 신규 공시 없음")
            await asyncio.sleep(0.3)

    updated_notes = update_observation_notes(disclosures_by_ticker)
    append_markdown_notes(today, disclosures_by_name)
    print(f"observation_disclosure_notes_updated={updated_notes}")

    if found:
        msg = f"🔔 <b>오전 공시 확인 ({today.strftime('%Y-%m-%d')})</b>\n\n" + "\n\n".join(found)
    else:
        msg = f"✅ <b>오전 공시 확인 ({today.strftime('%Y-%m-%d')})</b>\n관찰 종목 신규 공시 없음"

    await send_telegram(msg)
    safe_print(msg)


if __name__ == "__main__":
    asyncio.run(main())
