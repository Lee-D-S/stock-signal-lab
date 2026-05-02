"""평일 08:50 자동 실행 — 관찰 종목 DART 공시 확인 후 텔레그램 알림."""
from __future__ import annotations

import asyncio
import io
import json
import re
import sys
import zipfile
from datetime import datetime, timedelta
from html import escape, unescape
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
    "자기주식취득",
    "현금ㆍ현물배당",
    "현금·현물배당",
    "무상증자",
    "특허권취득",
]

NEGATIVE_DISCLOSURE_KEYWORDS = [
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

REVIEW_REQUIRED_DISCLOSURE_KEYWORDS = [
    "영업(잠정)실적",
    "잠정실적",
    "매출액또는손익구조",
    "매출액 또는 손익구조",
    "투자판단",
    "유상증자",
    "전환사채",
    "신주인수권부사채",
    "최대주주",
    "타법인주식",
    "자산양수도",
    "합병",
    "분할",
]

NEUTRAL_DISCLOSURE_KEYWORDS = [
    "주주총회",
    "기업설명회",
    "임원",
    "주식등의대량보유",
]

EVIDENCE_KEYWORDS = [
    "매출액",
    "영업이익",
    "당기순이익",
    "계약금액",
    "최근매출액",
    "매출액대비",
    "발행금액",
    "자금조달",
    "시설자금",
    "운영자금",
    "채무상환",
    "전환가액",
    "증가",
    "감소",
    "흑자전환",
    "적자전환",
]

PERFORMANCE_KEYWORDS = [
    "영업(잠정)실적",
    "잠정실적",
    "매출액또는손익구조",
    "매출액 또는 손익구조",
]

CONTRACT_KEYWORDS = ["단일판매", "공급계약", "수주"]
FINANCING_KEYWORDS = ["유상증자", "전환사채", "신주인수권부사채"]
NEGATIVE_EVIDENCE_WORDS = ["감소", "적자전환", "적자지속", "손실", "하락"]
POSITIVE_EVIDENCE_WORDS = ["증가", "흑자전환", "흑자지속", "개선", "계약금액", "수주"]


def normalize_title(text: str) -> str:
    return text.replace(" ", "")


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
    normalized = normalize_title(title)
    if any(keyword.replace(" ", "") in normalized for keyword in REVIEW_REQUIRED_DISCLOSURE_KEYWORDS):
        return "본문확인필요", "review_required"
    if any(keyword.replace(" ", "") in normalized for keyword in NEGATIVE_DISCLOSURE_KEYWORDS):
        return "악재성/리스크", "negative"
    if any(keyword.replace(" ", "") in normalized for keyword in POSITIVE_DISCLOSURE_KEYWORDS):
        return "호재성/보강", "positive"
    if any(keyword.replace(" ", "") in normalized for keyword in NEUTRAL_DISCLOSURE_KEYWORDS):
        return "중립/확인", "neutral"
    return "미분류/확인필요", "unknown"


def strip_xml_text(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def decode_document_payload(data: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            parts = []
            for name in archive.namelist():
                if name.lower().endswith((".xml", ".html", ".htm", ".txt")):
                    raw = archive.read(name)
                    parts.append(raw.decode("utf-8", errors="ignore"))
            if parts:
                return strip_xml_text(" ".join(parts))
    except zipfile.BadZipFile:
        pass
    for encoding in ("utf-8", "cp949", "euc-kr"):
        try:
            return strip_xml_text(data.decode(encoding))
        except UnicodeDecodeError:
            continue
    return strip_xml_text(data.decode("utf-8", errors="ignore"))


async def fetch_document_text(session: aiohttp.ClientSession, rcept_no: str) -> str:
    if not rcept_no:
        return ""
    url = f"{DART_BASE_URL}/document.xml"
    params = {"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
        data = await resp.read()
    return decode_document_payload(data)


def extract_keyword_evidence(text: str, max_items: int = 4) -> list[str]:
    if not text:
        return []
    compact = re.sub(r"\s+", " ", text)
    evidence: list[str] = []
    for keyword in EVIDENCE_KEYWORDS:
        idx = compact.find(keyword)
        if idx < 0:
            continue
        start = max(0, idx - 35)
        end = min(len(compact), idx + 95)
        snippet = compact[start:end].strip(" :;,.")
        if snippet and snippet not in evidence:
            evidence.append(snippet)
        if len(evidence) >= max_items:
            break
    return evidence


def stance_from_evidence(title: str, evidence_text: str) -> tuple[str, str]:
    normalized = normalize_title(title)
    if any(keyword.replace(" ", "") in normalized for keyword in CONTRACT_KEYWORDS):
        return "계약/수주 수치확인", "positive"
    if any(keyword.replace(" ", "") in normalized for keyword in FINANCING_KEYWORDS):
        return "자금조달/희석 확인", "review_required"
    if any(keyword.replace(" ", "") in normalized for keyword in PERFORMANCE_KEYWORDS):
        negative = any(word in evidence_text for word in NEGATIVE_EVIDENCE_WORDS)
        positive = any(word in evidence_text for word in POSITIVE_EVIDENCE_WORDS)
        if positive and not negative:
            return "실적수치 개선", "positive"
        if negative and not positive:
            return "실적수치 악화", "negative"
        if positive and negative:
            return "실적수치 혼재", "review_required"
        return "실적수치 확인필요", "review_required"
    return classify_disclosure_title(title)


async def analyze_disclosure_detail(session: aiohttp.ClientSession, disclosure: dict) -> dict[str, str]:
    title = str(disclosure.get("report_nm", "")).strip()
    title_label, title_stance = classify_disclosure_title(title)
    rcept_no = str(disclosure.get("rcept_no", "")).strip()

    should_fetch_document = title_stance == "review_required" or any(
        keyword.replace(" ", "") in normalize_title(title)
        for keyword in [*PERFORMANCE_KEYWORDS, *CONTRACT_KEYWORDS, *FINANCING_KEYWORDS]
    )
    if not should_fetch_document:
        return {"label": title_label, "stance": title_stance, "evidence": ""}

    try:
        text = await fetch_document_text(session, rcept_no)
    except (asyncio.TimeoutError, aiohttp.ClientError, zipfile.BadZipFile) as exc:
        return {
            "label": "본문조회실패",
            "stance": "review_required",
            "evidence": f"본문 조회 실패: {type(exc).__name__}",
        }

    evidence = extract_keyword_evidence(text)
    evidence_text = " / ".join(evidence)
    label, stance = stance_from_evidence(title, evidence_text)
    if evidence_text:
        return {"label": label, "stance": stance, "evidence": evidence_text}
    return {"label": label, "stance": stance, "evidence": "본문 수치 키워드 추출 실패"}


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
    elif "review_required" in stances:
        stance = "review_required"
        label = "본문확인필요"
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


def summarize_detail_interpretation(analyses: list[dict[str, str]]) -> tuple[str, str, str]:
    labels = [analysis.get("label", "") for analysis in analyses if analysis.get("label")]
    stances = [analysis.get("stance", "") for analysis in analyses if analysis.get("stance")]
    evidence = [analysis.get("evidence", "") for analysis in analyses if analysis.get("evidence")]

    if "negative" in stances:
        stance = "negative"
        label = "악재성/리스크"
    elif "review_required" in stances:
        stance = "review_required"
        label = next((item for item in labels if item), "본문확인필요")
    elif "positive" in stances:
        stance = "positive"
        label = "호재성/보강"
    elif "neutral" in stances:
        stance = "neutral"
        label = "중립/확인"
    else:
        stance = "unknown"
        label = "미분류/확인필요"
    return label, stance, " / ".join(evidence[:3])


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
    if stance == "review_required":
        return "공시 해석: 본문 수치/맥락 확인 전 판단 보류"
    return "공시 해석: 제목만으로 판단 불가"


def update_observation_notes(
    disclosures_by_ticker: dict[str, list[dict]],
    analyses_by_ticker: dict[str, list[dict[str, str]]] | None = None,
) -> int:
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
            analyses = (analyses_by_ticker or {}).get(ticker, [])
            if analyses:
                label, stance, evidence = summarize_detail_interpretation(analyses)
            else:
                label, stance = summarize_disclosure_interpretation(disclosures)
                evidence = ""
            interpretation = interpretation_for_observation(df.loc[idx], stance)
            evidence_note = f"; 근거: {evidence}" if evidence else ""
            note = f"{disclosure_note(disclosures)} [{label}; {interpretation}{evidence_note}]"
            old_note = "" if pd.isna(df.at[idx, "review_note"]) else str(df.at[idx, "review_note"])
            if note in old_note:
                continue
            df.at[idx, "review_note"] = f"{old_note}; {note}" if old_note else note
            changed += 1

    if changed:
        df.to_csv(OBS_UTF8_CSV, index=False, encoding="utf-8-sig")
        df.to_csv(OBS_CP949_CSV, index=False, encoding="cp949", errors="replace")
    return changed


def append_markdown_notes(
    today: datetime,
    disclosures_by_name: dict[str, list[dict]],
    analyses_by_name: dict[str, list[dict[str, str]]] | None = None,
) -> None:
    if not disclosures_by_name:
        return
    date_text = today.strftime("%Y-%m-%d")
    heading = f"## {date_text} 오전 DART 공시"
    lines = [heading, ""]
    for name, disclosures in disclosures_by_name.items():
        analyses = (analyses_by_name or {}).get(name, [])
        if analyses:
            label, _stance, evidence = summarize_detail_interpretation(analyses)
        else:
            label, _stance = summarize_disclosure_interpretation(disclosures)
            evidence = ""
        note = disclosure_note(disclosures)
        evidence_note = f" 근거: {evidence}" if evidence else ""
        lines.append(f"- {name}: {note} [{label}]{evidence_note}")
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
    analyses_by_ticker: dict[str, list[dict[str, str]]] = {}
    analyses_by_name: dict[str, list[dict[str, str]]] = {}

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
                analyses = []
                for disclosure in disclosures[:3]:
                    analyses.append(await analyze_disclosure_detail(session, disclosure))
                    await asyncio.sleep(0.2)
                label, _stance, evidence = summarize_detail_interpretation(analyses)
                lines = [f"📋 <b>{escape(name)} ({ticker})</b> 공시 {len(disclosures)}건"]
                lines.append(f"  • 해석: {escape(label)}")
                if evidence:
                    lines.append(f"  • 근거: {escape(evidence[:450])}")
                for d in disclosures[:3]:
                    lines.append(f"  • [{escape(str(d.get('rcept_dt','')))}] {escape(str(d.get('report_nm','')))}")
                found.append("\n".join(lines))
                disclosures_by_ticker[ticker] = disclosures
                display_name = f"{name} ({ticker})"
                disclosures_by_name[display_name] = disclosures
                analyses_by_ticker[ticker] = analyses
                analyses_by_name[display_name] = analyses
            else:
                print(f"[{name}] 신규 공시 없음")
            await asyncio.sleep(0.3)

    updated_notes = update_observation_notes(disclosures_by_ticker, analyses_by_ticker)
    append_markdown_notes(today, disclosures_by_name, analyses_by_name)
    print(f"observation_disclosure_notes_updated={updated_notes}")

    if found:
        msg = f"🔔 <b>오전 공시 확인 ({today.strftime('%Y-%m-%d')})</b>\n\n" + "\n\n".join(found)
    else:
        msg = f"✅ <b>오전 공시 확인 ({today.strftime('%Y-%m-%d')})</b>\n관찰 종목 신규 공시 없음"

    await send_telegram(msg)
    safe_print(msg)


if __name__ == "__main__":
    asyncio.run(main())
