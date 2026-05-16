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

from analysis_paths import (  # noqa: E402
    COMPANY_DIR,
    DATA_DIR,
    PATTERN_DIR,
)
from screener_lib.data import get_kis_other_major_ratios  # noqa: E402

OUT_CSV = DATA_DIR / "기업별_PER_EPS_현재스냅샷.csv"
OUT_MD = PATTERN_DIR / "PER_EPS_밸류에이션_요약.md"

REPORT_RE = re.compile(r"(?P<name>.+)_(?P<year>20\d{2})_Q(?P<quarter>[1-4])_원인후보_실제분석\.md$")
HEADER_RE = re.compile(r"^#\s+.+\((?P<ticker>\d{6})\)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="기업별 PER/EPS 현재 스냅샷과 DART 이익 체력 요약 생성")
    parser.add_argument("--company-dir", type=Path, default=COMPANY_DIR)
    parser.add_argument("--csv", type=Path, default=OUT_CSV)
    parser.add_argument("--md", type=Path, default=OUT_MD)
    parser.add_argument("--refresh-kis-major-ratios", action="store_true", help="Fetch KIS EBITDA/EV/EBITDA when reports do not have the table.")
    parser.add_argument("--delay", type=float, default=0.35)
    return parser.parse_args()


def latest_reports(company_dir: Path) -> list[Path]:
    latest: dict[str, tuple[tuple[int, int], Path]] = {}
    for path in company_dir.glob("*/*_원인후보_실제분석.md"):
        match = REPORT_RE.match(path.name)
        if not match:
            continue
        key = path.parent.name
        period = (int(match.group("year")), int(match.group("quarter")))
        if key not in latest or period > latest[key][0]:
            latest[key] = (period, path)
    return [item[1] for item in sorted(latest.values(), key=lambda item: item[1].parent.name)]


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "").replace("%", "").replace("원", "")
    if not text or text.upper() == "N/A":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_won_text(value: str) -> float | None:
    text = value.strip().replace(",", "")
    if not text or text.upper() == "N/A":
        return None
    sign = -1 if text.startswith("-") else 1
    text = text.lstrip("+-")
    multiplier = 1.0
    if text.endswith("조"):
        multiplier = 1_000_000_000_000
        text = text[:-1]
    elif text.endswith("억"):
        multiplier = 100_000_000
        text = text[:-1]
    try:
        return sign * float(text) * multiplier
    except ValueError:
        return None


def fmt_won(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    eok = float(value) / 100_000_000
    if abs(eok) >= 10_000:
        return f"{eok / 10_000:.2f}조"
    return f"{eok:,.0f}억"


def fmt_num(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):,.{digits}f}"


def table_after_heading(text: str, heading: str) -> list[list[str]]:
    lines = text.splitlines()
    start = None
    for idx, line in enumerate(lines):
        if line.strip() == heading:
            start = idx + 1
            break
    if start is None:
        return []

    table_lines: list[str] = []
    for line in lines[start:]:
        stripped = line.strip()
        if not stripped:
            if table_lines:
                break
            continue
        if stripped.startswith("## ") and table_lines:
            break
        if stripped.startswith("|"):
            table_lines.append(stripped)
        elif table_lines:
            break

    rows: list[list[str]] = []
    for line in table_lines:
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if cells and all(set(cell) <= {"-", ":"} for cell in cells):
            continue
        rows.append(cells)
    return rows


def parse_kis_snapshot(text: str) -> dict[str, float | None]:
    rows = table_after_heading(text, "## KIS 현재 참고 지표")
    values: dict[str, float | None] = {
        "현재가": None,
        "PER": None,
        "PBR": None,
        "EPS": None,
        "BPS": None,
    }
    for row in rows[1:]:
        if len(row) < 2:
            continue
        key = row[0]
        if key in values:
            values[key] = parse_number(row[1])
    return values


def parse_kis_major_ratios(text: str) -> dict[str, float | None]:
    rows = table_after_heading(text, "## KIS 기타 주요 비율")
    values: dict[str, float | None] = {
        "EBITDA": None,
        "EV/EBITDA": None,
        "EVA": None,
        "배당성향": None,
    }
    for row in rows[1:]:
        if len(row) < 2:
            continue
        key = row[0]
        if key in values:
            values[key] = parse_number(row[1])
    return values


def parse_financial_rows(text: str) -> list[dict[str, Any]]:
    rows = table_after_heading(text, "## DART 주요 재무 수치")
    financials: list[dict[str, Any]] = []
    for row in rows[1:]:
        if len(row) < 7:
            continue
        financials.append({
            "기준": row[0],
            "매출액": parse_won_text(row[1]),
            "영업이익": parse_won_text(row[2]),
            "순이익": parse_won_text(row[3]),
            "영업이익률": parse_number(row[4]),
            "부채비율": parse_number(row[5]),
            "ROE": parse_number(row[6]),
        })
    return financials


def extract_ticker(text: str) -> str:
    for line in text.splitlines()[:5]:
        match = HEADER_RE.match(line.strip())
        if match:
            return match.group("ticker")
    return ""


def per_bucket(per: float | None, eps: float | None) -> str:
    if per is None or eps is None or eps <= 0 or per <= 0:
        return "PER무효"
    if per < 10:
        return "저PER"
    if per <= 25:
        return "보통PER"
    if per <= 50:
        return "고PER"
    return "초고PER"


def pbr_bucket(pbr: float | None) -> str:
    if pbr is None or pbr <= 0:
        return "PBR무효"
    if pbr < 0.8:
        return "저PBR"
    if pbr <= 1.5:
        return "보통PBR"
    if pbr <= 3:
        return "고PBR"
    return "초고PBR"


def ev_ebitda_bucket(ev_ebitda: float | None) -> str:
    if ev_ebitda is None or ev_ebitda <= 0:
        return "EV/EBITDA결측"
    if ev_ebitda < 5:
        return "낮은EV/EBITDA"
    if ev_ebitda <= 10:
        return "보통EV/EBITDA"
    if ev_ebitda <= 15:
        return "높은EV/EBITDA"
    return "초고EV/EBITDA"


def eps_status(eps: float | None) -> str:
    if eps is None:
        return "EPS결측"
    if eps < 0:
        return "EPS적자"
    if eps == 0:
        return "EPS무효"
    if eps >= 10_000:
        return "EPS높음"
    if eps >= 1_000:
        return "EPS보통"
    return "EPS낮음"


def net_income_direction(financials: list[dict[str, Any]], years: int = 3) -> tuple[str, float | None, float | None]:
    usable = [row for row in financials if row.get("순이익") is not None]
    if not usable:
        return "순이익결측", None, None
    recent = usable[-years * 4 :]
    latest = recent[-1]["순이익"]
    earliest = recent[0]["순이익"]
    values = [row["순이익"] for row in recent]
    positives = sum(1 for value in values if value is not None and value > 0)
    negatives = sum(1 for value in values if value is not None and value < 0)

    if latest is None or earliest is None:
        return "순이익결측", latest, earliest
    if latest > 0 and earliest > 0 and latest >= earliest * 1.2:
        return "순이익증가", latest, earliest
    if latest < earliest * 0.8:
        return "순이익감소", latest, earliest
    if positives > 0 and negatives > 0:
        return "순이익불안정", latest, earliest
    return "순이익정체", latest, earliest


def profit_quality(direction: str, latest_net: float | None, roe: float | None, op_margin: float | None) -> str:
    if latest_net is None:
        return "판단제한"
    if latest_net <= 0:
        return "이익체력취약"
    if direction == "순이익증가" and (roe or 0) > 8 and (op_margin or 0) > 5:
        return "이익체력양호"
    if direction in {"순이익감소", "순이익불안정"}:
        return "이익체력주의"
    return "이익체력보통"


def valuation_class(per_group: str, eps_group: str, profit_trend: str) -> str:
    if per_group == "PER무효" or eps_group in {"EPS적자", "EPS무효", "EPS결측"}:
        return "PER비교부적합"
    if per_group == "저PER" and profit_trend == "순이익증가":
        return "저평가후보"
    if per_group == "저PER" and profit_trend in {"순이익감소", "순이익불안정"}:
        return "저평가함정가능"
    if per_group in {"고PER", "초고PER"} and profit_trend == "순이익증가":
        return "성장기대반영"
    if per_group in {"고PER", "초고PER"} and profit_trend in {"순이익감소", "순이익정체", "순이익불안정"}:
        return "기대과열가능"
    if eps_group == "EPS높음" and per_group == "보통PER":
        return "이익체력양호후보"
    return "중립"


def ev_ebitda_note(ev_ebitda_group: str, ebitda: float | None) -> str:
    if ebitda is None or ebitda <= 0:
        return "EBITDA 결측 또는 비양수로 EV/EBITDA 비교 제한"
    if ev_ebitda_group == "낮은EV/EBITDA":
        return "영업현금창출력 대비 기업가치 부담 낮음"
    if ev_ebitda_group == "보통EV/EBITDA":
        return "영업현금창출력 대비 기업가치 보통"
    if ev_ebitda_group in {"높은EV/EBITDA", "초고EV/EBITDA"}:
        return "영업현금창출력 대비 기업가치 부담 높음"
    return "EV/EBITDA 비교 제한"


def value_trap_check(valuation: str, profit_trend: str, debt_ratio: float | None, latest_net: float | None) -> str:
    risks = []
    if valuation == "저평가함정가능":
        risks.append("저PER이나 이익 추세 약화")
    if latest_net is not None and latest_net <= 0:
        risks.append("순손실")
    if debt_ratio is not None and debt_ratio >= 200:
        risks.append("부채비율 부담")
    if profit_trend == "순이익불안정":
        risks.append("이익 변동성")
    return ", ".join(risks) if risks else "특이 리스크 제한적"


def memo_for(row: dict[str, Any]) -> str:
    return (
        f"{row['EPS상태']}, {row['PER구간']}, {row['PBR구간']}, {row['EV/EBITDA구간']}. "
        f"최근 순이익 방향은 {row['3년순이익방향']}이며 "
        f"분류는 {row['밸류에이션분류']}."
    )


def analyze_report(path: Path, major_ratio_overrides: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8", errors="replace")
    snapshot = parse_kis_snapshot(text)
    major_ratios = parse_kis_major_ratios(text)
    financials = parse_financial_rows(text)
    ticker = extract_ticker(text)
    company = path.parent.name
    if major_ratio_overrides and ticker in major_ratio_overrides and major_ratios["EV/EBITDA"] is None:
        override = major_ratio_overrides[ticker]
        major_ratios = {
            "EBITDA": parse_number(override.get("ebitda")),
            "EV/EBITDA": parse_number(override.get("ev_ebitda")),
            "EVA": parse_number(override.get("eva")),
            "배당성향": parse_number(override.get("payout_rate")),
        }

    trend, latest_net, earliest_net = net_income_direction(financials)
    latest_fin = next((row for row in reversed(financials) if row.get("순이익") is not None), {})
    per_group = per_bucket(snapshot["PER"], snapshot["EPS"])
    pbr_group = pbr_bucket(snapshot["PBR"])
    ev_ebitda_group = ev_ebitda_bucket(major_ratios["EV/EBITDA"])
    eps_group = eps_status(snapshot["EPS"])
    quality = profit_quality(trend, latest_net, latest_fin.get("ROE"), latest_fin.get("영업이익률"))
    valuation = valuation_class(per_group, eps_group, trend)

    out = {
        "종목명": company,
        "코드": ticker,
        "기준보고서": path.name,
        "현재가": snapshot["현재가"],
        "PER": snapshot["PER"],
        "PBR": snapshot["PBR"],
        "EPS": snapshot["EPS"],
        "BPS": snapshot["BPS"],
        "EBITDA": major_ratios["EBITDA"],
        "EV/EBITDA": major_ratios["EV/EBITDA"],
        "EVA": major_ratios["EVA"],
        "배당성향": major_ratios["배당성향"],
        "PER구간": per_group,
        "PBR구간": pbr_group,
        "EV/EBITDA구간": ev_ebitda_group,
        "EV/EBITDA해석": ev_ebitda_note(ev_ebitda_group, major_ratios["EBITDA"]),
        "EPS상태": eps_group,
        "최근순이익": latest_net,
        "3년전순이익": earliest_net,
        "3년순이익방향": trend,
        "ROE": latest_fin.get("ROE"),
        "영업이익률": latest_fin.get("영업이익률"),
        "부채비율": latest_fin.get("부채비율"),
        "이익체력판단": quality,
        "밸류에이션분류": valuation,
    }
    out["저평가함정체크"] = value_trap_check(valuation, trend, out["부채비율"], latest_net)
    out["해석메모"] = memo_for(out)
    return out


def build_markdown(df: pd.DataFrame) -> str:
    total = len(df)
    class_counts = df["밸류에이션분류"].value_counts().to_dict() if not df.empty else {}
    per_counts = df["PER구간"].value_counts().to_dict() if not df.empty else {}
    pbr_counts = df["PBR구간"].value_counts().to_dict() if not df.empty else {}
    ev_ebitda_counts = df["EV/EBITDA구간"].value_counts().to_dict() if not df.empty else {}

    lines = [
        "# 밸류에이션 요약",
        "",
        "## 전제",
        "",
        "- 이 요약은 각 기업의 최신 보고서에 있는 KIS 현재 참고 지표, KIS 기타 주요 비율, DART 주요 재무 수치를 결합한 현재 단면 분석이다.",
        "- 기존 분기별 보고서의 PER/PBR/EV/EBITDA는 보고서 생성 시점의 현재 스냅샷이므로 과거 분기별 시계열로 해석하지 않는다.",
        "- 과거 이익 추세는 DART 순이익, ROE, 영업이익률로 대체해 판단한다.",
        "- EV/EBITDA는 KIS 기타주요비율 API가 제공한 값을 우선 사용한다. 기존 보고서에 해당 표가 없으면 결측으로 남긴다.",
        "- 업종 평균, 경쟁사 밸류에이션, Forward PER, 예상 EPS 기반 목표가는 별도 데이터 확보 후 확장한다.",
        "",
        "## 전체 요약",
        "",
        f"- 분석 기업 수: {total}개",
        f"- 밸류에이션 분류: {', '.join(f'{k} {v}개' for k, v in class_counts.items()) or 'N/A'}",
        f"- PER 구간: {', '.join(f'{k} {v}개' for k, v in per_counts.items()) or 'N/A'}",
        f"- PBR 구간: {', '.join(f'{k} {v}개' for k, v in pbr_counts.items()) or 'N/A'}",
        f"- EV/EBITDA 구간: {', '.join(f'{k} {v}개' for k, v in ev_ebitda_counts.items()) or 'N/A'}",
        "",
        "## 분류별 주요 종목",
        "",
    ]

    for label in ["저평가후보", "저평가함정가능", "성장기대반영", "기대과열가능", "이익체력양호후보", "PER비교부적합"]:
        subset = df[df["밸류에이션분류"] == label].copy()
        lines.extend([f"### {label}", ""])
        if subset.empty:
            lines.extend(["- 해당 종목 없음", ""])
            continue
        subset = subset.sort_values(["PER", "EPS"], ascending=[True, False], na_position="last").head(15)
        lines.extend([
            "| 종목 | 코드 | PER | PBR | EV/EBITDA | EPS | 최근순이익 | 3년순이익방향 | ROE | 영업이익률 | 저평가함정체크 |",
            "|---|---|---:|---:|---:|---:|---:|---|---:|---:|---|",
        ])
        for _, row in subset.iterrows():
            lines.append(
                f"| {row['종목명']} | {row['코드']} | {fmt_num(row['PER'])} | {fmt_num(row['PBR'])} | "
                f"{fmt_num(row['EV/EBITDA'])} | {fmt_num(row['EPS'], 0)} | "
                f"{fmt_won(row['최근순이익'])} | {row['3년순이익방향']} | {fmt_num(row['ROE'])}% | "
                f"{fmt_num(row['영업이익률'])}% | {row['저평가함정체크']} |"
            )
        lines.append("")

    lines.extend([
        "## 해석 기준",
        "",
        "| 조건 | 해석 |",
        "|---|---|",
        "| EPS > 0 + PER 낮음 + 순이익 증가 | 저평가 후보 |",
        "| EPS > 0 + PER 낮음 + 순이익 감소 | 저PER이나 저평가 함정 가능성 |",
        "| EPS > 0 + PER 높음 + 순이익 증가 | 성장 기대 반영 후보 |",
        "| EPS > 0 + PER 높음 + 순이익 정체/감소 | 기대 과열 가능성 |",
        "| PBR 낮음 | 장부가치 대비 가격 부담 낮음. 단, 자산 부실/ROE 저하 여부 확인 필요 |",
        "| EV/EBITDA 낮음 | 부채 포함 기업가치가 영업현금창출력 대비 낮음. 단, CAPEX와 업종 평균 확인 필요 |",
        "| EPS <= 0 또는 PER <= 0 | PER 비교 부적합 또는 적자 리스크 후보 |",
        "",
    ])
    return "\n".join(lines)


async def fetch_major_ratio_overrides(tickers: list[str], delay: float) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for ticker in tickers:
        row = await get_kis_other_major_ratios(ticker)
        if row:
            out[ticker] = row
        await asyncio.sleep(delay)
    return out


def main() -> None:
    args = parse_args()
    reports = latest_reports(args.company_dir)
    overrides: dict[str, dict[str, Any]] = {}
    if args.refresh_kis_major_ratios:
        tickers = [extract_ticker(path.read_text(encoding="utf-8", errors="replace")) for path in reports]
        overrides = asyncio.run(fetch_major_ratio_overrides([ticker for ticker in tickers if ticker], args.delay))
        print(f"major_ratio_overrides={len(overrides)}")
    rows = [analyze_report(path, overrides) for path in reports]
    df = pd.DataFrame(rows)

    args.csv.parent.mkdir(parents=True, exist_ok=True)
    args.md.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.csv, index=False, encoding="utf-8-sig")
    args.md.write_text(build_markdown(df), encoding="utf-8")
    print(f"reports={len(reports)} csv={args.csv} md={args.md}")


if __name__ == "__main__":
    main()
