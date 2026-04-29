from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
COMPANY_DIR = BASE_DIR / "00_기업별분석"
DATA_DIR = BASE_DIR / "03_원천데이터"
PATTERN_DIR = BASE_DIR / "04_패턴분석"
REVIEW_DIR = BASE_DIR / "05_가설검토"
BACKTEST_DIR = BASE_DIR / "06_백테스트"

EVENTS_CSV = DATA_DIR / "이벤트.csv"
HYPOTHESIS_CSV = PATTERN_DIR / "패턴_가설_후보.csv"

REVIEW_CSV = REVIEW_DIR / "가설_이벤트_검토.csv"
SUMMARY_CSV = REVIEW_DIR / "가설_이벤트_요약.csv"
BACKTEST_INPUT_CSV = BACKTEST_DIR / "가설_백테스트_입력값.csv"
REVIEW_MD = REVIEW_DIR / "가설_이벤트_검토.md"


RETURN_RE = re.compile(
    r"후속 수익률:\s*D\+1\s*([+-]?\d+(?:\.\d+)?%|N/A),\s*"
    r"D\+3\s*([+-]?\d+(?:\.\d+)?%|N/A),\s*"
    r"D\+5\s*([+-]?\d+(?:\.\d+)?%|N/A),\s*"
    r"D\+10\s*([+-]?\d+(?:\.\d+)?%|N/A)"
)


def parse_pct(value: str) -> float | None:
    value = value.strip()
    if value == "N/A":
        return None
    return float(value.replace("%", ""))


def report_path_for(row: pd.Series) -> Path:
    source = Path(str(row["source_file"]))
    company = source.parent.name
    quarter = row["quarter"]
    name = row["name"]
    return COMPANY_DIR / company / f"{name}_{quarter}_원인후보_실제분석.md"


def extract_forward_returns(row: pd.Series) -> dict[str, float | None]:
    path = report_path_for(row)
    if not path.is_file():
        return {"d1_return": None, "d3_return": None, "d5_return": None, "d10_return": None}

    direction_kr = "상승" if row["direction"] == "up" else "하락"
    header = f"### {row['date']} {direction_kr} 이벤트"
    text = path.read_text(encoding="utf-8", errors="ignore")
    start = text.find(header)
    if start < 0:
        return {"d1_return": None, "d3_return": None, "d5_return": None, "d10_return": None}
    next_block = text.find("\n### ", start + len(header))
    block = text[start:] if next_block < 0 else text[start:next_block]
    match = RETURN_RE.search(block)
    if not match:
        return {"d1_return": None, "d3_return": None, "d5_return": None, "d10_return": None}
    d1, d3, d5, d10 = (parse_pct(part) for part in match.groups())
    return {"d1_return": d1, "d3_return": d3, "d5_return": d5, "d10_return": d10}


def match_hypothesis_events(events: pd.DataFrame, hypotheses: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for idx, hyp in hypotheses.iterrows():
        mask = (
            (events["market_regime"] == hyp["market_regime"])
            & (events["direction"] == hyp["direction"])
            & (events["amount_tag"] == hyp["amount_tag"])
            & (events["flow_category"] == hyp["flow_category"])
            & (events["dart_tag"] == hyp["dart_tag"])
            & (events["window_category"] == hyp["window_category"])
        )
        matched = events[mask].copy()
        matched.insert(0, "hypothesis_id", f"H{idx + 1:02d}")
        for col in [
            "dominant_followup",
            "dominant_rate",
            "usable_event_count",
            "company_count",
        ]:
            matched[col] = hyp[col]
        rows.append(matched)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def classify_action(row: pd.Series) -> str:
    direction = row["direction"]
    followup = row["dominant_followup"]
    if direction == "up" and followup in {"상승지속", "상승유지"}:
        return "진입 후보"
    if direction == "up" and followup in {"상승실패", "단기되돌림"}:
        return "추격매수 회피 후보"
    if direction == "down" and followup in {"하락후반등", "단기반등"}:
        return "반등 관찰 후보"
    if direction == "down" and followup in {"하락지속", "하락유지"}:
        return "리스크 회피 후보"
    return "추가 검토"


def summarize(review: pd.DataFrame) -> pd.DataFrame:
    grouped = review.groupby("hypothesis_id", as_index=False)
    summary = grouped.agg(
        market_regime=("market_regime", "first"),
        direction=("direction", "first"),
        amount_tag=("amount_tag", "first"),
        flow_category=("flow_category", "first"),
        dart_tag=("dart_tag", "first"),
        window_category=("window_category", "first"),
        dominant_followup=("dominant_followup", "first"),
        dominant_rate=("dominant_rate", "first"),
        event_count=("event_id", "count"),
        usable_event_count=("has_usable_followup", "sum"),
        company_count=("ticker", "nunique"),
        avg_event_chg_pct=("chg_pct", "mean"),
        avg_d1_return=("d1_return", "mean"),
        avg_d3_return=("d3_return", "mean"),
        avg_d5_return=("d5_return", "mean"),
        avg_d10_return=("d10_return", "mean"),
        external_review_count=("needs_external_review", "sum"),
    )
    summary["action_hint"] = summary.apply(classify_action, axis=1)
    summary["d5_positive_rate"] = review.groupby("hypothesis_id")["d5_return"].apply(lambda s: (s.dropna() > 0).mean()).values
    summary["d10_positive_rate"] = review.groupby("hypothesis_id")["d10_return"].apply(lambda s: (s.dropna() > 0).mean()).values
    return summary


def fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):+.2f}%"


def markdown_table(df: pd.DataFrame, max_rows: int = 30) -> str:
    if df.empty:
        return "_데이터 없음_"
    view = df.head(max_rows).copy()
    for col in view.columns:
        if col.endswith("rate"):
            view[col] = view[col].map(lambda x: "N/A" if pd.isna(x) else f"{x:.2%}")
        elif (col.startswith("avg_") or col in {"chg_pct", "d1_return", "d3_return", "d5_return", "d10_return"}) and pd.api.types.is_numeric_dtype(view[col]):
            view[col] = view[col].map(fmt_pct)
        else:
            view[col] = view[col].map(lambda x: "N/A" if pd.isna(x) else x)
    headers = [str(col) for col in view.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for _, row in view.iterrows():
        values = [str(row[col]).replace("|", "\\|") for col in view.columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def build_backtest_inputs(summary: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "hypothesis_id",
        "market_regime",
        "direction",
        "amount_tag",
        "flow_category",
        "dart_tag",
        "window_category",
        "dominant_followup",
        "action_hint",
    ]
    out = summary[cols].copy()
    out["entry_timing"] = "이벤트 발생 다음 거래일 시가/종가 기준 비교 필요"
    out["hold_days_to_test"] = "5|10|20"
    out["validation_note"] = "후속 라벨은 결과값이므로 진입 조건에는 포함하지 말 것"
    return out


def build_markdown(summary: pd.DataFrame, review: pd.DataFrame) -> str:
    lines = [
        "# 국면별 패턴 후보 이벤트 리뷰",
        "",
        "## 결론",
        "",
        "- 전 기간 공통 패턴은 아직 없고, 현재 검증 대상은 국면별 후보 6개다.",
        "- 상승 이벤트 후보 3개 중 1개는 진입 후보, 2개는 추격매수 회피 후보로 보는 편이 자연스럽다.",
        "- 하락 이벤트 후보 3개는 모두 반등 관찰 후보로 분류된다.",
        "- `needs_external_review=true`가 많은 후보는 내부 KIS/DART 수치만으로 원인 설명이 약하므로 뉴스/외부 요인 확인이 필요하다.",
        "",
        "## 후보별 요약",
        "",
        markdown_table(
            summary[
                [
                    "hypothesis_id",
                    "market_regime",
                    "direction",
                    "amount_tag",
                    "flow_category",
                    "dart_tag",
                    "window_category",
                    "dominant_followup",
                    "dominant_rate",
                    "event_count",
                    "company_count",
                    "avg_d5_return",
                    "avg_d10_return",
                    "d5_positive_rate",
                    "d10_positive_rate",
                    "action_hint",
                    "external_review_count",
                ]
            ]
        ),
        "",
    ]
    for _, hyp in summary.iterrows():
        hid = hyp["hypothesis_id"]
        subset = review[review["hypothesis_id"] == hid].sort_values(["date", "ticker"])
        success = subset[subset["followup"] == hyp["dominant_followup"]]
        failure = subset[(subset["followup"] != hyp["dominant_followup"]) & (subset["has_usable_followup"])]
        lines.extend(
            [
                f"## {hid} 사례",
                "",
                f"- 조건: {hyp['market_regime']} / {hyp['direction']} / {hyp['amount_tag']} / {hyp['flow_category']} / {hyp['dart_tag']} / {hyp['window_category']}",
                f"- 대표 결과: {hyp['dominant_followup']} ({hyp['dominant_rate']:.2%})",
                f"- 해석: {hyp['action_hint']}",
                "",
                "### 성공 사례",
                "",
                markdown_table(
                    success[
                        [
                            "date",
                            "ticker",
                            "name",
                            "quarter",
                            "chg_pct",
                            "followup",
                            "d5_return",
                            "d10_return",
                            "needs_external_review",
                        ]
                    ].head(20)
                ),
                "",
                "### 실패/예외 사례",
                "",
                markdown_table(
                    failure[
                        [
                            "date",
                            "ticker",
                            "name",
                            "quarter",
                            "chg_pct",
                            "followup",
                            "d5_return",
                            "d10_return",
                            "needs_external_review",
                        ]
                    ].head(20)
                ),
                "",
            ]
        )
    return "\n".join(lines)


def main() -> None:
    events = pd.read_csv(EVENTS_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    hypotheses = pd.read_csv(HYPOTHESIS_CSV, encoding="utf-8-sig")
    review = match_hypothesis_events(events, hypotheses)
    if review.empty:
        raise SystemExit("매칭된 후보 이벤트가 없습니다.")

    returns = review.apply(extract_forward_returns, axis=1, result_type="expand")
    review = pd.concat([review, returns], axis=1)
    summary = summarize(review)
    backtest_inputs = build_backtest_inputs(summary)

    review.to_csv(REVIEW_CSV, index=False, encoding="utf-8-sig")
    summary.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    backtest_inputs.to_csv(BACKTEST_INPUT_CSV, index=False, encoding="utf-8-sig")
    REVIEW_MD.write_text(build_markdown(summary, review), encoding="utf-8")

    print(f"review_events={len(review)}")
    print(f"summary_rows={len(summary)}")
    print(f"review_csv={REVIEW_CSV}")
    print(f"summary_csv={SUMMARY_CSV}")
    print(f"backtest_inputs_csv={BACKTEST_INPUT_CSV}")
    print(f"review_md={REVIEW_MD}")


if __name__ == "__main__":
    main()
