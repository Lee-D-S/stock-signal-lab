from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
REVIEW_DIR = BASE_DIR / "05_가설검토"
BACKTEST_DIR = BASE_DIR / "06_백테스트"

REVIEW_CSV = REVIEW_DIR / "가설_이벤트_검토.csv"
SUMMARY_CSV = REVIEW_DIR / "가설_이벤트_요약.csv"

PROXY_CSV = BACKTEST_DIR / "가설_대리_백테스트.csv"
PROXY_MD = BACKTEST_DIR / "가설_대리_백테스트.md"


def score_row(row: pd.Series, horizon: str) -> float | None:
    raw = row[f"{horizon}_return"]
    if pd.isna(raw):
        return None
    value = float(raw)
    action = row["action_hint"]
    if action in {"진입 후보", "반등 관찰 후보"}:
        return value
    if action == "추격매수 회피 후보":
        return -value
    if action == "리스크 회피 후보":
        return -value
    return None


def hit_row(row: pd.Series, horizon: str) -> bool | None:
    score = score_row(row, horizon)
    if score is None:
        return None
    return score > 0


def fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):+.2f}%"


def fmt_rate(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.2%}"


def summarize(review: pd.DataFrame, base_summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for hid, group in review.groupby("hypothesis_id"):
        base = base_summary[base_summary["hypothesis_id"] == hid].iloc[0].to_dict()
        group = group.copy()
        group["action_hint"] = base["action_hint"]
        row = {
            "hypothesis_id": hid,
            "market_regime": base["market_regime"],
            "direction": base["direction"],
            "dominant_followup": base["dominant_followup"],
            "action_hint": base["action_hint"],
            "event_count": len(group),
            "company_count": group["ticker"].nunique(),
            "external_review_count": int(group["needs_external_review"].sum()),
        }
        for horizon in ("d5", "d10"):
            scores = group.apply(lambda r: score_row(r, horizon), axis=1).dropna()
            hits = group.apply(lambda r: hit_row(r, horizon), axis=1).dropna()
            row[f"{horizon}_sample_count"] = len(scores)
            row[f"{horizon}_avg_score"] = scores.mean() if len(scores) else None
            row[f"{horizon}_median_score"] = scores.median() if len(scores) else None
            row[f"{horizon}_hit_rate"] = hits.mean() if len(hits) else None
            row[f"{horizon}_worst_score"] = scores.min() if len(scores) else None
            row[f"{horizon}_best_score"] = scores.max() if len(scores) else None
        rows.append(row)
    out = pd.DataFrame(rows)
    return out.sort_values(["d10_avg_score", "d5_avg_score"], ascending=[False, False]).reset_index(drop=True)


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_데이터 없음_"
    view = df.copy()
    for col in view.columns:
        if col.endswith("_rate"):
            view[col] = view[col].map(fmt_rate)
        elif col.endswith("_score"):
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


def build_markdown(proxy: pd.DataFrame) -> str:
    lines = [
        "# 국면별 후보 프록시 백테스트",
        "",
        "## 해석 기준",
        "",
        "- 이 검증은 이벤트 당일 종가 기준 `D+5/D+10` 후속 수익률을 사용한 1차 프록시다.",
        "- `진입 후보`와 `반등 관찰 후보`는 후속 수익률이 플러스면 성공으로 봤다.",
        "- `추격매수 회피 후보`는 후속 수익률이 마이너스일수록 회피 효과가 큰 것으로 보고 부호를 반대로 점수화했다.",
        "- 실제 매매 검증은 다음 거래일 시가/종가 진입, 거래비용, 중복 신호 처리, 종목별 최대 보유 수를 별도로 반영해야 한다.",
        "",
        "## 결과 요약",
        "",
        markdown_table(
            proxy[
                [
                    "hypothesis_id",
                    "market_regime",
                    "direction",
                    "dominant_followup",
                    "action_hint",
                    "event_count",
                    "company_count",
                    "d5_sample_count",
                    "d5_avg_score",
                    "d5_hit_rate",
                    "d10_sample_count",
                    "d10_avg_score",
                    "d10_hit_rate",
                    "external_review_count",
                ]
            ]
        ),
        "",
        "## 1차 결론",
        "",
        "- H01은 진입 후보 중 가장 강하다. D+5와 D+10 모두 평균 점수와 적중률이 높다.",
        "- H02와 H03은 반등 관찰 후보로 유효해 보이나, H02는 외부 자료 확인 필요 이벤트가 전부라 뉴스/업황 검토 없이는 자동화하기 어렵다.",
        "- H04와 H06은 상승 추격매수 회피 후보로 의미가 있다. 특히 H06은 D+10 기준 회피 점수가 좋다.",
        "- H05는 D+5 반등은 보이지만 D+10 점수가 약해 짧은 반등 관찰 후보로만 보는 편이 맞다.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    review = pd.read_csv(REVIEW_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    base_summary = pd.read_csv(SUMMARY_CSV, encoding="utf-8-sig")
    proxy = summarize(review, base_summary)
    proxy.to_csv(PROXY_CSV, index=False, encoding="utf-8-sig")
    PROXY_MD.write_text(build_markdown(proxy), encoding="utf-8")
    print(f"proxy_rows={len(proxy)}")
    print(f"proxy_csv={PROXY_CSV}")
    print(f"proxy_md={PROXY_MD}")


if __name__ == "__main__":
    main()
