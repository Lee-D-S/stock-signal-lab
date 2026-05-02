from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
OBS_DIR = BASE_DIR / "08_관찰기록"

OBS_CSV = OBS_DIR / "관찰_로그(이상).csv"
SUMMARY_CSV = OBS_DIR / "관찰_성과_요약.csv"
SUMMARY_MD = OBS_DIR / "관찰_성과_요약.md"

RETURN_COLUMNS = [
    "next_close_return_pct",
    "d_plus_5_return_pct",
    "d_plus_10_return_pct",
    "d_plus_20_return_pct",
]

POSITIVE_LABELS = {"상승 지속", "상승 유지", "반등 성공", "단기 반등", "회피 성공"}
NEGATIVE_LABELS = {"상승 실패", "반등 실패", "회피 실패"}
NEUTRAL_LABELS = {"단기 되돌림", "반등 중립", "회피 중립", "중립"}


def read_observations() -> pd.DataFrame:
    if not OBS_CSV.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(OBS_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    except UnicodeDecodeError:
        return pd.read_csv(OBS_CSV, encoding="cp949", dtype={"ticker": str})


def as_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce")


def fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return ""
    return f"{float(value):+.2f}%"


def result_status(sample_count: int, completed_count: int) -> str:
    if sample_count < 20:
        return "표본 부족"
    if completed_count < 20:
        return "D+ 관찰 부족"
    return "검토 가능"


def label_bucket_counts(labels: pd.Series) -> tuple[int, int, int, str]:
    clean = labels.dropna().astype(str).str.strip()
    clean = clean[clean != ""]
    positive = int(clean.isin(POSITIVE_LABELS).sum())
    negative = int(clean.isin(NEGATIVE_LABELS).sum())
    neutral = int(clean.isin(NEUTRAL_LABELS).sum())
    if clean.empty:
        return positive, negative, neutral, ""
    counts = clean.value_counts()
    distribution = ", ".join(f"{label} {count}" for label, count in counts.items())
    return positive, negative, neutral, distribution


def summarize_group(hypothesis_id: str, use_type: str, group: pd.DataFrame) -> dict[str, Any]:
    labels = group["result_label"] if "result_label" in group.columns else pd.Series(dtype=str)
    clean_labels = labels.dropna().astype(str).str.strip()
    clean_labels = clean_labels[clean_labels != ""]
    positive, negative, neutral, distribution = label_bucket_counts(labels)

    row: dict[str, Any] = {
        "hypothesis_id": hypothesis_id,
        "use_type": use_type,
        "sample_count": len(group),
        "completed_count": len(clean_labels),
        "positive_label_count": positive,
        "negative_label_count": negative,
        "neutral_label_count": neutral,
        "result_label_distribution": distribution,
    }

    for column in RETURN_COLUMNS:
        values = to_number(group[column]) if column in group.columns else pd.Series(dtype=float)
        valid = values.dropna()
        prefix = column.replace("_return_pct", "")
        row[f"{prefix}_count"] = len(valid)
        row[f"{prefix}_avg_return_pct"] = round(float(valid.mean()), 2) if not valid.empty else ""
        row[f"{prefix}_median_return_pct"] = round(float(valid.median()), 2) if not valid.empty else ""
        row[f"{prefix}_positive_rate_pct"] = round(float((valid > 0).mean() * 100), 1) if not valid.empty else ""

    row["result_status"] = result_status(int(row["sample_count"]), int(row["completed_count"]))
    return row


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "hypothesis_id",
                "use_type",
                "sample_count",
                "completed_count",
                "next_close_avg_return_pct",
                "d_plus_5_avg_return_pct",
                "d_plus_10_avg_return_pct",
                "d_plus_20_avg_return_pct",
                "positive_label_count",
                "negative_label_count",
                "neutral_label_count",
                "result_status",
                "result_label_distribution",
            ]
        )

    work = df.copy()
    if "hypothesis_id" not in work.columns:
        work["hypothesis_id"] = ""
    if "use_type" not in work.columns:
        work["use_type"] = ""

    rows: list[dict[str, Any]] = []
    for (hypothesis_id, use_type), group in work.groupby(["hypothesis_id", "use_type"], dropna=False):
        rows.append(summarize_group(as_text(hypothesis_id), as_text(use_type), group))

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary
    return summary.sort_values(["result_status", "hypothesis_id", "use_type"]).reset_index(drop=True)


def markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    if df.empty:
        return "_없음_"
    view = df[[column for column in columns if column in df.columns]].copy()
    if view.empty:
        return "_없음_"
    for column in view.columns:
        if column.endswith("_return_pct") or column.endswith("_positive_rate_pct"):
            view[column] = view[column].map(fmt_pct)
    view = view.fillna("")
    lines = [
        "| " + " | ".join(view.columns) + " |",
        "| " + " | ".join("---" for _ in view.columns) + " |",
    ]
    for _, row in view.iterrows():
        lines.append("| " + " | ".join(str(row[column]).replace("|", "\\|") for column in view.columns) + " |")
    return "\n".join(lines)


def build_markdown(source: pd.DataFrame, summary: pd.DataFrame) -> str:
    completed = 0
    if not source.empty and "result_label" in source.columns:
        labels = source["result_label"].dropna().astype(str).str.strip()
        completed = int((labels != "").sum())

    lines = [
        "# 관찰 성과 요약",
        "",
        "## 목적",
        "",
        "관찰 로그에 쌓인 D+1/D+5/D+10/D+20 결과를 조건별로 집계한다. 표본이 20건 미만이면 결론을 내리지 않고 `표본 부족`으로 표시한다.",
        "",
        "## 전체 현황",
        "",
        f"- 누적 관찰 건수: {len(source):,}",
        f"- 결과 라벨 입력 건수: {completed:,}",
        f"- 조건 그룹 수: {len(summary):,}",
        "",
        "## 조건별 성과",
        "",
        markdown_table(
            summary,
            [
                "hypothesis_id",
                "use_type",
                "sample_count",
                "completed_count",
                "next_close_avg_return_pct",
                "d_plus_5_avg_return_pct",
                "d_plus_10_avg_return_pct",
                "d_plus_20_avg_return_pct",
                "positive_label_count",
                "negative_label_count",
                "neutral_label_count",
                "result_status",
                "result_label_distribution",
            ],
        ),
        "",
        "## 해석 기준",
        "",
        "- `sample_count`가 20건 미만이면 조건을 유지하되 판단은 보류한다.",
        "- `completed_count`는 결과 라벨이 채워진 관찰 건수다.",
        "- 평균 수익률은 이벤트 당일 종가 대비 이후 종가 기준이다.",
        "- 회피 조건과 매수/반등 조건은 성공 라벨의 의미가 다르므로 `use_type`을 함께 봐야 한다.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    OBS_DIR.mkdir(parents=True, exist_ok=True)
    observations = read_observations()
    summary = build_summary(observations)
    summary.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    SUMMARY_MD.write_text(build_markdown(observations, summary), encoding="utf-8")
    print(f"observations={len(observations)}")
    print(f"summary_rows={len(summary)}")
    print(f"summary_csv={SUMMARY_CSV}")
    print(f"summary_md={SUMMARY_MD}")


if __name__ == "__main__":
    main()
