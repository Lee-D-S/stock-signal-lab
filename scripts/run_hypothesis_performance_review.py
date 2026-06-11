from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

from analysis_paths import OBS_COMMON_CSV, SNAPSHOT_DIR  # noqa: E402

RETURN_COLUMNS = [
    "next_close_return_pct",
    "d_plus_5_return_pct",
    "d_plus_10_return_pct",
    "d_plus_20_return_pct",
]
NUMERIC_COLUMNS = [
    "event_close",
    "next_open",
    "next_close",
    "d_plus_5_close",
    "d_plus_10_close",
    "d_plus_20_close",
    "event_chg_pct",
    "foreign_5d",
    "institution_5d",
    *RETURN_COLUMNS,
]


def read_observations(path: Path) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "cp949"):
        try:
            df = pd.read_csv(path, encoding=encoding, dtype={"ticker": str})
            break
        except UnicodeDecodeError:
            continue
    else:
        raise UnicodeError(f"unsupported encoding: {path}")

    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    return df


def metric_row(hypothesis_id: str, segment: str, group: pd.DataFrame) -> dict[str, Any]:
    row: dict[str, Any] = {
        "hypothesis_id": hypothesis_id,
        "segment": segment,
        "sample_count": len(group),
        "unique_ticker_count": group["ticker"].nunique(),
    }
    for column in RETURN_COLUMNS:
        values = group[column].dropna()
        prefix = column.replace("_return_pct", "")
        row[f"{prefix}_count"] = len(values)
        row[f"{prefix}_avg_return_pct"] = round(float(values.mean()), 2) if not values.empty else ""
        row[f"{prefix}_median_return_pct"] = round(float(values.median()), 2) if not values.empty else ""
        row[f"{prefix}_positive_rate_pct"] = (
            round(float((values > 0).mean() * 100), 1) if not values.empty else ""
        )
    return row


def confirmed_entry_row(hypothesis_id: str, group: pd.DataFrame) -> dict[str, Any]:
    confirmed = group[group["next_close"] > group["event_close"]].copy()
    row: dict[str, Any] = {
        "hypothesis_id": hypothesis_id,
        "segment": "D+1 종가 회복 후 종가 진입",
        "sample_count": len(confirmed),
        "unique_ticker_count": confirmed["ticker"].nunique(),
    }
    for close_column, prefix in (
        ("d_plus_5_close", "entry_to_d_plus_5"),
        ("d_plus_10_close", "entry_to_d_plus_10"),
    ):
        valid = confirmed.dropna(subset=["next_close", close_column])
        returns = (valid[close_column] / valid["next_close"] - 1) * 100
        row[f"{prefix}_count"] = len(returns)
        row[f"{prefix}_avg_return_pct"] = round(float(returns.mean()), 2) if not returns.empty else ""
        row[f"{prefix}_median_return_pct"] = round(float(returns.median()), 2) if not returns.empty else ""
        row[f"{prefix}_positive_rate_pct"] = (
            round(float((returns > 0).mean() * 100), 1) if not returns.empty else ""
        )
    return row


def build_segments(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for hypothesis_id, group in df.groupby("hypothesis_id"):
        ordered = group.sort_values(["signal_date", "ticker"])
        foreign_median = ordered["foreign_5d"].median()
        institution_median = ordered["institution_5d"].median()
        segments = [
            ("전체 신호", ordered),
            ("종목별 최초 신호", ordered.drop_duplicates("ticker", keep="first")),
            ("D+1 종가가 이벤트 종가 초과", ordered[ordered["next_close"] > ordered["event_close"]]),
            (
                "외국인·기관 순매수 모두 중앙값 이상",
                ordered[
                    (ordered["foreign_5d"] >= foreign_median)
                    & (ordered["institution_5d"] >= institution_median)
                ],
            ),
            (
                "밸류에이션 특이 리스크 제한적",
                ordered[ordered["valuation_trap_check"] == "특이 리스크 제한적"],
            ),
            ("순이익 증가", ordered[ordered["valuation_profit_trend"] == "순이익증가"]),
        ]
        rows.extend(metric_row(hypothesis_id, name, segment) for name, segment in segments)
        rows.append(confirmed_entry_row(hypothesis_id, ordered))
    return pd.DataFrame(rows)


def fmt_pct(value: Any) -> str:
    if value == "" or pd.isna(value):
        return ""
    return f"{float(value):+.2f}%"


def metric_text(row: pd.Series, prefix: str) -> str:
    count = row.get(f"{prefix}_count", "")
    avg = row.get(f"{prefix}_avg_return_pct", "")
    median = row.get(f"{prefix}_median_return_pct", "")
    positive = row.get(f"{prefix}_positive_rate_pct", "")
    if count == "" or pd.isna(count) or int(count) == 0:
        return "표본 없음"
    return (
        f"n={int(count)}, 평균 {fmt_pct(avg)}, 중앙값 {fmt_pct(median)}, "
        f"양수율 {float(positive):.1f}%"
    )


def segment_row(summary: pd.DataFrame, hypothesis_id: str, segment: str) -> pd.Series:
    return summary[
        (summary["hypothesis_id"] == hypothesis_id) & (summary["segment"] == segment)
    ].iloc[0]


def build_markdown(source: pd.DataFrame, summary: pd.DataFrame, review_date: str) -> str:
    h01 = segment_row(summary, "H01", "전체 신호")
    h02 = segment_row(summary, "H02", "전체 신호")
    h01_unique = segment_row(summary, "H01", "종목별 최초 신호")
    h02_unique = segment_row(summary, "H02", "종목별 최초 신호")
    h01_entry = segment_row(summary, "H01", "D+1 종가 회복 후 종가 진입")
    h02_entry = segment_row(summary, "H02", "D+1 종가 회복 후 종가 진입")

    lines = [
        "# H01/H02 1차 성과 재검토",
        "",
        f"- 검토일: {review_date}",
        f"- 관찰 기간: {source['signal_date'].min()} ~ {source['signal_date'].max()}",
        f"- 원자료: {len(source):,}건",
        "- 목적: 표본 20건을 넘긴 H01/H02를 매매 후보로 계속 사용할지 판단한다.",
        "",
        "## 결론",
        "",
        "- H01/H02 모두 신규 매매 후보 생성을 동결하고 기존 로그만 비교군으로 유지한다.",
        "- H01은 D+5와 D+10 손실이 확인됐고, 종목별 최초 신호만 사용해도 개선되지 않았다.",
        "- H02는 D+1, D+5, D+10이 모두 음수이며 종목 중복 제거 후에도 결과가 유지됐다.",
        "- D+1 종가 회복 후 실제 진입 가능한 종가를 기준으로 계산해도 성과가 개선되지 않았다.",
        "- H01-v2/H02-v2는 즉시 매매 전략으로 승격하지 않고 실제 확인 시각과 진입가를 기록하는 별도 실험으로 시작한다.",
        "",
        "## 핵심 결과",
        "",
        "| 조건 | 기준 | D+1 | D+5 | D+10 |",
        "| --- | --- | --- | --- | --- |",
        f"| H01 | 전체 {int(h01['sample_count'])}건 | {metric_text(h01, 'next_close')} | {metric_text(h01, 'd_plus_5')} | {metric_text(h01, 'd_plus_10')} |",
        f"| H01 | 종목별 최초 신호 {int(h01_unique['sample_count'])}건 | {metric_text(h01_unique, 'next_close')} | {metric_text(h01_unique, 'd_plus_5')} | {metric_text(h01_unique, 'd_plus_10')} |",
        f"| H02 | 전체 {int(h02['sample_count'])}건 | {metric_text(h02, 'next_close')} | {metric_text(h02, 'd_plus_5')} | {metric_text(h02, 'd_plus_10')} |",
        f"| H02 | 종목별 최초 신호 {int(h02_unique['sample_count'])}건 | {metric_text(h02_unique, 'next_close')} | {metric_text(h02_unique, 'd_plus_5')} | {metric_text(h02_unique, 'd_plus_10')} |",
        "",
        "## 진입 시점 보정",
        "",
        "기존 수익률은 이벤트 당일 종가 기준이라 실제 진입 가능 수익률보다 유리하게 보일 수 있다. "
        "D+1 종가가 이벤트 종가를 회복한 경우에만 D+1 종가로 진입했다고 가정했다.",
        "",
        "| 조건 | 확인 표본 | 진입 후 D+5 | 진입 후 D+10 |",
        "| --- | --- | --- | --- |",
        f"| H01 | {int(h01_entry['sample_count'])}건 | {metric_text(h01_entry, 'entry_to_d_plus_5')} | {metric_text(h01_entry, 'entry_to_d_plus_10')} |",
        f"| H02 | {int(h02_entry['sample_count'])}건 | {metric_text(h02_entry, 'entry_to_d_plus_5')} | {metric_text(h02_entry, 'entry_to_d_plus_10')} |",
        "",
        "## 실패 원인",
        "",
        "1. 과거 백테스트 점수와 실전 관찰 성과가 크게 어긋났다.",
        "2. 이벤트 종가 기준 성과는 다음 거래일 확인 후 진입한다는 실행 규칙을 반영하지 못했다.",
        "3. 외국인·기관 동반매수 규모가 커도 D+5 성과가 일관되게 개선되지 않았다.",
        "4. 밸류에이션과 순이익 증가 필터도 현재 표본에서는 단독 개선 근거가 되지 못했다.",
        "5. D+20 유효 표본은 아직 매우 적어 장기 성과 판단에는 사용할 수 없다.",
        "",
        "## v2 관찰 프로토콜",
        "",
        "### H01-v2",
        "",
        "- 상태: 실험 관찰, 매매 후보 아님",
        "- 당일 상승만으로 진입하지 않는다.",
        "- D+1 장중 전일 종가 회복 시각, 확인 가격, 실제 가상 진입 가격을 별도 기록한다.",
        "- 진입 후 1/3/5거래일 수익률과 장중 최대 불리 변동을 기록한다.",
        "- 최소 20건의 실제 진입 가능 표본 전에는 active 승격을 검토하지 않는다.",
        "",
        "### H02-v2",
        "",
        "- 상태: 재설계 필요, 기존 반등 조건은 폐기 후보",
        "- D+1 종가 회복만으로도 성과가 개선되지 않아 단순 확인 필터를 사용하지 않는다.",
        "- 공시 악재 여부와 하락 추세 종료 신호를 먼저 분리한 뒤 새 조건을 정의한다.",
        "- 새 조건 정의 전에는 H02 이름으로 신규 표본을 추가하지 않는다.",
        "",
        "## 운영 결정",
        "",
        "- active 전략 CSV에서 H01/H02 상태를 `frozen_review`로 둔다.",
        "- 기존 관찰 로그와 D+ 업데이트는 유지한다.",
        "- 일일 후보 생성에서는 H01/H02를 제외한다.",
        "- 다음 검토는 H01-v2 실제 진입 가능 표본 20건 또는 H02 재설계 완료 시 수행한다.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="H01/H02 실전 관찰 성과 재검토")
    parser.add_argument("--input", type=Path, default=OBS_COMMON_CSV)
    parser.add_argument("--review-date", default="2026-06-11")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    output_dir = args.output_dir or SNAPSHOT_DIR / args.review_date
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "H01_H02_1차_성과_재검토.csv"
    output_md = output_dir / "H01_H02_1차_성과_재검토.md"

    source = read_observations(args.input)
    source = source[source["hypothesis_id"].isin(["H01", "H02"])].copy()
    summary = build_segments(source)
    summary.to_csv(output_csv, index=False, encoding="utf-8-sig")
    output_md.write_text(build_markdown(source, summary, args.review_date), encoding="utf-8")

    print(f"observations={len(source)}")
    print(f"summary_rows={len(summary)}")
    print(f"output_csv={output_csv}")
    print(f"output_md={output_md}")


if __name__ == "__main__":
    main()
