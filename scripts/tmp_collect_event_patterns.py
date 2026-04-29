from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
DATA_DIR = BASE_DIR / "03_원천데이터"
PATTERN_DIR = BASE_DIR / "04_패턴분석"

EVENTS_CSV = DATA_DIR / "이벤트.csv"
DISTRIBUTION_MD = DATA_DIR / "이벤트_분포_요약.md"
PATTERN_OVERALL_CSV = PATTERN_DIR / "패턴_분석_전체.csv"
PATTERN_BY_REGIME_CSV = PATTERN_DIR / "패턴_분석_시장국면별.csv"
PATTERN_5AXIS_CSV = PATTERN_DIR / "패턴_분석_5축.csv"
HYPOTHESIS_CSV = PATTERN_DIR / "패턴_가설_후보.csv"
PATTERN_MD = PATTERN_DIR / "패턴_분석_요약.md"

LIST_FIELDS = ("flow_tags", "window_types")
REQUIRED_FIELDS = {
    "event_id",
    "ticker",
    "name",
    "quarter",
    "date",
    "direction",
    "chg_pct",
    "trade_amount",
    "amount_tag",
    "flow_tags",
    "dart_tag",
    "window_types",
    "leading_signal",
    "followup",
    "needs_external_review",
    "market_regime",
}


def compact_label(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace(" ", "").strip()


def join_list(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, list):
        return "|".join(str(item).strip() for item in value if str(item).strip())
    return str(value)


def choose_flow_category(flow_tags: Any) -> str:
    tags = [compact_label(item) for item in (flow_tags or [])]
    if "외국인기관동반매수" in tags:
        return "외국인기관동반매수"
    if "외국인기관동반매도" in tags:
        return "외국인기관동반매도"
    if "외국인기관수급엇갈림" in tags:
        return "수급엇갈림"
    if "개인매수" in tags or "개인매도" in tags:
        return "개인중심"
    return "수급정보부족"


def choose_window_category(window_types: Any) -> str:
    tags = [compact_label(item) for item in (window_types or [])]
    priority = [
        ("외부충격형", "외부충격"),
        ("설명부족형", "설명부족"),
        ("직접반응형", "직접반응"),
        ("선반영형", "선반영"),
        ("누적배경형", "누적배경"),
    ]
    for raw, label in priority:
        if raw in tags:
            return label
    return "창분류부족"


def read_events() -> tuple[pd.DataFrame, list[str]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for path in sorted(BASE_DIR.rglob("*_events.jsonl")):
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"{path.relative_to(BASE_DIR)}:{line_no}: JSON 파싱 실패: {exc}")
                continue
            missing = REQUIRED_FIELDS - set(row)
            if missing:
                errors.append(f"{path.relative_to(BASE_DIR)}:{line_no}: 누락 필드 {sorted(missing)}")
            row["source_file"] = str(path.relative_to(BASE_DIR))
            rows.append(row)
    if not rows:
        return pd.DataFrame(), errors

    df = pd.DataFrame(rows)
    for field in LIST_FIELDS:
        df[field] = df[field].apply(join_list)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["chg_pct"] = pd.to_numeric(df["chg_pct"], errors="coerce")
    df["trade_amount"] = pd.to_numeric(df["trade_amount"], errors="coerce")
    df["trade_amount_eok"] = df["trade_amount"] / 100_000_000
    df["needs_external_review"] = df["needs_external_review"].astype(bool)

    raw_flow_tags = rows_to_list_series(rows, "flow_tags")
    raw_window_types = rows_to_list_series(rows, "window_types")
    df["flow_category"] = raw_flow_tags.apply(choose_flow_category)
    df["window_category"] = raw_window_types.apply(choose_window_category)
    df["followup"] = df["followup"].apply(compact_label)
    df["amount_tag"] = df["amount_tag"].apply(compact_label)
    df["dart_tag"] = df["dart_tag"].apply(compact_label)
    df["leading_signal"] = df["leading_signal"].apply(compact_label)
    df["has_usable_followup"] = df["followup"] != "후속거래일부족"

    ordered = [
        "event_id",
        "ticker",
        "name",
        "quarter",
        "date",
        "direction",
        "chg_pct",
        "trade_amount",
        "trade_amount_eok",
        "amount_tag",
        "flow_category",
        "flow_tags",
        "dart_tag",
        "window_category",
        "window_types",
        "leading_signal",
        "followup",
        "has_usable_followup",
        "needs_external_review",
        "external_review_trigger",
        "market_regime",
        "source_file",
    ]
    return df[[col for col in ordered if col in df.columns]], errors


def rows_to_list_series(rows: list[dict[str, Any]], field: str) -> pd.Series:
    return pd.Series([row.get(field) or [] for row in rows])


def count_table(df: pd.DataFrame, column: str) -> pd.DataFrame:
    out = df[column].value_counts(dropna=False).rename_axis(column).reset_index(name="count")
    out["rate"] = out["count"] / len(df)
    return out


def markdown_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df.empty:
        return "_데이터 없음_"
    view = df.head(max_rows).copy()
    for col in view.columns:
        if pd.api.types.is_float_dtype(view[col]):
            view[col] = view[col].map(lambda x: f"{x:.2%}" if col.endswith("rate") or col == "rate" else f"{x:.2f}")
    headers = [str(col) for col in view.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for _, row in view.iterrows():
        values = [str(row[col]).replace("|", "\\|") for col in view.columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def build_distribution_summary(df: pd.DataFrame, errors: list[str]) -> str:
    lines = [
        "# 이벤트 기초 분포 점검",
        "",
        "## 요약",
        "",
        f"- 전체 이벤트: {len(df):,}건",
        f"- 종목 수: {df['ticker'].nunique():,}개",
        f"- 분기 수: {df['quarter'].nunique():,}개",
        f"- 후속 흐름 산정 가능 이벤트: {int(df['has_usable_followup'].sum()):,}건",
        f"- 외부 자료 확인 필요 이벤트: {int(df['needs_external_review'].sum()):,}건",
        f"- JSON/스키마 오류: {len(errors):,}건",
        "",
        "## 방향별 이벤트",
        "",
        markdown_table(count_table(df, "direction")),
        "",
        "## 후속 흐름 분포",
        "",
        markdown_table(count_table(df, "followup")),
        "",
        "## 시장 국면 분포",
        "",
        markdown_table(count_table(df, "market_regime")),
        "",
        "## 거래대금 태그 분포",
        "",
        markdown_table(count_table(df, "amount_tag")),
        "",
        "## 수급 구조 분포",
        "",
        markdown_table(count_table(df, "flow_category")),
        "",
        "## 공시 유무 분포",
        "",
        markdown_table(count_table(df, "dart_tag")),
        "",
        "## 원인 창 유형 분포",
        "",
        markdown_table(count_table(df, "window_category")),
        "",
        "## 종목별 이벤트 수 상위",
        "",
        markdown_table(df.groupby(["ticker", "name"], as_index=False).size().sort_values("size", ascending=False).head(30)),
        "",
    ]
    if errors:
        lines.extend(["## 오류 목록", ""])
        lines.extend(f"- {err}" for err in errors[:50])
        if len(errors) > 50:
            lines.append(f"- 외 {len(errors) - 50:,}건")
        lines.append("")
    return "\n".join(lines)


def dominant_followup(group: pd.DataFrame) -> pd.Series:
    usable = group[group["has_usable_followup"]]
    if usable.empty:
        return pd.Series(
            {
                "event_count": len(group),
                "usable_event_count": 0,
                "company_count": group["ticker"].nunique(),
                "dominant_followup": "",
                "dominant_count": 0,
                "dominant_rate": 0.0,
                "external_review_count": int(group["needs_external_review"].sum()),
                "leading_signal_count": int((group["leading_signal"] == "있음").sum()),
            }
        )
    counts = usable["followup"].value_counts()
    label = str(counts.index[0])
    count = int(counts.iloc[0])
    return pd.Series(
        {
            "event_count": len(group),
            "usable_event_count": len(usable),
            "company_count": group["ticker"].nunique(),
            "dominant_followup": label,
            "dominant_count": count,
            "dominant_rate": count / len(usable),
            "external_review_count": int(group["needs_external_review"].sum()),
            "leading_signal_count": int((group["leading_signal"] == "있음").sum()),
        }
    )


def pattern_table(df: pd.DataFrame, by_regime: bool = False) -> pd.DataFrame:
    keys = [
        "direction",
        "amount_tag",
        "flow_category",
        "dart_tag",
        "window_category",
    ]
    if by_regime:
        keys.insert(0, "market_regime")
    out = df.groupby(keys, dropna=False).apply(dominant_followup).reset_index()
    out["dominant_rate"] = pd.to_numeric(out["dominant_rate"], errors="coerce").fillna(0)
    out["hypothesis_pass"] = (
        (out["usable_event_count"] >= 10)
        & (out["dominant_rate"] >= 0.65)
        & (out["company_count"] >= 3)
    )
    out["leading_share"] = out["leading_signal_count"] / out["event_count"]
    return out.sort_values(
        ["hypothesis_pass", "dominant_rate", "usable_event_count", "company_count"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)


def five_axis_table(df: pd.DataFrame) -> pd.DataFrame:
    keys = [
        "direction",
        "amount_tag",
        "flow_category",
        "dart_tag",
        "window_category",
        "followup",
    ]
    out = df.groupby(keys, dropna=False).agg(
        event_count=("event_id", "count"),
        company_count=("ticker", "nunique"),
        external_review_count=("needs_external_review", "sum"),
        leading_signal_count=("leading_signal", lambda s: int((s == "있음").sum())),
    ).reset_index()
    out = out.sort_values(["event_count", "company_count"], ascending=[False, False]).reset_index(drop=True)
    return out


def build_pattern_summary(overall: pd.DataFrame, by_regime: pd.DataFrame, five_axis: pd.DataFrame, hypotheses: pd.DataFrame) -> str:
    overall_pass_count = int(overall["hypothesis_pass"].sum())
    by_regime_pass_count = int(by_regime["hypothesis_pass"].sum())
    lines = [
        "# 이벤트 패턴 분석 요약",
        "",
        "## 산출 기준",
        "",
        "- 패턴 후보는 방향, 거래대금 강도, 수급 구조, 공시 유무, 원인 창 유형으로 묶고 후속 흐름의 최빈 라벨을 계산했다.",
        "- `후속거래일부족`은 성공률 계산 분모에서 제외했다.",
        "- 가설 후보 통과 기준: 산정 가능 이벤트 10회 이상, 최빈 후속 흐름 비율 65% 이상, 3개 이상 종목.",
        "",
        "## 가설 후보 요약",
        "",
        f"- 전 기간 합산 패턴 후보 통과: {overall_pass_count:,}개",
        f"- 국면별 패턴 후보 통과: {by_regime_pass_count:,}개",
        f"- 후보 CSV 총 행 수: {len(hypotheses):,}개",
        f"- 전체 조합 수: {len(overall):,}개",
        f"- 국면별 조합 수: {len(by_regime):,}개",
        f"- 5축 실제 발생 조합 수: {len(five_axis):,}개",
        "",
        "## 통과 패턴 상위",
        "",
        markdown_table(
            hypotheses[
                [
                    "scope",
                    "market_regime",
                    "direction",
                    "amount_tag",
                    "flow_category",
                    "dart_tag",
                    "window_category",
                    "dominant_followup",
                    "usable_event_count",
                    "company_count",
                    "dominant_rate",
                    "leading_share",
                ]
            ].head(30)
        ),
        "",
        "## 전체 패턴 상위",
        "",
        markdown_table(
            overall[
                [
                    "direction",
                    "amount_tag",
                    "flow_category",
                    "dart_tag",
                    "window_category",
                    "dominant_followup",
                    "usable_event_count",
                    "company_count",
                    "dominant_rate",
                    "hypothesis_pass",
                ]
            ].head(30)
        ),
        "",
        "## 국면별 패턴 상위",
        "",
        markdown_table(
            by_regime[
                [
                    "market_regime",
                    "direction",
                    "amount_tag",
                    "flow_category",
                    "dart_tag",
                    "window_category",
                    "dominant_followup",
                    "usable_event_count",
                    "company_count",
                    "dominant_rate",
                    "hypothesis_pass",
                ]
            ].head(30)
        ),
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    df, errors = read_events()
    if df.empty:
        raise SystemExit("events.jsonl에서 이벤트를 찾지 못했습니다.")

    df.to_csv(EVENTS_CSV, index=False, encoding="utf-8-sig")
    DISTRIBUTION_MD.write_text(build_distribution_summary(df, errors), encoding="utf-8")

    overall = pattern_table(df, by_regime=False)
    by_regime = pattern_table(df, by_regime=True)
    five_axis = five_axis_table(df)
    overall_candidates = overall[overall["hypothesis_pass"]].copy()
    overall_candidates.insert(0, "scope", "전체")
    overall_candidates.insert(1, "market_regime", "전체")
    by_regime_candidates = by_regime[by_regime["hypothesis_pass"]].copy()
    by_regime_candidates.insert(0, "scope", "국면별")
    hypotheses = pd.concat([overall_candidates, by_regime_candidates], ignore_index=True)
    if not hypotheses.empty:
        hypotheses = hypotheses.sort_values(
            ["scope", "dominant_rate", "usable_event_count", "company_count"],
            ascending=[True, False, False, False],
        ).reset_index(drop=True)

    overall.to_csv(PATTERN_OVERALL_CSV, index=False, encoding="utf-8-sig")
    by_regime.to_csv(PATTERN_BY_REGIME_CSV, index=False, encoding="utf-8-sig")
    five_axis.to_csv(PATTERN_5AXIS_CSV, index=False, encoding="utf-8-sig")
    hypotheses.to_csv(HYPOTHESIS_CSV, index=False, encoding="utf-8-sig")
    PATTERN_MD.write_text(build_pattern_summary(overall, by_regime, five_axis, hypotheses), encoding="utf-8")

    print(f"events={len(df)} errors={len(errors)}")
    print(f"events_csv={EVENTS_CSV}")
    print(f"distribution_md={DISTRIBUTION_MD}")
    print(f"pattern_overall_csv={PATTERN_OVERALL_CSV}")
    print(f"pattern_by_regime_csv={PATTERN_BY_REGIME_CSV}")
    print(f"pattern_5axis_csv={PATTERN_5AXIS_CSV}")
    print(f"hypothesis_csv={HYPOTHESIS_CSV}")
    print(f"pattern_md={PATTERN_MD}")


if __name__ == "__main__":
    main()
