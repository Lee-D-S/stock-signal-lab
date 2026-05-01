from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
SNAPSHOT_ROOT = BASE_DIR / "09_조건스냅샷"


def pct(value: float) -> str:
    return f"{float(value):+.2f}%"


def rate(value: float) -> str:
    return f"{float(value):.2%}"


def markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    if df.empty:
        return "_없음_"
    view = df[columns].fillna("")
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in view.iterrows():
        lines.append("| " + " | ".join(str(row[col]).replace("|", "\\|") for col in columns) + " |")
    return "\n".join(lines)


def verdict_for(hypothesis_id: str, tested_trades: int, score: float, hit_rate: float) -> tuple[str, str]:
    if hypothesis_id in {"H05", "H07"}:
        return (
            "우선 관찰",
            "기존 active에는 없고, 표본은 제한적이지만 여러 종목에서 양의 점수와 높은 적중률이 확인됨",
        )
    if hypothesis_id in {"H01", "H02"}:
        return (
            "소표본 보류",
            "수익률은 좋아 보이나 실전 테스트 표본/종목 수가 너무 작아 active 승격 전 추가 관찰 필요",
        )
    if hypothesis_id == "H04":
        return "보류", "테스트 표본은 있으나 평균 점수 우위가 약해 단독 조건 승격 근거 부족"
    if hypothesis_id == "H09":
        return "승격 제외", "추격매수 회피 후보인데 평균 점수가 음수라 현재 기준 회피 조건으로 부적합"
    if tested_trades >= 10 and score > 5 and hit_rate >= 0.65:
        return "우선 관찰", "표본과 점수 기준이 1차 관찰 요건을 충족"
    return "검토", "추가 검토 필요"


def build_review(snapshot_date: str) -> tuple[pd.DataFrame, str]:
    snapshot_dir = SNAPSHOT_ROOT / snapshot_date
    comparison = pd.read_csv(snapshot_dir / "기존_active_조건_비교.csv", encoding="utf-8-sig")
    inputs = pd.read_csv(snapshot_dir / "가설_백테스트_입력값.csv", encoding="utf-8-sig")
    summary = pd.read_csv(snapshot_dir / "가설_실전_백테스트_요약.csv", encoding="utf-8-sig")
    configs = pd.read_csv(snapshot_dir / "가설_실전_백테스트_전체_설정.csv", encoding="utf-8-sig")

    new_ids = comparison.loc[comparison["status"] == "신규 후보", "snapshot_hypothesis_id"].dropna().tolist()
    rows = []
    for hypothesis_id in new_ids:
        inp = inputs[inputs["hypothesis_id"] == hypothesis_id].iloc[0]
        summ = summary[summary["hypothesis_id"] == hypothesis_id].iloc[0]
        best_config = (
            configs[configs["hypothesis_id"] == hypothesis_id]
            .sort_values(["avg_score_return_pct", "hit_rate", "tested_trades"], ascending=[False, False, False])
            .iloc[0]
        )
        tested_trades = int(summ["tested_trades"])
        score = float(summ["avg_score_return_pct"])
        hit = float(summ["hit_rate"])
        verdict, reason = verdict_for(hypothesis_id, tested_trades, score, hit)
        rows.append(
            {
                "snapshot_id": hypothesis_id,
                "suggested_stable_id": f"NEW-{snapshot_date.replace('-', '')}-{hypothesis_id}",
                "verdict": verdict,
                "market_regime": inp["market_regime"],
                "direction": inp["direction"],
                "amount_tag": inp["amount_tag"],
                "flow_category": inp["flow_category"],
                "dart_tag": inp["dart_tag"],
                "window_category": inp["window_category"],
                "action_hint": inp["action_hint"],
                "dominant_followup": inp["dominant_followup"],
                "total_events": int(summ["total_events"]),
                "tested_trades": tested_trades,
                "company_count": int(summ["company_count"]),
                "avg_score_return_pct": score,
                "hit_rate": hit,
                "worst_score_return_pct": float(summ["worst_score_return_pct"]),
                "best_score_return_pct": float(summ["best_score_return_pct"]),
                "best_entry_mode": best_config["entry_mode"],
                "best_hold_days": int(best_config["hold_days"]),
                "best_config_score_pct": float(best_config["avg_score_return_pct"]),
                "best_config_hit_rate": float(best_config["hit_rate"]),
                "decision_reason": reason,
            }
        )

    out = pd.DataFrame(rows)
    order = {"우선 관찰": 0, "소표본 보류": 1, "보류": 2, "승격 제외": 3, "검토": 4}
    out["sort_key"] = out["verdict"].map(order).fillna(9)
    out = out.sort_values(["sort_key", "avg_score_return_pct"], ascending=[True, False]).drop(columns="sort_key")
    return out, build_markdown(snapshot_date, out)


def build_markdown(snapshot_date: str, review: pd.DataFrame) -> str:
    view = review.copy()
    for col in ["avg_score_return_pct", "worst_score_return_pct", "best_score_return_pct", "best_config_score_pct"]:
        view[col] = view[col].map(pct)
    for col in ["hit_rate", "best_config_hit_rate"]:
        view[col] = view[col].map(rate)

    priority = review[review["verdict"] == "우선 관찰"]
    held = review[review["verdict"].isin(["소표본 보류", "보류"])]
    rejected = review[review["verdict"] == "승격 제외"]
    columns = [
        "snapshot_id",
        "suggested_stable_id",
        "verdict",
        "market_regime",
        "direction",
        "amount_tag",
        "flow_category",
        "dart_tag",
        "window_category",
        "action_hint",
        "tested_trades",
        "company_count",
        "avg_score_return_pct",
        "hit_rate",
        "best_entry_mode",
        "best_hold_days",
        "decision_reason",
    ]

    return "\n".join(
        [
            f"# {snapshot_date} 신규 조건 검토",
            "",
            "## 결론",
            "",
            "- 신규 후보 조건 6개 중 active 즉시 추가 대상은 없다.",
            f"- 우선 관찰 대상은 {len(priority)}개다.",
            f"- 보류 대상은 {len(held)}개다.",
            f"- 승격 제외 대상은 {len(rejected)}개다.",
            "",
            "## 신규 조건별 판단",
            "",
            markdown_table(view, columns),
            "",
            "## 운영 방침",
            "",
            "- 이 조건들은 기존 `H01~H06` 번호 체계에 섞지 않는다.",
            "- 일별 후보 산출에는 아직 active 조건만 사용한다.",
            "- 신규 조건은 최소 20건 이상 관찰 표본이 쌓일 때까지 스냅샷 검토 대상으로만 유지한다.",
            "- 승격이 필요하면 별도 stable ID를 부여한 뒤 active 조건 파일에 명시적으로 추가한다.",
            "",
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="스냅샷 신규 조건 검토 문서 생성")
    parser.add_argument("--snapshot-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    snapshot_dir = SNAPSHOT_ROOT / args.snapshot_date
    review, markdown = build_review(args.snapshot_date)
    csv_path = snapshot_dir / "신규_조건_검토.csv"
    md_path = snapshot_dir / "신규_조건_검토.md"
    review.to_csv(csv_path, index=False, encoding="utf-8-sig")
    md_path.write_text(markdown, encoding="utf-8")

    print(f"new_conditions={len(review)}")
    print(f"priority_watch={int((review['verdict'] == '우선 관찰').sum())}")
    print(f"csv={csv_path}")
    print(f"md={md_path}")


if __name__ == "__main__":
    main()
