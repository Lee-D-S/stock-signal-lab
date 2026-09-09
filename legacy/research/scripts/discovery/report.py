"""팩터 리서치 결과 출력 및 CSV 저장.

screener.py CLI 조건 후보 자동 제안 포함.
새 특징의 screener_arg 는 collector.py 의 Feature 정의에서 관리한다.
"""

from pathlib import Path

import pandas as pd

from .collector import _FEATURES

# feature → screener.py CLI 옵션 매핑 — collector.py Feature.screener_arg 에서 자동 파생
_SCREENER_ARGS: dict[str, str] = {
    f.name: f.screener_arg for f in _FEATURES if f.screener_arg
}


def print_report(
    ranked: pd.DataFrame,
    group_df: pd.DataFrame,
    meta: dict,
) -> None:
    """결과 테이블을 콘솔에 출력."""
    print()
    print("=" * 80)
    print("[ 팩터 리서치 결과 ]")
    print(f"  기간    : {meta.get('start')} ~ {meta.get('end')}")
    print(f"  급등 기준: {meta.get('up_threshold', 0.10)*100:.0f}일 +{meta.get('up_threshold_pct', 10):.0f}%")
    print(f"  급락 기준: {meta.get('down_threshold', -0.08)*100:.0f}일 {meta.get('down_threshold_pct', -8):.0f}%")
    print(f"  보유일  : {meta.get('hold_days', 20)}일")
    print(f"  총 레코드: {meta.get('total_records', 0):,}건")
    print(f"  급등 그룹: {meta.get('up_n', 0):,}건 | 급락 그룹: {meta.get('down_n', 0):,}건")
    print("=" * 80)

    # IC 기반 순위표
    print()
    print("── 지표별 예측력 (IC 기준 종합점수 내림차순) ──")
    header = f"{'순위':>3}  {'지표':<18} {'IC':>7} {'그룹차이':>9} {'점수':>6}  {'샘플':>7}  {'해석'}"
    print(header)
    print("-" * 80)

    for _, row in ranked.iterrows():
        ic_str   = f"{row['ic']:+.4f}" if pd.notna(row["ic"]) else "  N/A "
        diff_str = f"{row['mean_diff']:+.4f}" if pd.notna(row["mean_diff"]) else "  N/A "
        print(
            f"{int(row['rank']):>3}  {row['feature']:<18} {ic_str:>7} "
            f"{diff_str:>9} {row['score']:>6.3f}  {int(row['sample_n']):>7}  "
            f"{row['interpretation']}"
        )

    # 급등/급락 그룹 비교 상위 5개
    print()
    print("── 급등 vs 급락 그룹 평균 비교 (상위 5개) ──")
    top5 = group_df.head(5)
    for _, row in top5.iterrows():
        up   = f"{row['up_mean']:+.4f}" if pd.notna(row["up_mean"])   else "  N/A"
        down = f"{row['down_mean']:+.4f}" if pd.notna(row["down_mean"]) else "  N/A"
        diff = f"{row['mean_diff']:+.4f}" if pd.notna(row["mean_diff"]) else "  N/A"
        print(f"  {row['feature']:<18}  급등={up}  급락={down}  차이={diff}")


def print_condition_candidates(ranked: pd.DataFrame, top_n: int = 5) -> None:
    """상위 지표로 screener.py 조건 후보 출력."""
    print()
    print("── 권장 screener 조건 후보 (검증 전 가설) ──")
    print("  ※ 이 조건들은 BACKTEST_PLAN.md 기준으로 별도 검증 필요")
    print()

    args_set: set[str] = set()
    for _, row in ranked.head(top_n).iterrows():
        feat = row["feature"]
        arg  = _SCREENER_ARGS.get(feat)
        if arg:
            args_set.add(arg)

    cmd = "python scripts/screener.py --by marcap --to 300 " + " ".join(sorted(args_set))
    print(f"  {cmd}")
    print()

    print("  개별 조건 목록:")
    for _, row in ranked.head(top_n).iterrows():
        feat = row["feature"]
        arg  = _SCREENER_ARGS.get(feat, "(매핑 없음)")
        ic   = f"{row['ic']:+.4f}" if pd.notna(row["ic"]) else "N/A"
        print(f"    IC={ic}  {row['description']:<28} → {arg}")


def save_csv(
    ranked: pd.DataFrame,
    group_df: pd.DataFrame,
    output_dir: str | Path = "scripts/discovery/results",
    prefix: str = "discovery",
) -> None:
    """결과를 CSV로 저장."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    ic_path    = out / f"{prefix}_ic_ranking.csv"
    group_path = out / f"{prefix}_group_stats.csv"

    ranked.to_csv(ic_path,    index=False, encoding="utf-8-sig")
    group_df.to_csv(group_path, index=False, encoding="utf-8-sig")

    print(f"[report] IC 순위표 저장: {ic_path}")
    print(f"[report] 그룹 통계 저장: {group_path}")
