"""팩터 예측력 분석 — IC, 그룹 비교, 상관계수.

입력:  collector.collect_samples() 결과 DataFrame
출력:  지표별 예측력 순위 DataFrame
"""

import pandas as pd

from .collector import FEATURE_COLS, FEATURE_META


def _spearman_ic(x: pd.Series, y: pd.Series) -> float | None:
    """두 Series의 Spearman 순위 상관계수 (scipy 불필요)."""
    valid = pd.concat([x, y], axis=1).dropna()
    if len(valid) < 30:
        return None
    x_clean, y_clean = valid.iloc[:, 0], valid.iloc[:, 1]
    return float(x_clean.rank().corr(y_clean.rank()))


def _pearson_corr(x: pd.Series, y: pd.Series) -> float | None:
    valid = pd.concat([x, y], axis=1).dropna()
    if len(valid) < 30:
        return None
    return float(valid.iloc[:, 0].corr(valid.iloc[:, 1]))


def compute_ic(records: pd.DataFrame) -> pd.DataFrame:
    """지표별 Information Coefficient (Spearman IC) 계산.

    Returns:
        DataFrame with columns:
            feature, description, direction, ic, pearson_corr,
            sample_n, ic_abs (정렬용)
    """
    future_ret = records["future_return"]
    rows = []

    for feat in FEATURE_COLS:
        if feat not in records.columns:
            continue
        desc, direction = FEATURE_META[feat]
        ic     = _spearman_ic(records[feat], future_ret)
        corr   = _pearson_corr(records[feat], future_ret)
        n      = records[feat].dropna().shape[0]
        rows.append({
            "feature":    feat,
            "description": desc,
            "direction":  direction,
            "ic":         ic,
            "pearson_corr": corr,
            "sample_n":   n,
        })

    df = pd.DataFrame(rows)
    df["ic_abs"] = df["ic"].abs()
    return df.sort_values("ic_abs", ascending=False).reset_index(drop=True)


def compute_group_stats(
    records: pd.DataFrame,
    up_threshold: float = 0.10,
    down_threshold: float = -0.08,
) -> pd.DataFrame:
    """급등/급락 그룹별 지표 평균 비교.

    Args:
        up_threshold:   급등 기준 수익률 (예: 0.10 = +10%)
        down_threshold: 급락 기준 수익률 (예: -0.08 = -8%)

    Returns:
        DataFrame with columns:
            feature, description,
            up_mean, up_std, up_n,
            down_mean, down_std, down_n,
            mean_diff (up_mean - down_mean)
    """
    up_mask   = records["future_return"] >= up_threshold
    down_mask = records["future_return"] <= down_threshold

    up_grp   = records[up_mask]
    down_grp = records[down_mask]

    rows = []
    for feat in FEATURE_COLS:
        if feat not in records.columns:
            continue
        desc, _ = FEATURE_META[feat]
        u = up_grp[feat].dropna()
        d = down_grp[feat].dropna()
        rows.append({
            "feature":    feat,
            "description": desc,
            "up_mean":    u.mean()   if len(u) >= 5 else None,
            "up_std":     u.std()    if len(u) >= 5 else None,
            "up_n":       len(u),
            "down_mean":  d.mean()   if len(d) >= 5 else None,
            "down_std":   d.std()    if len(d) >= 5 else None,
            "down_n":     len(d),
        })

    df = pd.DataFrame(rows)
    df["mean_diff"] = df["up_mean"] - df["down_mean"]
    df["diff_abs"]  = df["mean_diff"].abs()
    return df.sort_values("diff_abs", ascending=False).reset_index(drop=True)


def rank_indicators(
    ic_df: pd.DataFrame,
    group_df: pd.DataFrame,
) -> pd.DataFrame:
    """IC 순위와 그룹 비교를 결합한 최종 종합 순위표.

    IC와 그룹 mean_diff를 각각 정규화 후 합산하여 종합점수 산출.

    Returns:
        DataFrame with columns:
            rank, feature, description, direction,
            ic, ic_rank, mean_diff, group_rank,
            score (종합), sample_n, interpretation
    """
    ic   = ic_df[["feature", "description", "direction", "ic", "ic_abs", "sample_n"]].copy()
    grp  = group_df[["feature", "mean_diff", "diff_abs"]].copy()

    merged = ic.merge(grp, on="feature", how="left")

    # 정규화 (0~1)
    ic_max   = merged["ic_abs"].max()
    diff_max = merged["diff_abs"].max()
    merged["ic_norm"]   = merged["ic_abs"]   / ic_max   if ic_max   > 0 else 0
    merged["diff_norm"] = merged["diff_abs"] / diff_max if diff_max > 0 else 0

    # 종합점수 (IC 60% + 그룹차이 40%)
    merged["score"] = 0.6 * merged["ic_norm"] + 0.4 * merged["diff_norm"]
    merged = merged.sort_values("score", ascending=False).reset_index(drop=True)
    merged.insert(0, "rank", range(1, len(merged) + 1))

    # IC 강도 해석 라벨
    def _interp(ic_val: float | None) -> str:
        if ic_val is None:
            return "데이터 부족"
        a = abs(ic_val)
        if a >= 0.10:
            return "강한 예측력"
        if a >= 0.05:
            return "유의미"
        if a >= 0.02:
            return "약한 예측력"
        return "노이즈 수준"

    merged["interpretation"] = merged["ic"].apply(_interp)

    cols = [
        "rank", "feature", "description", "direction",
        "ic", "mean_diff", "score", "sample_n", "interpretation",
    ]
    return merged[cols]
