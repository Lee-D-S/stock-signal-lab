"""조건 임계값 탐색.

scorer.py의 각 조건에 대해 임계값 variant를 독립 binary feature로 만들어
IC·평균수익률·승률·Profit Factor를 train/validation 구간으로 분리해 비교한다.

## 단기/장기 조건 분리 원칙

조건마다 신호의 정보 유효 기간(information horizon)이 다르다.
같은 조건이라도 hold_days가 달라지면 성과가 완전히 달라질 수 있다.

  short  (5·10·20일):  빠른 가격 반응 — RSI, Stoch, MACD, 거래량, OBV, BB, 단기 MA
  medium (20·60일):    스윙·추세 지속 — 거래량, OBV, 수급, 업종 모멘텀, MA 정배열
  long   (60·120·240일): 기업 가치·큰 추세 — PER, PBR, ROE, ROA, 부채비율, 장기 MA

단기 조건을 240일로 검증하면 신호가 희석된다.
장기 조건을 5일로 검증하면 신호가 아직 가격에 반영되지 않았을 수 있다.

## 권장 실행 예시

    # 단기 조건 검증 (records: hold_days=5 또는 10 또는 20)
    python scripts/run_condition_search.py \\
        --load-records scripts/discovery/results/records_hold5.parquet \\
        --train-end 2022-12-31 --val-end 2024-12-31 --horizon short

    # 장기 조건 검증 (records: hold_days=60 이상)
    python scripts/run_condition_search.py \\
        --load-records scripts/discovery/results/records_hold60.parquet \\
        --train-end 2022-12-31 --val-end 2024-12-31 --horizon long

    # hold_days 없이 전체 (horizon 경고만 표시)
    python scripts/run_condition_search.py \\
        --load-records scripts/discovery/results/records.parquet \\
        --train-end 2022-12-31 --val-end 2024-12-31

    # 특정 조건만
    python scripts/run_condition_search.py \\
        --load-records scripts/discovery/results/records.parquet \\
        --conditions rsi_low,stoch_low,vol_surge

## records 파일 준비 (hold_days별 분리 수집)

    python scripts/run_discovery.py --start 2020-01-01 --end 2024-12-31 \\
        --hold-days 5  --save-records scripts/discovery/results/records_hold5.parquet
    python scripts/run_discovery.py --start 2020-01-01 --end 2024-12-31 \\
        --hold-days 20 --save-records scripts/discovery/results/records_hold20.parquet
    python scripts/run_discovery.py --start 2020-01-01 --end 2024-12-31 \\
        --hold-days 60 --save-records scripts/discovery/results/records_hold60.parquet
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv
load_dotenv()


_MIN_SAMPLES = 50
_RESULTS_DIR = Path(__file__).parent / "condition_search" / "results"

# horizon → 적합한 hold_days 집합
_HORIZON_HOLD_DAYS: dict[str, set[int]] = {
    "short":  {5, 10, 20},
    "medium": {20, 60},
    "long":   {60, 120, 240},
}


@dataclass
class ConditionVariant:
    condition:   str
    name:        str
    feature:     str
    threshold:   float
    operator:    str          # "<=", ">=", ">", "<"
    description: str
    horizon:     str          # "short" | "medium" | "long"
    is_current:  bool = False
    min_val:     float | None = None  # feature > min_val 추가 필터 (PER > 0 등)


_S, _M, _L = "short", "medium", "long"

_VARIANTS: list[ConditionVariant] = [
    # ── rsi_low · SHORT (빠른 과매도 반등, 5/10/20일) ─────────────────────────
    ConditionVariant("rsi_low", "rsi_low_40", "rsi", 40, "<=", "RSI ≤ 40", _S),
    ConditionVariant("rsi_low", "rsi_low_45", "rsi", 45, "<=", "RSI ≤ 45", _S),
    ConditionVariant("rsi_low", "rsi_low_50", "rsi", 50, "<=", "RSI ≤ 50", _S, is_current=True),
    ConditionVariant("rsi_low", "rsi_low_55", "rsi", 55, "<=", "RSI ≤ 55", _S),
    ConditionVariant("rsi_low", "rsi_low_60", "rsi", 60, "<=", "RSI ≤ 60", _S),

    # ── stoch_low · SHORT (과매도, 5/10/20일) ────────────────────────────────
    ConditionVariant("stoch_low", "stoch_low_30", "stoch_k", 30, "<=", "Stoch K ≤ 30", _S),
    ConditionVariant("stoch_low", "stoch_low_40", "stoch_k", 40, "<=", "Stoch K ≤ 40", _S),
    ConditionVariant("stoch_low", "stoch_low_50", "stoch_k", 50, "<=", "Stoch K ≤ 50", _S, is_current=True),
    ConditionVariant("stoch_low", "stoch_low_60", "stoch_k", 60, "<=", "Stoch K ≤ 60", _S),

    # ── vol_above_ma · MEDIUM (추세 지속 확인, 20/60일) ──────────────────────
    ConditionVariant("vol_above_ma", "vol_above_ma_1_0", "vol_ratio", 1.0, ">", "Vol > 1.0× MA20", _M, is_current=True),
    ConditionVariant("vol_above_ma", "vol_above_ma_1_2", "vol_ratio", 1.2, ">", "Vol > 1.2× MA20", _M),
    ConditionVariant("vol_above_ma", "vol_above_ma_1_5", "vol_ratio", 1.5, ">", "Vol > 1.5× MA20", _M),
    ConditionVariant("vol_above_ma", "vol_above_ma_2_0", "vol_ratio", 2.0, ">", "Vol > 2.0× MA20", _M),

    # ── vol_surge · SHORT (단기 급증 반응, 5/10/20일) ─────────────────────────
    ConditionVariant("vol_surge", "vol_surge_1_2", "vol_ratio", 1.2, ">", "Vol > 1.2× MA20", _S),
    ConditionVariant("vol_surge", "vol_surge_1_5", "vol_ratio", 1.5, ">", "Vol > 1.5× MA20", _S, is_current=True),
    ConditionVariant("vol_surge", "vol_surge_2_0", "vol_ratio", 2.0, ">", "Vol > 2.0× MA20", _S),
    ConditionVariant("vol_surge", "vol_surge_3_0", "vol_ratio", 3.0, ">", "Vol > 3.0× MA20", _S),

    # ── binary technical conditions · SHORT/MEDIUM ───────────────────────────
    ConditionVariant("macd_positive", "macd_positive_true", "macd_positive", 0.5, ">=", "MACD hist > 0", _S, is_current=True),
    ConditionVariant("macd_rising", "macd_rising_true", "macd_rising", 0.5, ">=", "MACD hist rising", _S, is_current=True),
    ConditionVariant("obv_rising", "obv_rising_true", "obv_rising", 0.5, ">=", "OBV MA5 > MA20", _S, is_current=True),
    ConditionVariant("bb_breakout", "bb_breakout_true", "bb_breakout", 0.5, ">=", "Close > BB upper", _S, is_current=True),
    ConditionVariant("fib_support", "fib_support_true", "fib_support", 0.5, ">=", "Fib support ±2%", _S, is_current=True),
    ConditionVariant("bb_above_mid", "bb_above_mid_true", "bb_above_mid", 0.5, ">=", "Close > MA20", _M, is_current=True),
    ConditionVariant("ma_align_short", "ma_align_short_true", "ma_align_short", 0.5, ">=", "MA5 > MA20 > MA60", _M, is_current=True),
    ConditionVariant("ma_align_long", "ma_align_long_true", "ma_align_long", 0.5, ">=", "MA60 > MA120 > MA240", _M, is_current=True),
    ConditionVariant("bb_squeeze", "bb_squeeze_true", "bb_squeeze", 0.5, ">=", "BB width shrinking", _M, is_current=True),

    # ── per_low · LONG (기업 가치, 60/120/240일) ──────────────────────────────
    ConditionVariant("per_low", "per_low_10", "per", 10, "<=", "0 < PER ≤ 10", _L,                  min_val=0.0),
    ConditionVariant("per_low", "per_low_15", "per", 15, "<=", "0 < PER ≤ 15", _L, is_current=True, min_val=0.0),
    ConditionVariant("per_low", "per_low_20", "per", 20, "<=", "0 < PER ≤ 20", _L,                  min_val=0.0),
    ConditionVariant("per_low", "per_low_25", "per", 25, "<=", "0 < PER ≤ 25", _L,                  min_val=0.0),

    # ── pbr_low · LONG ────────────────────────────────────────────────────────
    ConditionVariant("pbr_low", "pbr_low_1_0", "pbr", 1.0, "<=", "0 < PBR ≤ 1.0", _L,                  min_val=0.0),
    ConditionVariant("pbr_low", "pbr_low_1_5", "pbr", 1.5, "<=", "0 < PBR ≤ 1.5", _L, is_current=True, min_val=0.0),
    ConditionVariant("pbr_low", "pbr_low_2_0", "pbr", 2.0, "<=", "0 < PBR ≤ 2.0", _L,                  min_val=0.0),
    ConditionVariant("pbr_low", "pbr_low_3_0", "pbr", 3.0, "<=", "0 < PBR ≤ 3.0", _L,                  min_val=0.0),

    # ── roe_good · LONG ───────────────────────────────────────────────────────
    ConditionVariant("roe_good", "roe_good_5",  "roe",  5, ">=", "ROE ≥ 5%",  _L),
    ConditionVariant("roe_good", "roe_good_10", "roe", 10, ">=", "ROE ≥ 10%", _L, is_current=True),
    ConditionVariant("roe_good", "roe_good_15", "roe", 15, ">=", "ROE ≥ 15%", _L),
    ConditionVariant("roe_good", "roe_good_20", "roe", 20, ">=", "ROE ≥ 20%", _L),

    # ── debt_low · LONG ───────────────────────────────────────────────────────
    ConditionVariant("debt_low", "debt_low_80",  "debt_ratio",  80, "<=", "부채비율 ≤ 80%",  _L),
    ConditionVariant("debt_low", "debt_low_100", "debt_ratio", 100, "<=", "부채비율 ≤ 100%", _L, is_current=True),
    ConditionVariant("debt_low", "debt_low_150", "debt_ratio", 150, "<=", "부채비율 ≤ 150%", _L),
    ConditionVariant("debt_low", "debt_low_200", "debt_ratio", 200, "<=", "부채비율 ≤ 200%", _L),

    # ── roa_good · LONG ───────────────────────────────────────────────────────
    ConditionVariant("roa_good", "roa_good_3",  "roa",  3, ">=", "ROA ≥ 3%",  _L),
    ConditionVariant("roa_good", "roa_good_5",  "roa",  5, ">=", "ROA ≥ 5%",  _L, is_current=True),
    ConditionVariant("roa_good", "roa_good_7",  "roa",  7, ">=", "ROA ≥ 7%",  _L),
    ConditionVariant("roa_good", "roa_good_10", "roa", 10, ">=", "ROA ≥ 10%", _L),

    # ── op_margin_good · LONG ─────────────────────────────────────────────────
    ConditionVariant("op_margin_good", "op_margin_3",  "op_margin",  3, ">=", "영업이익률 ≥ 3%",  _L),
    ConditionVariant("op_margin_good", "op_margin_5",  "op_margin",  5, ">=", "영업이익률 ≥ 5%",  _L, is_current=True),
    ConditionVariant("op_margin_good", "op_margin_8",  "op_margin",  8, ">=", "영업이익률 ≥ 8%",  _L),
    ConditionVariant("op_margin_good", "op_margin_10", "op_margin", 10, ">=", "영업이익률 ≥ 10%", _L),
]


# ── 통계 계산 ─────────────────────────────────────────────────────────────────

def _apply_signal(series: pd.Series, op: str, threshold: float, min_val: float | None) -> pd.Series:
    if op == "<=":   sig = series <= threshold
    elif op == ">=": sig = series >= threshold
    elif op == ">":  sig = series > threshold
    elif op == "<":  sig = series < threshold
    else: raise ValueError(f"unknown operator: {op}")
    if min_val is not None:
        sig = sig & (series > min_val)
    return sig


def _spearman_ic(x: pd.Series, y: pd.Series) -> float | None:
    """scipy 불필요한 Spearman IC (analyzer.py와 동일 구현)."""
    valid = pd.concat([x, y], axis=1).dropna()
    if len(valid) < _MIN_SAMPLES:
        return None
    return float(valid.iloc[:, 0].rank().corr(valid.iloc[:, 1].rank()))


def _compute_stats(
    df: pd.DataFrame,
    feature: str,
    op: str,
    threshold: float,
    period_mask: pd.Series,
    min_val: float | None = None,
) -> dict:
    if feature not in df.columns:
        return {"n": 0, "valid": False, "missing_col": True}

    feat   = df[feature]
    signal = _apply_signal(feat, op, threshold, min_val)
    avail  = feat.notna() & df["future_return"].notna() & period_mask
    n_sig  = int((signal & avail).sum())

    if n_sig < _MIN_SAMPLES:
        return {"n": n_sig, "valid": False}

    ic = _spearman_ic(signal[avail].astype("float64"), df.loc[avail, "future_return"])
    if ic is None:
        return {"n": n_sig, "valid": False}

    ret      = df.loc[signal & avail, "future_return"]
    mean_ret = float(ret.mean())
    win_rate = float((ret > 0).mean())
    pos_sum  = float(ret[ret > 0].sum())
    neg_sum  = float(abs(ret[ret <= 0].sum()))
    pf       = (pos_sum / neg_sum) if neg_sum > 0 else float("inf")

    return {
        "n":        n_sig,
        "ic":       round(ic, 4),
        "mean_ret": round(mean_ret * 100, 2),
        "win_rate": round(win_rate * 100, 1),
        "pf":       round(min(pf, 99.9), 2),
        "valid":    True,
    }


def _verdict(t: dict, v: dict) -> str:
    if t.get("missing_col") or v.get("missing_col"):
        return "- (데이터없음)"
    if not t.get("valid") or not v.get("valid"):
        tn, vn = t.get("n", 0), v.get("n", 0)
        return f"- (n:{tn}/{vn})"
    ic_ok = (t["ic"] or 0) > 0    and (v["ic"] or 0) > 0
    wr_ok = (t["win_rate"] or 0) > 50 and (v["win_rate"] or 0) > 50
    pf_ok = (t["pf"] or 0) > 1.2  and (v["pf"] or 0) > 1.2
    score = ic_ok + wr_ok + pf_ok
    if score == 3: return "KEEP"
    if score == 2: return "CHECK"
    return "DROP"


# ── 탐색 ──────────────────────────────────────────────────────────────────────

def _check_horizon_mismatch(hold_days: int | None, horizon: str) -> bool:
    """records의 hold_days가 조건의 horizon에 맞지 않으면 True."""
    if hold_days is None:
        return False
    return hold_days not in _HORIZON_HOLD_DAYS.get(horizon, set())


def _search(
    records: pd.DataFrame,
    variants: list[ConditionVariant],
    train_end: str,
    val_end: str,
    records_hold_days: int | None = None,
) -> pd.DataFrame:
    records = records.copy()
    records["date"] = pd.to_datetime(records["date"])

    train_mask = records["date"] <= pd.Timestamp(train_end)
    val_mask   = (records["date"] > pd.Timestamp(train_end)) & \
                 (records["date"] <= pd.Timestamp(val_end))

    rows = []
    for v in variants:
        mismatch = _check_horizon_mismatch(records_hold_days, v.horizon)
        ts = _compute_stats(records, v.feature, v.operator, v.threshold, train_mask, v.min_val)
        vs = _compute_stats(records, v.feature, v.operator, v.threshold, val_mask,   v.min_val)
        rows.append({
            "condition":      v.condition,
            "horizon":        v.horizon,
            "variant":        v.name,
            "description":    v.description,
            "is_current":     v.is_current,
            "horizon_mismatch": mismatch,
            "train_n":        ts.get("n"),
            "train_ic":       ts.get("ic"),
            "train_mean_ret": ts.get("mean_ret"),
            "train_win_rate": ts.get("win_rate"),
            "train_pf":       ts.get("pf"),
            "val_n":          vs.get("n"),
            "val_ic":         vs.get("ic"),
            "val_mean_ret":   vs.get("mean_ret"),
            "val_win_rate":   vs.get("win_rate"),
            "val_pf":         vs.get("pf"),
            "verdict":        _verdict(ts, vs),
        })

    return pd.DataFrame(rows)


# ── 출력 ──────────────────────────────────────────────────────────────────────

def _fv(val, spec: str) -> str:
    """float 포맷. None/NaN은 N/A 반환."""
    if val is None:
        return "  N/A"
    try:
        f = float(val)
        if f != f:  # NaN
            return "  N/A"
        return format(f, spec)
    except (TypeError, ValueError):
        return "  N/A"


def _fn(val) -> str:
    """int 포맷 (천 단위 구분)."""
    if val is None:
        return "    N/A"
    try:
        return f"{int(val):7,}"
    except (TypeError, ValueError):
        return "    N/A"


_HORIZON_LABEL = {"short": "SHORT (5/10/20일)", "medium": "MEDIUM (20/60일)", "long": "LONG (60/120/240일)"}
_HORIZON_ORDER = ["short", "medium", "long"]


def _print_results(
    df: pd.DataFrame,
    train_end: str,
    val_end: str,
    records_hold_days: int | None,
) -> None:
    W = 110
    hdr = (f"  {'임계값':<26} {'tr_n':>7} {'IC':>6} {'수익%':>6} {'승률%':>6} {'PF':>5}  "
           f"{'va_n':>7} {'IC':>6} {'수익%':>6} {'승률%':>6} {'PF':>5}  판정")

    hold_note = f"hold_days={records_hold_days}" if records_hold_days else "hold_days 미지정"

    print()
    print("=" * W)
    print(f"  조건 임계값 탐색 결과  ({hold_note})")
    print(f"  Train ≤ {train_end}  /  Val {int(train_end[:4])+1}-01-01 ~ {val_end}")
    print(f"  IC: Spearman(binary, future_return)  |  수익%·승률%·PF: signal=True 행 기준")
    print("=" * W)

    # horizon 순서대로 출력
    horizons_in_df = [h for h in _HORIZON_ORDER if h in df["horizon"].values]
    for horizon in horizons_in_df:
        h_df = df[df["horizon"] == horizon]
        label = _HORIZON_LABEL[horizon]

        # hold_days 불일치 경고
        mismatch_conds = h_df[h_df["horizon_mismatch"]]["condition"].unique()
        recommended = sorted(_HORIZON_HOLD_DAYS[horizon])

        print(f"\n{'-' * W}")
        print(f"  [{label}]  권장 hold_days: {recommended}")
        if len(mismatch_conds) > 0 and records_hold_days is not None:
            print(f"  WARN: records hold_days={records_hold_days}는 이 horizon({horizon})에 적합하지 않음 "
                  f"- 결과 참고용으로만 볼 것")
        print(f"{'-' * W}")

        for condition, group in h_df.groupby("condition", sort=False):
            print(f"\n  [{condition}]")
            print(hdr)
            print("  " + "-" * (W - 2))

            for _, row in group.iterrows():
                star    = "* " if row["is_current"] else "  "
                mflag   = " !" if row["horizon_mismatch"] else "  "
                desc    = (star + row["description"])

                line = (
                    f"{mflag}{desc:<26}"
                    f" {_fn(row['train_n'])}"
                    f" {_fv(row['train_ic'],       '6.3f')}"
                    f" {_fv(row['train_mean_ret'],  '6.2f')}"
                    f" {_fv(row['train_win_rate'],  '6.1f')}"
                    f" {_fv(row['train_pf'],        '5.2f')}"
                    f"  {_fn(row['val_n'])}"
                    f" {_fv(row['val_ic'],         '6.3f')}"
                    f" {_fv(row['val_mean_ret'],    '6.2f')}"
                    f" {_fv(row['val_win_rate'],    '6.1f')}"
                    f" {_fv(row['val_pf'],          '5.2f')}"
                    f"  {row['verdict']}"
                )
                print(line)

    # ── 요약 ──────────────────────────────────────────────────────────────────
    print()
    print("=" * W)
    print("  권장 임계값 요약  (KEEP 중 train IC 최대 / CHECK 포함)")
    print("=" * W)

    for horizon in horizons_in_df:
        h_df  = df[df["horizon"] == horizon]
        label = _HORIZON_LABEL[horizon]
        print(f"\n  [{label}]")

        for condition in h_df["condition"].unique():
            cond_df  = h_df[h_df["condition"] == condition]
            curr_row = cond_df[cond_df["is_current"]]

            if cond_df["verdict"].eq("- (데이터없음)").all():
                print(f"    {condition:<22}: 건너뜀 - records에 컬럼 없음")
                continue

            keep_df = cond_df[cond_df["verdict"].isin(["KEEP", "CHECK"])]
            if keep_df.empty:
                curr_ic = _fv(curr_row["train_ic"].values[0] if not curr_row.empty else None, ".3f")
                mw = " WARN" if (records_hold_days and cond_df["horizon_mismatch"].any()) else ""
                print(f"    {condition:<22}: DROP 통과 없음  IC={curr_ic}{mw}")
                continue

            best    = keep_df.nlargest(1, "train_ic").iloc[0]
            curr_ic = float(curr_row["train_ic"].values[0]) if (not curr_row.empty and pd.notna(curr_row["train_ic"].values[0])) else None
            best_ic = float(best["train_ic"]) if pd.notna(best["train_ic"]) else None
            mw      = " WARN" if best["horizon_mismatch"] else ""

            if best["is_current"]:
                note = "(현재값 유지)"
            elif curr_ic is not None and best_ic is not None and best_ic > curr_ic:
                curr_desc = curr_row["description"].values[0] if not curr_row.empty else "?"
                note = f"<- [{curr_desc}] 대비 IC +{best_ic - curr_ic:.3f}"
            else:
                note = ""

            print(f"    {condition:<22}: {best['description']:<28}  {best['verdict']}  "
                  f"IC={_fv(best_ic, '.3f')}{mw}  {note}")

    print()


# ── 진입점 ────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="scorer.py 조건 임계값 탐색",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "horizon별 권장 hold_days:\n"
            "  short  → 5, 10, 20일\n"
            "  medium → 20, 60일\n"
            "  long   → 60, 120, 240일\n"
        ),
    )
    p.add_argument("--load-records", default=None,
                   help="기존 records parquet 경로 (없으면 신규 수집)")
    p.add_argument("--start",       default="2020-01-01",
                   help="수집 시작일 (--load-records 없을 때, 기본: 2020-01-01)")
    p.add_argument("--end",         default="2024-12-31",
                   help="수집 종료일 (--load-records 없을 때, 기본: 2024-12-31)")
    p.add_argument("--train-end",   default="2022-12-31",
                   help="train 구간 종료일 (기본: 2022-12-31)")
    p.add_argument("--val-end",     default="2024-12-31",
                   help="validation 구간 종료일 (기본: 2024-12-31)")
    p.add_argument("--hold-days",   type=int, default=None,
                   help="records의 보유 기간 거래일 수 (수집 시 사용 + horizon 경고 판단)")
    p.add_argument("--by",          default="marcap", choices=["marcap", "volume", "amount"],
                   help="유니버스 기준 (기본: marcap)")
    p.add_argument("--to",          type=int, default=300,
                   help="유니버스 종목 수 (기본: 300)")
    p.add_argument("--step",        type=int, default=5,
                   help="날짜 샘플링 간격 거래일 (기본: 5)")
    p.add_argument("--horizon",     default=None, choices=["short", "medium", "long"],
                   help="이 horizon의 조건만 실행 (미지정 시 전체)")
    p.add_argument("--conditions",  default=None,
                   help="검색할 조건 목록, 쉼표 구분 (예: rsi_low,vol_surge). 미지정 시 전체")
    p.add_argument("--no-save",          action="store_true",
                   help="CSV 저장 생략")
    # ── 장기 펀더멘털 옵션 ─────────────────────────────────────────────────────
    p.add_argument("--with-fundamentals", action="store_true",
                   help="DART 재무 데이터를 records에 조인해 long 조건 검증 활성화")
    p.add_argument("--fund-start-year",  type=int, default=2019,
                   help="재무 데이터 수집 시작 연도 (기본: 2019)")
    p.add_argument("--fund-end-year",    type=int, default=2024,
                   help="재무 데이터 수집 종료 연도 (기본: 2024)")
    p.add_argument("--fund-force-refresh", action="store_true",
                   help="DART 재무 캐시 무시하고 재조회")
    return p.parse_args()


async def main() -> None:
    args = parse_args()

    # ── 1. records 로드 또는 수집 ─────────────────────────────────────────────
    if args.load_records:
        print(f"[main] 기존 records 로드: {args.load_records}")
        try:
            records = pd.read_parquet(args.load_records)
        except Exception:
            records = pd.read_pickle(args.load_records)
        print(f"[main] {len(records):,}개 레코드 로드 완료")
        print(f"[main] 컬럼: {list(records.columns)}")
    else:
        from discovery.collector import collect_samples
        hold_days_collect = args.hold_days if args.hold_days is not None else 20
        if args.hold_days is None:
            print(f"[main] --hold-days 미지정 → 기본값 {hold_days_collect}일 사용")
        print(f"[main] 신규 수집: {args.start} ~ {args.end}, hold_days={hold_days_collect}")
        records = await collect_samples(
            start=args.start,
            end=args.end,
            hold_days=hold_days_collect,
            universe_by=args.by,
            universe_to=args.to,
            step=args.step,
        )
        hold_days = args.hold_days
        if hold_days is None:
            hold_days = hold_days_collect

    if records.empty:
        print("[main] 레코드가 없습니다. 종료.")
        return

    # ── 1b. 장기 펀더멘털 조인 (--with-fundamentals) ──────────────────────────
    if args.with_fundamentals:
        from discovery.fundamental_loader import load_dart_history, enrich_records_with_fundamentals
        tickers = list(records["ticker"].unique())
        print(f"[main] DART 재무 데이터 로드 중 "
              f"({args.fund_start_year}~{args.fund_end_year}, {len(tickers)}개 종목)...")
        fund_df = await load_dart_history(
            tickers=tickers,
            start_year=args.fund_start_year,
            end_year=args.fund_end_year,
            force_refresh=args.fund_force_refresh,
        )
        if fund_df.empty:
            print("[main] 재무 데이터를 가져오지 못했습니다. "
                  "DART_API_KEY 환경변수를 확인하세요.")
        else:
            before = len(records)
            records = enrich_records_with_fundamentals(records, fund_df)
            covered = records[["roe", "roa", "op_margin", "debt_ratio"]].notna().any(axis=1).sum()
            print(f"[main] 재무 조인 완료 - {covered:,}/{before:,}개 레코드에 재무 데이터 추가")

    # ── 2. variant 필터링 ─────────────────────────────────────────────────────
    variants = _VARIANTS
    if args.horizon:
        variants = [v for v in variants if v.horizon == args.horizon]
    if args.conditions:
        target   = set(args.conditions.split(","))
        variants = [v for v in variants if v.condition in target]
    if not variants:
        print(f"[main] 필터 조건에 해당하는 variant가 없습니다. --horizon / --conditions 확인.")
        return

    # hold_days 감지: --hold-days 미지정 시 파일명에서 추론 시도
    hold_days = args.hold_days
    if hold_days is None and args.load_records:
        import re
        m = re.search(r"hold(\d+)", str(args.load_records))
        if m:
            hold_days = int(m.group(1))
            print(f"[main] records 파일명에서 hold_days={hold_days} 감지")

    # ── 3. 탐색 ───────────────────────────────────────────────────────────────
    print(f"[main] 임계값 탐색 중 - train <= {args.train_end}, val <= {args.val_end}, "
          f"variant {len(variants)}개, hold_days={hold_days}...")
    results = _search(records, variants, args.train_end, args.val_end, hold_days)

    # ── 4. 출력 ───────────────────────────────────────────────────────────────
    _print_results(results, args.train_end, args.val_end, hold_days)

    # ── 5. CSV 저장 ───────────────────────────────────────────────────────────
    if not args.no_save:
        _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        h_suffix = f"_h{hold_days}" if hold_days else ""
        hz_suffix = f"_{args.horizon}" if args.horizon else ""
        fname = f"train{args.train_end[:4]}_val{args.val_end[:4]}{h_suffix}{hz_suffix}_condition_search.csv"
        path  = _RESULTS_DIR / fname
        results.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"[main] 결과 저장: {path}")


if __name__ == "__main__":
    asyncio.run(main())
