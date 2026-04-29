"""팩터 스코어링 진입점.

Usage:
    # Train 구간 스코어 통계 -> 임계값 결정
    python scripts/run_scoring.py --mode threshold \\
      --start 2020-01-01 --end 2022-12-31 --hold-days 20

    # 오늘 시총 상위 300개 스코어링 (임계값 60% 이상)
    python scripts/run_scoring.py --mode screen --threshold 0.60

    # Factor Research IC 결과로 군 가중치 조정
    python scripts/run_scoring.py --mode screen --threshold 0.70 \\
      --ic-weights scripts/discovery/results/2020_2022_hold20_ic_ranking.csv

    # threshold 결과 저장 후 재분석 (API 재호출 없음)
    python scripts/run_scoring.py --mode threshold \\
      --start 2020-01-01 --end 2022-12-31 \\
      --save-raw scripts/scoring/results/raw.parquet

    python scripts/run_scoring.py --mode threshold \\
      --load-raw scripts/scoring/results/raw.parquet --hold-days 10
"""

import argparse
import asyncio
import sys
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv

load_dotenv()

from scoring.scorer import DEFAULT_WEIGHTS, GROUP_NAMES, score_ticker    # noqa: E402
from scoring.threshold import (                                           # noqa: E402
    compute_bin_stats, compute_threshold_stats, print_stats, suggest_threshold,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="팩터 스코어링")
    p.add_argument("--mode",          choices=["threshold", "screen"], default="screen",
                   help="threshold: Train 구간 통계 | screen: 오늘 스크리닝")
    p.add_argument("--start",         default="2020-01-01",
                   help="threshold 모드: 분석 시작일 (기본: 2020-01-01)")
    p.add_argument("--end",           default="2022-12-31",
                   help="threshold 모드: 분석 종료일 (기본: 2022-12-31)")
    p.add_argument("--hold-days",     type=int,   default=20,
                   help="수익률 측정 보유 거래일 수 (기본: 20)")
    p.add_argument("--by",            choices=["marcap", "volume", "amount"], default="marcap",
                   help="유니버스 기준 (기본: marcap)")
    p.add_argument("--to",            type=int,   default=300,
                   help="유니버스 상위 N개 (기본: 300)")
    p.add_argument("--step",          type=int,   default=5,
                   help="threshold 모드: 날짜 샘플링 간격 (기본: 5 = 주 1회)")
    p.add_argument("--threshold",     type=float, default=None,
                   help="screen 모드: 이 스코어 이상만 출력 (기본: 0.60)")
    p.add_argument("--top-n",         type=int,   default=20,
                   help="screen 모드: 상위 N개 출력 (기본: 20)")
    p.add_argument("--ic-weights",    default=None,
                   help="Factor Research IC CSV 경로 (군 가중치 자동 조정)")
    p.add_argument("--force-refresh", action="store_true",
                   help="OHLCV 캐시 무시하고 API 재조회")
    p.add_argument("--save-raw",      default=None,
                   help="threshold 모드: 원시 레코드 parquet 저장 경로")
    p.add_argument("--load-raw",      default=None,
                   help="threshold 모드: 기존 레코드 로드 (수집 단계 생략)")
    return p.parse_args()


async def main() -> None:
    import pandas as pd

    args = parse_args()

    # 군 가중치 결정
    weights = None
    if args.ic_weights:
        from scoring.weight_tuner import load_ic_weights, print_weights
        weights = load_ic_weights(args.ic_weights)
        print_weights(weights)
    else:
        weights = dict(DEFAULT_WEIGHTS)

    if args.mode == "threshold":
        await _run_threshold(args, weights)
    else:
        await _run_screen(args, weights)


async def _run_threshold(args: argparse.Namespace, weights: dict) -> None:
    import pandas as pd

    if args.load_raw:
        print(f"[threshold] 기존 레코드 로드: {args.load_raw}")
        raw_df   = pd.read_parquet(args.load_raw)
        stats_df = compute_bin_stats(raw_df)
        print(f"[threshold] {len(raw_df):,}개 레코드 로드 완료")
    else:
        raw_df, stats_df = await compute_threshold_stats(
            start=args.start,
            end=args.end,
            hold_days=args.hold_days,
            universe_by=args.by,
            universe_to=args.to,
            force_refresh=args.force_refresh,
            step=args.step,
            weights=weights,
        )

    if raw_df.empty:
        print("[threshold] 분석 데이터 없음. 종료.")
        return

    if args.save_raw:
        Path(args.save_raw).parent.mkdir(parents=True, exist_ok=True)
        raw_df.to_parquet(args.save_raw, index=False)
        print(f"[threshold] 원시 레코드 저장: {args.save_raw}")

    print_stats(stats_df, args.hold_days)

    rec = suggest_threshold(stats_df)
    if rec is not None:
        print(f"[threshold] 권장 임계값: {rec:.0%}")
        print(f"  -> --threshold {rec:.2f}  으로 screen 모드 실행 가능")
    else:
        print("[threshold] 권장 임계값 없음 (샘플 부족 또는 모든 구간 기준 미달)")

    # CSV 저장
    out_dir = Path("scripts/scoring/results")
    out_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"{args.start[:4]}_{args.end[:4]}_hold{args.hold_days}"
    csv_path = out_dir / f"{prefix}_threshold_stats.csv"
    stats_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"[threshold] 통계 저장: {csv_path}")


async def _run_screen(args: argparse.Namespace, weights: dict) -> None:
    import asyncio

    from screener_lib.data import get_ohlcv
    from screener_lib.universe import get_stock_universe

    threshold = args.threshold if args.threshold is not None else 0.60

    print(f"[screen] 유니버스 조회 중... (by={args.by}, top={args.to})")
    universe = await get_stock_universe(by=args.by)
    stocks   = universe[:args.to]
    print(f"[screen] {len(stocks)}개 종목 스코어 계산 중 (임계값: {threshold:.0%})")

    results = []
    for i, stock in enumerate(stocks, 1):
        ticker = stock["ticker"]
        df, _  = await get_ohlcv(ticker)
        if df.empty or len(df) < 60:
            continue

        score, details = score_ticker(df, weights=weights)
        results.append({
            "ticker":  ticker,
            "name":    stock.get("name", ""),
            "score":   score,
            "details": details,
        })

        if i % 50 == 0:
            print(f"[screen] {i}/{len(stocks)} 완료")

        await asyncio.sleep(0.1)

    if not results:
        print("[screen] 결과 없음")
        return

    results.sort(key=lambda x: x["score"], reverse=True)
    candidates = [r for r in results if r["score"] >= threshold][: args.top_n]

    _print_screen_results(candidates, threshold, len(results))

    # CSV 저장
    out_dir = Path("scripts/scoring/results")
    out_dir.mkdir(parents=True, exist_ok=True)
    from datetime import date
    csv_path = out_dir / f"screen_{date.today().isoformat()}.csv"
    _save_screen_csv(results, threshold, csv_path)
    print(f"[screen] 전체 결과 저장: {csv_path}")


def _print_screen_results(
    candidates: list[dict],
    threshold: float,
    total: int,
) -> None:
    _GROUP_KO = {
        "momentum":    "모멘텀",
        "trend":       "추세",
        "value":       "가치",
        "fundamental": "펀더",
        "volatility":  "변동",
    }

    print()
    print("=" * 72)
    print(f"[ 팩터 스코어링 결과 ]  임계값: {threshold:.0%}  전체: {total}개 분석")
    print("=" * 72)

    if not candidates:
        print(f"  임계값({threshold:.0%}) 이상인 종목 없음")
        return

    grp_cols = "  ".join(f"{_GROUP_KO[g]:>4}" for g in GROUP_NAMES)
    print(f"{'순위':>3}  {'코드':>6}  {'종목명':<16}  {'스코어':>6}  {grp_cols}")
    print("-" * 72)

    def _fmt(d: dict, gname: str) -> str:
        fr = d.get(gname, {}).get("fill_rate")
        return f"{fr:.0%}" if fr is not None else " N/A"

    for rank, r in enumerate(candidates, 1):
        d = r["details"]
        grp_vals = "  ".join(f"{_fmt(d, g):>4}" for g in GROUP_NAMES)
        print(f"{rank:>3}  {r['ticker']:>6}  {r['name']:<16}  {r['score']:>6.1%}  {grp_vals}")

    print()


def _save_screen_csv(results: list[dict], threshold: float, csv_path: Path) -> None:
    rows = []
    for r in results:
        row = {
            "ticker": r["ticker"],
            "name":   r["name"],
            "score":  round(r["score"], 4),
            "above_threshold": r["score"] >= threshold,
        }
        for gname, d in r["details"].items():
            row[f"{gname}_fill"] = round(d["fill_rate"], 4) if d["fill_rate"] is not None else None
            row[f"{gname}_met"]  = d["met"]
            row[f"{gname}_total"] = d["total"]
        rows.append(row)

    import pandas as pd
    pd.DataFrame(rows).to_csv(csv_path, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    asyncio.run(main())
