"""팩터 리서치 진입점.

Usage:
    # 기본 실행 (2020~2022, 보유 20일)
    python scripts/run_discovery.py \\
      --start 2020-01-01 --end 2022-12-31

    # 단기 급등(10일 +10%) 패턴, 상위 10개 지표만 출력
    python scripts/run_discovery.py \\
      --start 2020-01-01 --end 2022-12-31 \\
      --hold-days 10 --up-threshold 0.10 --top-n 10

    # 시가총액 상위 100개, 캐시 강제 갱신
    python scripts/run_discovery.py \\
      --start 2021-01-01 --end 2023-12-31 \\
      --to 100 --force-refresh

    # 샘플 저장 후 재분석 (캐시된 레코드 재사용)
    python scripts/run_discovery.py --load-records scripts/discovery/results/records.parquet
"""

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv

load_dotenv()

from discovery.analyzer import compute_group_stats, compute_ic, rank_indicators
from discovery.collector import collect_samples
from discovery.report import print_condition_candidates, print_report, save_csv


def _read_records(path: str):
    import pandas as pd

    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.read_pickle(path)


def _write_records(df, path: str) -> str:
    target = Path(path)
    try:
        df.to_parquet(target, index=False)
        return str(target)
    except Exception:
        fallback = target.with_suffix(".pkl")
        df.to_pickle(fallback)
        return str(fallback)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="팩터 리서치 — 지표 예측력 분석")
    p.add_argument("--start",          default="2020-01-01",
                   help="분석 시작일 YYYY-MM-DD (기본: 2020-01-01)")
    p.add_argument("--end",            default="2022-12-31",
                   help="분석 종료일 YYYY-MM-DD (기본: 2022-12-31)")
    p.add_argument("--hold-days",      type=int,   default=20,
                   help="수익률 측정 보유 기간 거래일 수 (기본: 20)")
    p.add_argument("--up-threshold",   type=float, default=0.10,
                   help="급등 기준 수익률 (기본: 0.10 = +10%%)")
    p.add_argument("--down-threshold", type=float, default=-0.08,
                   help="급락 기준 수익률 (기본: -0.08 = -8%%)")
    p.add_argument("--by",             choices=["marcap", "volume"], default="marcap",
                   help="종목 유니버스 기준 (기본: marcap)")
    p.add_argument("--to",             type=int,   default=300,
                   help="유니버스 상위 N개 (기본: 300)")
    p.add_argument("--step",           type=int,   default=5,
                   help="날짜 샘플링 간격 거래일 수 (기본: 5 = 주 1회)")
    p.add_argument("--top-n",          type=int,   default=5,
                   help="조건 후보 제안 지표 수 (기본: 5)")
    p.add_argument("--no-save",        action="store_true",
                   help="CSV 저장 생략")
    p.add_argument("--force-refresh",  action="store_true",
                   help="OHLCV 캐시 무시하고 API 재조회")
    p.add_argument("--load-records",   default=None,
                   help="기존 레코드 parquet 파일 경로 (지정 시 수집 단계 생략)")
    p.add_argument("--save-records",   default=None,
                   help="수집된 레코드를 parquet로 저장할 경로")
    return p.parse_args()


async def main() -> None:
    import pandas as pd

    args = parse_args()

    # ── 1. 레코드 수집 ────────────────────────────────────────────────────────
    if args.load_records:
        print(f"[main] 기존 레코드 로드: {args.load_records}")
        records = _read_records(args.load_records)
        print(f"[main] {len(records):,}개 레코드 로드 완료")
    else:
        records = await collect_samples(
            start=args.start,
            end=args.end,
            hold_days=args.hold_days,
            universe_by=args.by,
            universe_to=args.to,
            force_refresh=args.force_refresh,
            step=args.step,
        )

    if records.empty:
        print("[main] 수집된 레코드가 없습니다. 종료.")
        return

    if args.save_records:
        Path(args.save_records).parent.mkdir(parents=True, exist_ok=True)
        saved_path = _write_records(records, args.save_records)
        print(f"[main] 레코드 저장: {saved_path}")

    # ── 2. 분석 ──────────────────────────────────────────────────────────────
    print("[main] IC 분석 중...")
    ic_df = compute_ic(records)

    print("[main] 그룹 비교 분석 중...")
    group_df = compute_group_stats(
        records,
        up_threshold=args.up_threshold,
        down_threshold=args.down_threshold,
    )

    ranked = rank_indicators(ic_df, group_df)

    # ── 3. 출력 ──────────────────────────────────────────────────────────────
    up_mask   = records["future_return"] >= args.up_threshold
    down_mask = records["future_return"] <= args.down_threshold

    meta = {
        "start":             args.start,
        "end":               args.end,
        "hold_days":         args.hold_days,
        "up_threshold":      args.up_threshold,
        "up_threshold_pct":  args.up_threshold * 100,
        "down_threshold":    args.down_threshold,
        "down_threshold_pct": args.down_threshold * 100,
        "total_records":     len(records),
        "up_n":              int(up_mask.sum()),
        "down_n":            int(down_mask.sum()),
    }

    print_report(ranked, group_df, meta)
    print_condition_candidates(ranked, top_n=args.top_n)

    # ── 4. CSV 저장 ───────────────────────────────────────────────────────────
    if not args.no_save:
        prefix = f"{args.start[:4]}_{args.end[:4]}_hold{args.hold_days}"
        save_csv(ranked, group_df, prefix=prefix)


if __name__ == "__main__":
    asyncio.run(main())
