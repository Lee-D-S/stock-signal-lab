from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"

from analysis_paths import (  # noqa: E402
    BACKTEST_DIR,
    COMPANY_DIR,
    DATA_DIR,
    OBS_ALIGN_DIR,
    OBS_COMMON_MD,
    OBS_CANDLE_DIR,
    OBS_DIR,
    OBS_FOREIGN_FLOW_DIR,
    OBS_FOREIGN_FLOW_MD,
    OBS_SCORE_DIR,
    PATTERN_DIR,
    REVIEW_DIR,
    SNAPSHOT_DIR,
    STRATEGY_DIR,
    STRATEGY_PLAN_DIR,
    OBS_NEW_CONDITION_MD,
    WATCHLIST_CONFIRMED_MD,
    WATCHLIST_CSV,
    WATCHLIST_MD,
    NEW_CONDITION_CONFIRMED_MD,
    NEW_CONDITION_WATCHLIST_MD,
    FOREIGN_FLOW_WATCHLIST_MD,
)


@dataclass(frozen=True)
class Step:
    name: str
    script: str
    args: tuple[str, ...] = ()
    network: bool = False


BACKTEST_STEPS = [
    Step("이벤트 집계/패턴 분석", "collect_event_patterns.py"),
    Step("가설 이벤트 리뷰", "review_hypothesis_events.py"),
    Step("1차 프록시 백테스트", "proxy_backtest_hypotheses.py"),
    Step("기본 실전 백테스트", "realistic_backtest_hypotheses.py", ("--entry-mode", "next_open", "--hold-days", "20")),
    Step("실전 백테스트 전체 조건 비교", "batch_realistic_backtest_hypotheses.py"),
    Step("잔여 갭 분류/전략 조건 초안", "classify_gaps_and_draft_strategy.py"),
]

SNAPSHOT_OUTPUTS = [
    DATA_DIR / "이벤트.csv",
    DATA_DIR / "이벤트_분포_요약.md",
    PATTERN_DIR / "패턴_분석_전체.csv",
    PATTERN_DIR / "패턴_분석_시장국면별.csv",
    PATTERN_DIR / "패턴_분석_5축.csv",
    PATTERN_DIR / "패턴_가설_후보.csv",
    PATTERN_DIR / "패턴_분석_요약.md",
    REVIEW_DIR / "가설_이벤트_검토.csv",
    REVIEW_DIR / "가설_이벤트_요약.csv",
    REVIEW_DIR / "가설_이벤트_검토.md",
    BACKTEST_DIR / "가설_백테스트_입력값.csv",
    BACKTEST_DIR / "가설_대리_백테스트.csv",
    BACKTEST_DIR / "가설_대리_백테스트.md",
    BACKTEST_DIR / "가설_실전_백테스트_거래.csv",
    BACKTEST_DIR / "가설_실전_백테스트_요약.csv",
    BACKTEST_DIR / "가설_실전_백테스트.md",
    BACKTEST_DIR / "가설_실전_백테스트_전체_설정.csv",
    BACKTEST_DIR / "가설_실전_백테스트_전체_설정.md",
    BACKTEST_DIR / "가설_백테스트_갭_분류.csv",
    BACKTEST_DIR / "가설_백테스트_갭_분류.md",
]


REPORT_STEPS = [
    Step("분기 보고서 배치 생성", "regenerate_all_quarterly_reports.py", network=True),
]


def run_step(step: Step, dry_run: bool = False) -> None:
    cmd = [sys.executable, "-u", str(SCRIPTS / step.script), *step.args]
    rel_cmd = " ".join(["python", "-u", f"scripts/{step.script}", *step.args])
    suffix = " [network]" if step.network else ""
    print(f"\n==> {step.name}{suffix}")
    print(f"    {rel_cmd}")
    if dry_run:
        return
    result = subprocess.run(cmd, cwd=ROOT, text=True)
    if result.returncode != 0:
        raise SystemExit(f"step failed: {step.name} ({result.returncode})")


def run_daily(args: argparse.Namespace) -> None:
    if args.refresh_universe:
        universe_args: list[str] = ["--top", str(args.universe_top)]
        if args.date:
            universe_args.extend(["--date", args.date])
        run_step(
            Step("거래대금 상위 유니버스 갱신", "run_daily_universe_refresh.py", tuple(universe_args), network=True),
            dry_run=args.dry_run,
        )

    run_step(
        Step("PER/EPS 현재 스냅샷 및 이익 체력 요약", "analyze_per_eps_valuation.py"),
        dry_run=args.dry_run,
    )

    watch_args: list[str] = []
    if args.date:
        watch_args.extend(["--date", args.date])
    if args.lookback_days:
        watch_args.extend(["--lookback-days", str(args.lookback_days)])
    watch_args.extend(["--delay", str(args.delay)])

    run_step(
        Step("일별 전략 감시 후보 산출", "generate_watchlist_signals.py", tuple(watch_args), network=True),
        dry_run=args.dry_run,
    )
    if args.recheck:
        run_step(
            Step("일별 후보 수급 재조회", "recheck_watchlist_flows.py", ("--delay", str(args.delay)), network=True),
            dry_run=args.dry_run,
        )
        if not args.skip_observation_update:
            run_step(
                Step("확정 후보 관찰 로그 추가", "run_observation_update.py"),
                dry_run=args.dry_run,
            )

    if not args.skip_foreign_flow_observation:
        foreign_flow_args: list[str] = ["--top", str(args.foreign_flow_top), "--pool-size", str(args.foreign_flow_pool_size)]
        if args.date:
            foreign_flow_args.extend(["--date", args.date])
        run_step(
            Step("외국인 연속 순매수 관찰 기록/추적", "run_foreign_flow_observation.py", tuple(foreign_flow_args), network=True),
            dry_run=args.dry_run,
        )

    if not args.skip_scoring_observation:
        scoring_screen_args: list[str] = ["--mode", "screen", "--by", "marcap", "--to", str(args.scoring_pool_size)]
        if args.date:
            scoring_screen_args.extend(["--date", args.date])
        run_step(
            Step("팩터 스코어링 스크린", "run_scoring.py", tuple(scoring_screen_args), network=True),
            dry_run=args.dry_run,
        )
        scoring_obs_args: list[str] = ["--threshold", str(args.scoring_threshold)]
        if args.date:
            scoring_obs_args.extend(["--date", args.date])
        run_step(
            Step("팩터 스코어 관찰 기록/추적", "run_scoring_observation.py", tuple(scoring_obs_args)),
            dry_run=args.dry_run,
        )

    if not args.skip_alignment_observation:
        alignment_args: list[str] = ["--pool-size", str(args.alignment_pool_size)]
        if args.date:
            alignment_args.extend(["--date", args.date])
        run_step(
            Step("단기/장기 정배열 관찰 기록/추적", "run_alignment_observation.py", tuple(alignment_args), network=True),
            dry_run=args.dry_run,
        )


def run_backtest(args: argparse.Namespace) -> None:
    for step in BACKTEST_STEPS:
        if step.script == "classify_gaps_and_draft_strategy.py":
            step_args = [*step.args, "--snapshot-date", args.snapshot_date]
            if args.promote_strategy:
                step_args.append("--promote-strategy")
            run_step(Step(step.name, step.script, tuple(step_args), step.network), dry_run=args.dry_run)
        else:
            run_step(step, dry_run=args.dry_run)
    snapshot_backtest_outputs(args.snapshot_date, dry_run=args.dry_run)


def snapshot_backtest_outputs(snapshot_date: str, dry_run: bool = False) -> None:
    out_dir = SNAPSHOT_DIR / snapshot_date
    print("\n==> 백테스트 산출물 날짜 스냅샷")
    print(f"    {out_dir}")
    if dry_run:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    copied = 0
    for src in SNAPSHOT_OUTPUTS:
        if not src.exists():
            continue
        dst = out_dir / src.name
        shutil.copy2(src, dst)
        copied += 1
    print(f"snapshot_files={copied} snapshot_dir={out_dir}")


def run_full(args: argparse.Namespace) -> None:
    if args.include_reports:
        for step in REPORT_STEPS:
            run_step(step, dry_run=args.dry_run)
    else:
        print("\n==> 분기 보고서 배치 생성 건너뜀")
        print("    필요할 때만 --include-reports를 붙여 540개 보고서 재생성을 실행한다.")
    run_backtest(args)
    if args.daily:
        run_daily(args)


def print_outputs() -> None:
    outputs = [
        COMPANY_DIR / "삼성전자" / "삼성전자_2026_Q1_원인후보_실제분석.md",
        DATA_DIR / "이벤트.csv",
        PATTERN_DIR / "패턴_가설_후보.csv",
        REVIEW_DIR / "가설_이벤트_검토.md",
        BACKTEST_DIR / "가설_대리_백테스트.md",
        BACKTEST_DIR / "가설_실전_백테스트_전체_설정.md",
        STRATEGY_PLAN_DIR / "전략_조건_초안.md",
        WATCHLIST_MD,
        WATCHLIST_CONFIRMED_MD,
        NEW_CONDITION_WATCHLIST_MD,
        NEW_CONDITION_CONFIRMED_MD,
        FOREIGN_FLOW_WATCHLIST_MD,
        OBS_COMMON_MD,
        OBS_NEW_CONDITION_MD,
        OBS_FOREIGN_FLOW_MD,
    ]
    print("\n주요 산출물:")
    for path in outputs:
        status = "exists" if path.exists() else "missing"
        print(f"- {status}: {path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="주가 변동 원인 분석 파이프라인 실행기")
    parser.add_argument(
        "--mode",
        choices=["daily", "backtest", "full", "outputs"],
        default="daily",
        help="daily=오늘 후보 산출, backtest=로컬 이벤트/가설 검증 재계산, full=보고서 선택 재생성 후 백테스트",
    )
    parser.add_argument("--date", help="daily 기준일 YYYY-MM-DD. 생략하면 KIS 최신 거래일")
    parser.add_argument("--lookback-days", type=int, default=220)
    parser.add_argument("--delay", type=float, default=0.35)
    parser.add_argument("--recheck", action="store_true", help="daily 후 KIS 수급 재조회까지 실행")
    parser.add_argument("--refresh-universe", action="store_true", help="daily 전에 거래대금 상위 유니버스를 갱신")
    parser.add_argument("--universe-top", type=int, default=30, help="거래대금 상위 유니버스 종목 수")
    parser.add_argument("--skip-observation-update", action="store_true", help="recheck 후 확정 후보 관찰 로그 자동 추가를 건너뜀")
    parser.add_argument("--skip-foreign-flow-observation", action="store_true", help="외국인 연속 순매수 관찰 기록/추적을 건너뜀")
    parser.add_argument("--foreign-flow-top", type=int, default=50, help="외국인 연속 순매수 최대 기록 후보 수")
    parser.add_argument("--foreign-flow-pool-size", type=int, default=120, help="외국인 연속 순매수 스캔 대상 거래대금 상위 후보 수")
    parser.add_argument("--skip-scoring-observation", action="store_true", help="팩터 스코어링 스크린 + 관찰 기록/추적을 건너뜀")
    parser.add_argument("--scoring-threshold", type=float, default=0.60, help="팩터 스코어 관찰 기록 임계값 (기본: 0.60)")
    parser.add_argument("--scoring-pool-size", type=int, default=300, help="팩터 스코어링 스캔 종목 수 (기본: 300)")
    parser.add_argument("--skip-alignment-observation", action="store_true", help="단기/장기 정배열 관찰 기록/추적을 건너뜀")
    parser.add_argument("--alignment-pool-size", type=int, default=300, help="정배열 스캔 시총 상위 N개 (기본: 300)")
    parser.add_argument("--snapshot-date", default=date.today().isoformat(), help="backtest 산출물 스냅샷 기준일 YYYY-MM-DD")
    parser.add_argument("--promote-strategy", action="store_true", help="backtest로 만든 조건 초안을 active 전략 조건으로 승격")
    parser.add_argument("--include-reports", action="store_true", help="full 모드에서 분기 보고서 배치 생성까지 실행")
    parser.add_argument("--daily", action="store_true", help="full 모드 마지막에 일별 후보 산출까지 실행")
    parser.add_argument("--dry-run", action="store_true", help="실행할 단계만 출력하고 실제 실행하지 않음")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "daily":
        run_daily(args)
    elif args.mode == "backtest":
        run_backtest(args)
    elif args.mode == "full":
        run_full(args)
    elif args.mode == "outputs":
        print_outputs()
        return
    print_outputs()


if __name__ == "__main__":
    main()
