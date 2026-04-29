from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
COMPANY_DIR = BASE_DIR / "00_기업별분석"
DATA_DIR = BASE_DIR / "03_원천데이터"
PATTERN_DIR = BASE_DIR / "04_패턴분석"
REVIEW_DIR = BASE_DIR / "05_가설검토"
BACKTEST_DIR = BASE_DIR / "06_백테스트"
STRATEGY_DIR = BASE_DIR / "07_전략신호"
OBS_DIR = BASE_DIR / "08_관찰기록"


@dataclass(frozen=True)
class Step:
    name: str
    script: str
    args: tuple[str, ...] = ()
    network: bool = False


BACKTEST_STEPS = [
    Step("이벤트 집계/패턴 분석", "tmp_collect_event_patterns.py"),
    Step("가설 이벤트 리뷰", "tmp_review_hypothesis_events.py"),
    Step("1차 프록시 백테스트", "tmp_proxy_backtest_hypotheses.py"),
    Step("기본 실전 백테스트", "tmp_realistic_backtest_hypotheses.py", ("--entry-mode", "next_open", "--hold-days", "20")),
    Step("실전 백테스트 전체 조건 비교", "tmp_batch_realistic_backtest_hypotheses.py"),
    Step("잔여 갭 분류/전략 조건 초안", "tmp_classify_gaps_and_draft_strategy.py"),
]


REPORT_STEPS = [
    Step("분기 보고서 배치 생성", "tmp_regenerate_all_quarterly_reports.py", network=True),
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
    watch_args: list[str] = []
    if args.date:
        watch_args.extend(["--date", args.date])
    if args.lookback_days:
        watch_args.extend(["--lookback-days", str(args.lookback_days)])
    watch_args.extend(["--delay", str(args.delay)])

    run_step(
        Step("일별 전략 감시 후보 산출", "tmp_generate_watchlist_signals.py", tuple(watch_args), network=True),
        dry_run=args.dry_run,
    )
    if args.recheck:
        run_step(
            Step("일별 후보 수급 재조회", "tmp_recheck_watchlist_flows.py", ("--delay", str(args.delay)), network=True),
            dry_run=args.dry_run,
        )


def run_backtest(args: argparse.Namespace) -> None:
    for step in BACKTEST_STEPS:
        run_step(step, dry_run=args.dry_run)


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
        STRATEGY_DIR / "전략_조건_초안.md",
        STRATEGY_DIR / "관심종목_시그널_후보.md",
        STRATEGY_DIR / "관심종목_시그널_후보_확정.md",
        OBS_DIR / "관찰_로그.md",
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
