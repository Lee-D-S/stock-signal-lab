from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from core.agents.free_base import AgentContext, Candidate  # noqa: E402
from core.agents.free_pipeline import (  # noqa: E402
    FreeAgentPipeline,
    discover_candidates_from_csv,
    load_portfolio,
)
from scripts.build_portfolio_snapshot import build_portfolio_snapshot  # noqa: E402


DEFAULT_RESEARCH_ROOT = ROOT / "legacy" / "research_data" / "ai 주가 변동 원인 분석" / "00_기업별분석"
DEFAULT_DISCOVERY_ROOT = ROOT / "legacy" / "research_data" / "ai 주가 변동 원인 분석" / "07_전략신호"
DEFAULT_OUTPUT_DIR = ROOT / "legacy" / "data" / "agent_runs"
PORTFOLIO_DIR = ROOT / "legacy" / "data" / "portfolios"
TEST_PORTFOLIOS = {
    "balanced": PORTFOLIO_DIR / "test_inputs" / "balanced.json",
    "cash-heavy": PORTFOLIO_DIR / "test_inputs" / "cash_heavy.json",
    "growth": PORTFOLIO_DIR / "test_inputs" / "growth.json",
    "risk-test": PORTFOLIO_DIR / "test_inputs" / "risk_test.json",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="LLM/API 없이 무료 규칙 기반 투자 Agent 파이프라인을 실행합니다."
    )
    parser.add_argument(
        "--date",
        "--as-of-date",
        dest="date",
        default=date.today().isoformat(),
        help="투자 판단 기준일/as-of date (YYYY-MM-DD). 미래 날짜는 기본 차단됩니다.",
    )
    parser.add_argument(
        "--candidate",
        action="append",
        default=[],
        help="TICKER[:NAME[:AMOUNT]] 형식의 후보 입력입니다. 여러 번 지정할 수 있습니다.",
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="--candidate가 없을 때 최근 CSV 파일에서 후보를 찾습니다.",
    )
    parser.add_argument("--discover-limit", type=int, default=10)
    parser.add_argument("--portfolio-json", type=Path)
    parser.add_argument(
        "--test-portfolio",
        choices=sorted(TEST_PORTFOLIOS),
        help="Use a test portfolio preset. Cannot be combined with --portfolio-json.",
    )
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_ROOT)
    parser.add_argument("--discovery-root", type=Path, default=DEFAULT_DISCOVERY_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--default-amount", type=int, default=settings.max_order_amount)
    parser.add_argument("--max-position-pct", type=float, default=0.25)
    parser.add_argument("--max-sector-pct", type=float, default=0.40)
    parser.add_argument("--max-daily-new-buy-amount", type=int, default=settings.max_daily_spend)
    parser.add_argument("--stale-signal-days", type=int, default=3)
    parser.add_argument(
        "--allow-future-date",
        action="store_true",
        help="테스트 목적으로 오늘보다 미래인 기준일 실행을 허용합니다. 실운영에서는 사용하지 마세요.",
    )
    parser.add_argument(
        "--no-require-research-file",
        action="store_true",
        help="준법 승인에 기존 리서치 파일을 필수로 요구하지 않습니다.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.date)
    today = date.today()
    if run_date > today and not args.allow_future_date:
        raise SystemExit(
            f"--date는 투자 판단 기준일(as-of date)입니다. "
            f"미래 날짜({run_date.isoformat()})는 사용할 수 없습니다. "
            f"오늘 기준일은 {today.isoformat()}입니다. "
            "테스트 목적이면 --allow-future-date를 명시하세요."
        )
    run_id = datetime.now().strftime("%H%M%S_%f")
    candidates = [parse_candidate(raw, args.default_amount) for raw in args.candidate]
    if not candidates and args.discover:
        candidates = discover_candidates_from_csv(args.discovery_root, args.discover_limit)

    portfolio_path = resolve_portfolio_path(args, run_date, run_id)
    portfolio, cash = load_portfolio(portfolio_path)
    context = AgentContext(
        run_date=run_date,
        candidates=candidates,
        run_id=run_id,
        portfolio=portfolio,
        cash=cash,
        output_dir=args.output_dir,
        research_root=args.research_root,
        config={
            "default_suggested_amount": args.default_amount,
            "max_position_pct": args.max_position_pct,
            "max_sector_pct": args.max_sector_pct,
            "max_order_amount": settings.max_order_amount,
            "max_daily_new_buy_amount": args.max_daily_new_buy_amount,
            "stale_signal_days": args.stale_signal_days,
            "require_research_file": not args.no_require_research_file,
        },
    )

    results = FreeAgentPipeline().run(context)
    print(f"실행 폴더={context.run_dir}")
    for result in results:
        print(f"{result.agent}: {result.status} - {result.summary}")


def resolve_portfolio_path(args: argparse.Namespace, run_date: date, run_id: str) -> Path | None:
    if args.portfolio_json and args.test_portfolio:
        raise SystemExit("--portfolio-json and --test-portfolio cannot be used together.")
    if args.test_portfolio:
        input_path = TEST_PORTFOLIOS[args.test_portfolio]
        if not input_path.exists():
            raise SystemExit(f"test portfolio input file not found: {input_path}")
        output_path = args.output_dir / run_date.isoformat() / run_id / f"test_portfolio_{args.test_portfolio}_snapshot.json"
        asyncio.run(build_portfolio_snapshot(input_path, output_path))
        return output_path
    return args.portfolio_json


def parse_candidate(raw: str, default_amount: int) -> Candidate:
    parts = [part.strip() for part in raw.split(":")]
    ticker = parts[0]
    name = parts[1] if len(parts) >= 2 else ""
    amount = int(parts[2]) if len(parts) >= 3 and parts[2] else default_amount
    return Candidate(
        ticker=ticker,
        name=name,
        source="cli",
        source_type="manual",
        suggested_amount=amount,
    )


if __name__ == "__main__":
    main()
