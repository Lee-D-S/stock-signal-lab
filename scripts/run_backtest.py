"""백테스트 진입점.

Usage:
    # 장기 정배열 전략 (2020~2022 Train 구간)
    python scripts/run_backtest.py \\
      --ma-align 60,120,240 \\
      --start 2020-01-01 --end 2022-12-31 \\
      --hold-days 20 --max-positions 10

    # RSI 과매도 반등 전략
    python scripts/run_backtest.py \\
      --ma-align 60,120,240 --rsi-max 40 \\
      --start 2020-01-01 --end 2022-12-31 \\
      --hold-days 10 --stop-loss -0.05 --take-profit 0.10

    # MACD 골든크로스 + OBV 상승
    python scripts/run_backtest.py \\
      --macd-cross-up --obv-rising \\
      --start 2020-01-01 --end 2022-12-31 \\
      --hold-days 15

    # 기존 OHLCV 캐시 재사용 (API 호출 없음)
    python scripts/run_backtest.py \\
      --ma-align 60,120,240 --rsi-max 50 \\
      --start 2021-01-01 --end 2021-12-31
"""

import argparse
import asyncio
import io
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Windows cp949 터미널에서 한글/특수문자 출력 가능하도록 강제 UTF-8 설정
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from screener_lib.indicators import add_all_args, all_labels       # noqa: E402
from screener_lib.indicators.fundamentals import needs_dart         # noqa: E402
from screener_lib.indicators.valuation import needs_valuation       # noqa: E402
from screener_lib.universe import get_stock_universe                # noqa: E402

from backtest.data_loader import load_universe_ohlcv               # noqa: E402
from backtest.engine import run_backtest                            # noqa: E402
from backtest.report import print_report, save_equity_csv, save_trades_csv  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="백테스트 — 스크리닝 조건 조합을 과거 데이터로 검증",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    g_univ = p.add_argument_group("유니버스")
    g_univ.add_argument("--by",  choices=["marcap", "volume"], default="marcap",
                        help="종목 선정 기준 (기본: marcap)")
    g_univ.add_argument("--to",  type=int, default=300,
                        help="유니버스 상위 N개 (기본: 300)")

    g_bt = p.add_argument_group("백테스트 파라미터")
    g_bt.add_argument("--start",           default="2020-01-01",
                      help="백테스트 시작일 YYYY-MM-DD (기본: 2020-01-01)")
    g_bt.add_argument("--end",             default="2022-12-31",
                      help="백테스트 종료일 YYYY-MM-DD (기본: 2022-12-31)")
    g_bt.add_argument("--hold-days",       type=int,   default=20,
                      help="최대 보유 거래일 수 (기본: 20)")
    g_bt.add_argument("--max-positions",   type=int,   default=10,
                      help="최대 동시 보유 포지션 수 (기본: 10)")
    g_bt.add_argument("--stop-loss",       type=float, default=-0.05,
                      help="손절 기준 수익률 (기본: -0.05 = -5%%)")
    g_bt.add_argument("--take-profit",     type=float, default=0.10,
                      help="익절 기준 수익률 (기본: 0.10 = +10%%)")
    g_bt.add_argument("--initial-capital", type=float, default=10_000_000,
                      help="초기 자본금 원 (기본: 10,000,000)")

    # 기술적 조건 (screener_lib 재사용)
    add_all_args(p)

    g_etc = p.add_argument_group("기타")
    g_etc.add_argument("--force-refresh", action="store_true",
                       help="OHLCV 캐시 무시하고 API 재조회")
    g_etc.add_argument("--no-save",       action="store_true",
                       help="결과 CSV 저장 생략")

    return p.parse_args()


async def main(args: argparse.Namespace) -> None:
    # ── 0. 지원 불가 조건 경고 ──────────────────────────────────────────
    if needs_dart(args):
        print("[경고] --roe-min 등 DART 재무 조건은 백테스트에서 역사적 데이터 미지원 → 해당 조건 무시됨")
        print("       과거 재무제표 데이터 연동 시 universe_loader.py 에서 DART 연동 구현 필요")
    if needs_valuation(args):
        print("[경고] --per-max 등 밸류에이션 조건은 백테스트에서 역사적 PER/PBR 미지원 → 해당 조건 무시됨")

    conditions_labels = all_labels(args)
    conditions_desc   = " & ".join(conditions_labels) if conditions_labels else "조건 없음"

    print()
    print("[ 백테스트 ]")
    print(f"  조건: {conditions_desc}")
    print(f"  기간: {args.start} ~ {args.end}")
    print(
        f"  보유일: {args.hold_days}일  "
        f"포지션: {args.max_positions}개  "
        f"손절: {args.stop_loss:.0%}  "
        f"익절: {args.take_profit:.0%}"
    )
    print()

    if not conditions_labels:
        print("[경고] 조건이 하나도 지정되지 않았습니다. 모든 종목이 매일 신호를 발생시킵니다.")
        print("       예: --ma-align 60,120,240 --rsi-max 50")
        print()

    # ── 1. 유니버스 조회 ──────────────────────────────────────────────
    print(f"[main] 유니버스 조회 중... (by={args.by}, top={args.to})")
    universe = await get_stock_universe(by=args.by)
    tickers  = [s["ticker"] for s in universe[: args.to]]
    print(f"[main] {len(tickers)}개 종목 확보")

    # ── 2. OHLCV 로드 ─────────────────────────────────────────────────
    # 지표 warm-up 기간: MA240 = 240거래일 ≈ 365 캘린더일
    # 추가로 60일 여유를 더해 로드 시작일을 설정
    load_start = (
        datetime.strptime(args.start, "%Y-%m-%d") - timedelta(days=425)
    ).strftime("%Y-%m-%d")

    print(f"[main] OHLCV 로드 시작 ({load_start} ~ {args.end})...")
    universe_data = await load_universe_ohlcv(
        tickers=tickers,
        start=load_start,
        end=args.end,
        force_refresh=args.force_refresh,
    )

    if not universe_data:
        print("[main] OHLCV 데이터 없음. 종료.")
        return

    # ── 3. 백테스트 실행 ─────────────────────────────────────────────
    print()
    portfolio = run_backtest(
        universe_data=universe_data,
        conditions=args,
        start=args.start,
        end=args.end,
        hold_days=args.hold_days,
        max_positions=args.max_positions,
        initial_capital=args.initial_capital,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit,
    )

    # ── 4. 결과 출력 ─────────────────────────────────────────────────
    print_report(
        portfolio,
        conditions_desc,
        {
            "start":           args.start,
            "end":             args.end,
            "hold_days":       args.hold_days,
            "max_positions":   args.max_positions,
            "stop_loss":       args.stop_loss,
            "take_profit":     args.take_profit,
            "initial_capital": args.initial_capital,
        },
    )

    # ── 5. CSV 저장 ───────────────────────────────────────────────────
    if not args.no_save and portfolio.trades:
        out_dir = Path("scripts/backtest/results")
        out_dir.mkdir(parents=True, exist_ok=True)
        prefix = f"{args.start[:7].replace('-','')}_{args.end[:7].replace('-','')}"
        save_trades_csv(portfolio, out_dir / f"{prefix}_trades.csv")
        save_equity_csv(portfolio, out_dir / f"{prefix}_equity.csv")


if __name__ == "__main__":
    _args = parse_args()
    asyncio.run(main(_args))
