"""Walk-forward 분석 진입점.

슬라이딩 윈도우로 백테스트 결과의 시간 일관성 검증.
동일 조건이 여러 구간에 걸쳐 일관되게 동작하는지 확인한다.

Usage:
    # 장기 정배열 + RSI 과매도 전략: 훈련 3년 / 검증 1년
    python scripts/run_walkforward.py \\
      --ma-align 60,120,240 --rsi-max 40 \\
      --start 2020-01-01 --end 2024-12-31 \\
      --train-years 3 --test-years 1 \\
      --hold-days 10 --stop-loss -0.05 --take-profit 0.10

    # 훈련 2년 / 검증 1년, MACD 골든크로스
    python scripts/run_walkforward.py \\
      --macd-cross-up --obv-rising \\
      --start 2020-01-01 --end 2024-12-31 \\
      --train-years 2 --test-years 1

    # 생존 편향 제거 (DART 유니버스)
    python scripts/run_walkforward.py \\
      --ma-align 60,120,240 \\
      --start 2020-01-01 --end 2024-12-31 \\
      --train-years 3 --test-years 1 \\
      --historical-universe
"""

import argparse
import asyncio
import io
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from screener_lib.indicators import add_all_args, all_labels       # noqa: E402
from screener_lib.indicators.fundamentals import needs_dart         # noqa: E402
from screener_lib.indicators.valuation import needs_valuation       # noqa: E402
from screener_lib.universe import get_stock_universe                # noqa: E402

from backtest.data_loader import load_universe_ohlcv               # noqa: E402
from backtest.engine import run_backtest                            # noqa: E402
from backtest.metrics import compute_metrics                        # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Walk-forward 분석 — 슬라이딩 윈도우 시간 일관성 검증",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    g_univ = p.add_argument_group("유니버스")
    g_univ.add_argument("--by",  choices=["marcap", "volume"], default="marcap",
                        help="종목 선정 기준 (기본: marcap)")
    g_univ.add_argument("--to",  type=int, default=300,
                        help="유니버스 상위 N개 (기본: 300)")
    g_univ.add_argument("--historical-universe", action="store_true",
                        help="DART 공시 기반 역사적 유니버스 사용 (생존 편향 제거)")
    g_univ.add_argument("--max-tickers", type=int, default=None,
                        help="--historical-universe 시 최대 종목 수 (기본: 전체)")

    g_wf = p.add_argument_group("Walk-forward 파라미터")
    g_wf.add_argument("--start",       default="2020-01-01",
                      help="전체 시작일 YYYY-MM-DD (기본: 2020-01-01)")
    g_wf.add_argument("--end",         default="2024-12-31",
                      help="전체 종료일 YYYY-MM-DD (기본: 2024-12-31)")
    g_wf.add_argument("--train-years", type=int, default=3,
                      help="훈련 구간 연수 (기본: 3)")
    g_wf.add_argument("--test-years",  type=int, default=1,
                      help="검증 구간 연수 (기본: 1)")

    g_bt = p.add_argument_group("백테스트 파라미터")
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

    add_all_args(p)

    g_etc = p.add_argument_group("기타")
    g_etc.add_argument("--force-refresh", action="store_true",
                       help="OHLCV / DART 캐시 무시하고 재조회")
    g_etc.add_argument("--no-save",       action="store_true",
                       help="결과 CSV 저장 생략")

    return p.parse_args()


def calculate_windows(
    start: str,
    end: str,
    train_years: int,
    test_years: int,
) -> list[dict]:
    """슬라이딩 윈도우 목록 계산.

    Returns:
        [{"train_start", "train_end", "test_start", "test_end"}, ...]
    """
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)

    windows = []
    win_start = s

    while True:
        # train 구간 끝 = win_start + train_years - 1일
        train_end_year  = win_start.year + train_years
        train_end_month = win_start.month
        train_end_day   = win_start.day
        try:
            train_end = date(train_end_year, train_end_month, train_end_day) - timedelta(days=1)
        except ValueError:
            break

        # test 구간
        test_start = train_end + timedelta(days=1)
        try:
            test_end = date(test_start.year + test_years, test_start.month, test_start.day) - timedelta(days=1)
        except ValueError:
            break

        if test_start > e:
            break

        windows.append({
            "train_start": win_start.isoformat(),
            "train_end":   train_end.isoformat(),
            "test_start":  test_start.isoformat(),
            "test_end":    min(test_end, e).isoformat(),
        })

        # 다음 윈도우: test_years 만큼 앞으로 이동
        try:
            win_start = date(win_start.year + test_years, win_start.month, win_start.day)
        except ValueError:
            break

    return windows


def _fmt(val: float | None, fmt: str = "+.1%") -> str:
    if val is None:
        return "  N/A"
    return f"{val:{fmt}}"


def print_walkforward_report(
    window_results: list[dict],
    conditions_desc: str,
    args: argparse.Namespace,
) -> None:
    print()
    print("=" * 90)
    print("[ Walk-forward 분석 결과 ]")
    print(f"  조건: {conditions_desc}")
    print(
        f"  전체 기간: {args.start} ~ {args.end}"
        f"  |  훈련: {args.train_years}년 / 검증: {args.test_years}년"
    )
    print(
        f"  보유일: {args.hold_days}일  |  포지션: {args.max_positions}개"
        f"  |  손절: {args.stop_loss:.0%}  |  익절: {args.take_profit:.0%}"
    )
    print("=" * 90)

    header = (
        f"{'창':>2}  {'훈련 구간':<22}  {'검증 구간':<22}"
        f"  {'거래':>4}  {'승률':>6}  {'CAGR':>7}  {'MDD':>6}  {'Sharpe':>7}"
    )
    print(header)
    print("-" * 90)

    cagrs, sharpes, win_rates, trade_counts = [], [], [], []

    for i, wr in enumerate(window_results, 1):
        m    = wr["metrics"]
        win  = wr["window"]
        n    = m["total_trades"]
        wr_  = m["win_rate"]
        cagr = m["cagr"]
        mdd  = m["mdd"]
        sh   = m["sharpe"]

        cagrs.append(cagr)
        sharpes.append(sh)
        win_rates.append(wr_)
        trade_counts.append(n)

        print(
            f"{i:>2}  "
            f"{win['train_start']}~{win['train_end'][:4]}  "
            f"  {win['test_start']}~{win['test_end'][:4]}  "
            f"  {n:>4}  "
            f"{wr_:>6.1%}  "
            f"{_fmt(cagr):>7}  "
            f"{mdd:>6.2%}  "
            f"{sh:>7.2f}"
        )

    if not window_results:
        print("  결과 없음")
        print("=" * 90)
        return

    # 집계
    n_wins = len(window_results)
    print("-" * 90)
    avg_cagr  = sum(cagrs) / n_wins
    avg_sh    = sum(sharpes) / n_wins
    avg_wr    = sum(win_rates) / n_wins
    avg_tr    = sum(trade_counts) / n_wins

    print(
        f"{'평균':>2}  {'':22}  {'':22}"
        f"  {avg_tr:>4.0f}  "
        f"{avg_wr:>6.1%}  "
        f"{_fmt(avg_cagr):>7}  "
        f"{'':>6}  "
        f"{avg_sh:>7.2f}"
    )
    print("=" * 90)

    # 일관성 평가
    cagr_positive = sum(1 for c in cagrs if c > 0)
    sharpe_above1 = sum(1 for s in sharpes if s > 1.0)
    winrate_above50 = sum(1 for w in win_rates if w > 0.50)

    print(f"  CAGR 양수 비율: {cagr_positive}/{n_wins}  "
          f"Sharpe>1.0 비율: {sharpe_above1}/{n_wins}  "
          f"승률>50%% 비율: {winrate_above50}/{n_wins}")

    consistency = (cagr_positive + sharpe_above1 + winrate_above50) / (n_wins * 3)
    verdict = "일관성 우수" if consistency >= 0.7 else ("보통" if consistency >= 0.5 else "일관성 부족")
    print(f"  종합 일관성 점수: {consistency:.0%}  →  {verdict}")
    print("=" * 90)


def save_walkforward_csv(window_results: list[dict], path: Path) -> None:
    import pandas as pd

    rows = []
    for i, wr in enumerate(window_results, 1):
        m   = wr["metrics"]
        win = wr["window"]
        rows.append({
            "window":       i,
            "train_start":  win["train_start"],
            "train_end":    win["train_end"],
            "test_start":   win["test_start"],
            "test_end":     win["test_end"],
            "total_trades": m["total_trades"],
            "win_rate":     round(m["win_rate"], 4),
            "avg_return":   round(m["avg_return"], 4),
            "cagr":         round(m["cagr"], 4),
            "mdd":          round(m["mdd"], 4),
            "sharpe":       round(m["sharpe"], 4),
            "total_return": round(m["total_return"], 4),
        })

    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[report] walk-forward 결과 저장: {path}")


async def main(args: argparse.Namespace) -> None:
    # ── 0. 경고 ──────────────────────────────────────────────────────────
    if needs_dart(args):
        print("[경고] DART 재무 조건은 백테스트에서 미지원 → 무시됨")
    if needs_valuation(args):
        print("[경고] 밸류에이션 조건(PER/PBR)은 백테스트에서 미지원 → 무시됨")

    conditions_labels = all_labels(args)
    conditions_desc   = " & ".join(conditions_labels) if conditions_labels else "조건 없음"

    # ── 1. 윈도우 계산 ───────────────────────────────────────────────────
    windows = calculate_windows(args.start, args.end, args.train_years, args.test_years)
    if not windows:
        print(f"[main] 윈도우 없음. 기간({args.start}~{args.end})과 "
              f"훈련/검증 연수({args.train_years}+{args.test_years})를 확인하세요.")
        return

    print()
    print("[ Walk-forward 분석 ]")
    print(f"  조건: {conditions_desc}")
    print(f"  기간: {args.start} ~ {args.end}  |  창: {len(windows)}개")
    for i, w in enumerate(windows, 1):
        print(f"    창 {i}: 훈련 {w['train_start']}~{w['train_end']}  "
              f"→ 검증 {w['test_start']}~{w['test_end']}")
    print()

    # ── 2. OHLCV 로드 (전체 기간, 창 간 공유) ──────────────────────────
    from datetime import datetime

    load_start = (
        datetime.strptime(args.start, "%Y-%m-%d") - timedelta(days=425)
    ).strftime("%Y-%m-%d")

    if args.historical_universe:
        from backtest.universe_loader import load_historical_universe_ohlcv
        print(f"[main] 역사적 유니버스 로드 중... (DART 공시 기반)")
        universe_data = await load_historical_universe_ohlcv(
            start=args.start,
            end=args.end,
            force_refresh=args.force_refresh,
            max_tickers=args.max_tickers,
        )
    else:
        print(f"[main] 유니버스 조회 중... (by={args.by}, top={args.to})")
        universe = await get_stock_universe(by=args.by)
        tickers  = [s["ticker"] for s in universe[: args.to]]
        print(f"[main] {len(tickers)}개 종목 OHLCV 로드 시작...")
        universe_data = await load_universe_ohlcv(
            tickers=tickers,
            start=load_start,
            end=args.end,
            force_refresh=args.force_refresh,
        )

    if not universe_data:
        print("[main] OHLCV 데이터 없음. 종료.")
        return

    print(f"[main] {len(universe_data)}개 종목 로드 완료")
    print()

    # ── 3. 창별 백테스트 실행 (검증 구간) ────────────────────────────────
    window_results = []

    for i, window in enumerate(windows, 1):
        test_start = window["test_start"]
        test_end   = window["test_end"]
        print(f"[main] 창 {i}/{len(windows)} 검증 구간 실행: {test_start} ~ {test_end}")

        portfolio = run_backtest(
            universe_data=universe_data,
            conditions=args,
            start=test_start,
            end=test_end,
            hold_days=args.hold_days,
            max_positions=args.max_positions,
            initial_capital=args.initial_capital,
            stop_loss_pct=args.stop_loss,
            take_profit_pct=args.take_profit,
        )

        metrics = compute_metrics(portfolio)
        window_results.append({"window": window, "metrics": metrics, "portfolio": portfolio})
        print(
            f"  → 거래: {metrics['total_trades']}건  "
            f"승률: {metrics['win_rate']:.1%}  "
            f"CAGR: {metrics['cagr']:+.2%}  "
            f"Sharpe: {metrics['sharpe']:.2f}"
        )
        print()

    # ── 4. 결과 출력 ─────────────────────────────────────────────────────
    print_walkforward_report(window_results, conditions_desc, args)

    # ── 5. CSV 저장 ───────────────────────────────────────────────────────
    if not args.no_save and window_results:
        out_dir = Path("scripts/backtest/results")
        out_dir.mkdir(parents=True, exist_ok=True)
        prefix = f"wf_{args.start[:7].replace('-','')}_{args.end[:7].replace('-','')}"
        save_walkforward_csv(window_results, out_dir / f"{prefix}_summary.csv")

        # 창별 거래 내역도 저장
        for i, wr in enumerate(window_results, 1):
            from backtest.report import save_trades_csv
            trades_path = out_dir / f"{prefix}_w{i}_trades.csv"
            save_trades_csv(wr["portfolio"], trades_path)


if __name__ == "__main__":
    _args = parse_args()
    asyncio.run(main(_args))
