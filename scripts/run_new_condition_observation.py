from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
STRATEGY_DIR = BASE_DIR / "07_전략신호"
OBS_DIR = BASE_DIR / "08_관찰기록"
SNAPSHOT_DIR = BASE_DIR / "09_조건스냅샷"

UNIVERSE_CSV = STRATEGY_DIR / "거래대금_상위_유니버스.csv"
NEW_STRATEGY_CSV = STRATEGY_DIR / "신규조건_전략_조건.csv"
NEW_STRATEGY_MD = STRATEGY_DIR / "신규조건_전략_조건.md"
NEW_SCAN_CSV = STRATEGY_DIR / "신규조건_관심종목_시그널_스캔.csv"
NEW_WATCHLIST_CSV = STRATEGY_DIR / "신규조건_관심종목_시그널_후보.csv"
NEW_WATCHLIST_MD = STRATEGY_DIR / "신규조건_관심종목_시그널_후보.md"
NEW_CONFIRMED_CSV = STRATEGY_DIR / "신규조건_관심종목_시그널_후보_확정.csv"
NEW_CONFIRMED_MD = STRATEGY_DIR / "신규조건_관심종목_시그널_후보_확정.md"
NEW_OBS_UTF8_CSV = OBS_DIR / "신규조건_관찰_로그(이상).csv"
NEW_OBS_CP949_CSV = OBS_DIR / "신규조건_관찰_로그.csv"
NEW_OBS_MD = OBS_DIR / "신규조건_관찰_로그.md"

STRATEGY_COLUMNS = [
    "priority",
    "hypothesis_id",
    "use_type",
    "market_regime",
    "direction",
    "amount_tag",
    "flow_category",
    "dart_tag",
    "window_category",
    "action_hint",
    "suggested_response",
    "preferred_entry_mode",
    "preferred_hold_days",
    "tested_trades",
    "avg_score_return_pct",
    "hit_rate",
    "risk_note",
]


def latest_snapshot_dir(snapshot_date: str | None) -> Path:
    if snapshot_date:
        return SNAPSHOT_DIR / snapshot_date
    dirs = [path for path in SNAPSHOT_DIR.iterdir() if path.is_dir()]
    if not dirs:
        raise SystemExit(f"snapshot directory not found: {SNAPSHOT_DIR}")
    return sorted(dirs, key=lambda path: path.name)[-1]


def use_type_for(action_hint: Any) -> str:
    text = str(action_hint)
    if "반등" in text:
        return "신규조건 반등 감시 후보"
    if "회피" in text or "추격" in text:
        return "신규조건 회피 후보"
    return "신규조건 매수 후보"


def response_for(action_hint: Any) -> str:
    text = str(action_hint)
    if "반등" in text:
        return "신규 조건 관찰: 하락 이벤트 다음 거래일 반등 확인 후 단기 진입 가능성만 검토"
    if "회피" in text or "추격" in text:
        return "신규 조건 관찰: 추격매수 회피 가능성 검증"
    return "신규 조건 관찰: 다음 거래일 분할 진입 가능성 검토"


def build_strategy(review_csv: Path, verdict: str) -> pd.DataFrame:
    review = pd.read_csv(review_csv, encoding="utf-8-sig")
    selected = review[review["verdict"].astype(str) == verdict].copy()
    if selected.empty:
        return pd.DataFrame(columns=STRATEGY_COLUMNS)

    rows: list[dict[str, Any]] = []
    for priority, (_, row) in enumerate(selected.iterrows(), start=1):
        rows.append(
            {
                "priority": priority,
                "hypothesis_id": row["suggested_stable_id"],
                "use_type": use_type_for(row.get("action_hint")),
                "market_regime": row["market_regime"],
                "direction": row["direction"],
                "amount_tag": row["amount_tag"],
                "flow_category": row["flow_category"],
                "dart_tag": row["dart_tag"],
                "window_category": row["window_category"],
                "action_hint": row["action_hint"],
                "suggested_response": response_for(row.get("action_hint")),
                "preferred_entry_mode": row.get("best_entry_mode", "next_open"),
                "preferred_hold_days": int(row.get("best_hold_days", 20)),
                "tested_trades": int(row.get("tested_trades", 0)),
                "avg_score_return_pct": float(row.get("avg_score_return_pct", 0)),
                "hit_rate": float(row.get("hit_rate", 0)),
                "risk_note": row.get("decision_reason", ""),
            }
        )
    return pd.DataFrame(rows, columns=STRATEGY_COLUMNS)


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_대상 조건 없음_"
    lines = [
        "| " + " | ".join(df.columns) + " |",
        "| " + " | ".join("---" for _ in df.columns) + " |",
    ]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(str(row[col]).replace("|", "\\|") for col in df.columns) + " |")
    return "\n".join(lines)


def write_strategy_files(strategy: pd.DataFrame, snapshot: Path, verdict: str) -> None:
    STRATEGY_DIR.mkdir(parents=True, exist_ok=True)
    strategy.to_csv(NEW_STRATEGY_CSV, index=False, encoding="utf-8-sig")
    lines = [
        "# 신규 조건 별도 관찰 전략",
        "",
        f"- 스냅샷: `{snapshot.name}`",
        f"- 선별 기준: `{verdict}`",
        f"- 조건 수: {len(strategy):,}",
        "- 이 파일은 active 전략 조건이 아니라 별도 관찰용 조건이다.",
        "",
        "## 조건",
        "",
        markdown_table(
            strategy[
                [
                    "priority",
                    "hypothesis_id",
                    "use_type",
                    "market_regime",
                    "direction",
                    "amount_tag",
                    "flow_category",
                    "dart_tag",
                    "window_category",
                    "preferred_entry_mode",
                    "preferred_hold_days",
                    "avg_score_return_pct",
                    "hit_rate",
                ]
            ]
            if not strategy.empty
            else strategy
        ),
        "",
    ]
    NEW_STRATEGY_MD.write_text("\n".join(lines), encoding="utf-8")


def run_command(args: list[str], dry_run: bool) -> None:
    print("    " + " ".join(["python", "-u", f"scripts/{Path(args[0]).name}", *args[1:]]))
    if dry_run:
        return
    result = subprocess.run([sys.executable, "-u", *args], cwd=ROOT, text=True)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description="신규 조건을 active와 분리해 별도 관찰")
    parser.add_argument("--snapshot-date", help="YYYY-MM-DD. 생략하면 최신 조건 스냅샷")
    parser.add_argument("--date", help="daily 기준일 YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=220)
    parser.add_argument("--delay", type=float, default=0.35)
    parser.add_argument("--verdict", default="우선 관찰")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    snapshot = latest_snapshot_dir(args.snapshot_date)
    review_csv = snapshot / "신규_조건_검토.csv"
    if not review_csv.exists():
        raise SystemExit(f"review csv not found: {review_csv}")

    strategy = build_strategy(review_csv, args.verdict)
    write_strategy_files(strategy, snapshot, args.verdict)
    print(f"new_strategy_count={len(strategy)}")
    print(f"new_strategy_csv={NEW_STRATEGY_CSV}")
    if strategy.empty:
        return

    date_args = ["--date", args.date] if args.date else []
    print("\n==> 신규 조건 후보 탐지")
    run_command(
        [
            str(SCRIPTS / "tmp_generate_watchlist_signals.py"),
            *date_args,
            "--lookback-days",
            str(args.lookback_days),
            "--delay",
            str(args.delay),
            "--universe-csv",
            str(UNIVERSE_CSV),
            "--strategy-csv",
            str(NEW_STRATEGY_CSV),
            "--scan-csv",
            str(NEW_SCAN_CSV),
            "--watchlist-csv",
            str(NEW_WATCHLIST_CSV),
            "--watchlist-md",
            str(NEW_WATCHLIST_MD),
            "--title",
            "신규 조건 별도 관찰 후보",
        ],
        args.dry_run,
    )

    print("\n==> 신규 조건 후보 수급 재조회")
    run_command(
        [
            str(SCRIPTS / "tmp_recheck_watchlist_flows.py"),
            "--delay",
            str(args.delay),
            "--candidates-csv",
            str(NEW_WATCHLIST_CSV),
            "--confirmed-csv",
            str(NEW_CONFIRMED_CSV),
            "--confirmed-md",
            str(NEW_CONFIRMED_MD),
            "--title",
            "신규 조건 별도 관찰 후보 수급 재조회",
        ],
        args.dry_run,
    )

    print("\n==> 신규 조건 확정 후보 관찰 로그 추가")
    run_command(
        [
            str(SCRIPTS / "run_observation_update.py"),
            "--confirmed-csv",
            str(NEW_CONFIRMED_CSV),
            "--obs-utf8-csv",
            str(NEW_OBS_UTF8_CSV),
            "--obs-cp949-csv",
            str(NEW_OBS_CP949_CSV),
            "--obs-md",
            str(NEW_OBS_MD),
            "--title",
            "신규 조건 별도 관찰 로그",
        ],
        args.dry_run,
    )

    print("\n==> 신규 조건 관찰 로그 D+ 추적 업데이트")
    tracking_date_args = ["--as-of", args.date] if args.date else []
    run_command(
        [
            str(SCRIPTS / "run_observation_tracking_update.py"),
            *tracking_date_args,
            "--delay",
            str(args.delay),
            "--obs-utf8-csv",
            str(NEW_OBS_UTF8_CSV),
            "--obs-cp949-csv",
            str(NEW_OBS_CP949_CSV),
            "--obs-md",
            str(NEW_OBS_MD),
        ],
        args.dry_run,
    )


if __name__ == "__main__":
    main()
