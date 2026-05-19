from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.llm.committee import (  # noqa: E402
    build_skipped_review,
    find_latest_run,
    write_review_artifacts,
)


DEFAULT_OUTPUT_DIR = ROOT / "data" / "agent_runs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="최신 Python Agent 결과를 읽어 로컬 LLM 투자위원회 산출물 skeleton을 생성합니다."
    )
    parser.add_argument("--run-dir", type=Path, help="특정 agent run 디렉터리를 지정합니다.")
    parser.add_argument(
        "--date",
        dest="run_date",
        help="특정 실행일의 최신 run을 사용합니다 (YYYY-MM-DD).",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.run_date) if args.run_date else None
    run_dir = args.run_dir or find_latest_run(args.output_dir, run_date)
    review = build_skipped_review(run_dir)
    write_review_artifacts(review, run_dir)
    print(f"run_dir={run_dir}")
    print(f"local_llm_review={run_dir / 'local_llm_review.json'}")
    print(f"human_approval_brief={run_dir / 'human_approval_brief.md'}")
    print(f"effective_status={review.final_gate.effective_status}")


if __name__ == "__main__":
    main()
