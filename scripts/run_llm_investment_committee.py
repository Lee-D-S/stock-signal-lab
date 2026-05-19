from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from core.llm.committee import (  # noqa: E402
    build_local_llm_review,
    build_skipped_review,
    find_latest_run,
    write_review_artifacts,
)
from core.llm.local_client import LocalLLMConfig  # noqa: E402


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
    parser.add_argument("--model", default=settings.local_llm_model, help="Ollama 모델명입니다. 비우면 skipped로 기록합니다.")
    parser.add_argument("--backend", default=settings.local_llm_backend, help="로컬 LLM 백엔드입니다. 현재 ollama만 지원합니다.")
    parser.add_argument("--base-url", default=settings.local_llm_base_url, help="로컬 LLM base URL입니다.")
    parser.add_argument("--timeout-sec", type=int, default=settings.local_llm_timeout_sec)
    parser.add_argument("--no-llm", action="store_true", help="로컬 LLM 호출 없이 skipped 산출물만 생성합니다.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.run_date) if args.run_date else None
    run_dir = args.run_dir or find_latest_run(args.output_dir, run_date)
    if args.no_llm or not settings.local_llm_enabled or not str(args.model).strip():
        reason = "local LLM disabled or model is empty"
        review = build_skipped_review(run_dir, reason)
    else:
        review = build_local_llm_review(
            run_dir,
            LocalLLMConfig(
                backend=args.backend,
                base_url=args.base_url,
                model=args.model,
                timeout_sec=args.timeout_sec,
            ),
        )
    write_review_artifacts(review, run_dir)
    print(f"run_dir={run_dir}")
    print(f"local_llm_review={run_dir / 'local_llm_review.json'}")
    print(f"human_approval_brief={run_dir / 'human_approval_brief.md'}")
    print(f"llm_status={review.status}")
    if review.skipped_reason:
        print(f"skipped_reason={review.skipped_reason}")
    print(f"effective_status={review.final_gate.effective_status}")


if __name__ == "__main__":
    main()
