from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Python 투자위원회 실행 후 같은 run 디렉터리에 로컬 LLM 리뷰를 생성합니다."
    )
    parser.add_argument("--model", default="", help="Ollama 모델명입니다. 비우면 LLM 단계는 skipped로 기록합니다.")
    parser.add_argument("--roles", default="secretary", help="LLM 호출 역할입니다. 예: secretary, risk,compliance,trader, all")
    parser.add_argument("--backend", default="ollama")
    parser.add_argument("--base-url", default="http://127.0.0.1:11434")
    parser.add_argument("--timeout-sec", type=int, default=300)
    parser.add_argument("--no-llm", action="store_true", help="LLM 호출 없이 skipped 산출물만 생성합니다.")
    parser.add_argument(
        "committee_args",
        nargs=argparse.REMAINDER,
        help="scripts/run_agent_committee.py에 전달할 인자입니다. -- 뒤에 적을 수 있습니다.",
    )
    known, unknown = parser.parse_known_args()
    known.committee_args = [*unknown, *strip_separator(known.committee_args)]
    return known


def main() -> None:
    args = parse_args()
    committee_cmd = [
        sys.executable,
        "-u",
        str(ROOT / "scripts" / "run_agent_committee.py"),
        *args.committee_args,
    ]
    committee = run_command(committee_cmd)
    print(committee.stdout, end="")
    if committee.returncode != 0:
        print(committee.stderr, end="", file=sys.stderr)
        raise SystemExit(committee.returncode)

    run_dir = parse_run_dir(committee.stdout)
    llm_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_llm_investment_committee.py"),
        "--run-dir",
        str(run_dir),
        "--backend",
        args.backend,
        "--base-url",
        args.base_url,
        "--timeout-sec",
        str(args.timeout_sec),
        "--roles",
        args.roles,
    ]
    if args.model:
        llm_cmd.extend(["--model", args.model])
    if args.no_llm:
        llm_cmd.append("--no-llm")

    llm = run_command(llm_cmd)
    print(llm.stdout, end="")
    if llm.returncode != 0:
        print(llm.stderr, end="", file=sys.stderr)
        print(f"LLM review failed, but Python agent run was preserved: {run_dir}", file=sys.stderr)
        raise SystemExit(llm.returncode)


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def parse_run_dir(output: str) -> Path:
    for line in output.splitlines():
        if line.startswith("실행 폴더="):
            return Path(line.split("=", 1)[1].strip())
    raise SystemExit("failed to parse run directory from run_agent_committee.py output")


def strip_separator(args: list[str]) -> list[str]:
    if args and args[0] == "--":
        return args[1:]
    return args


if __name__ == "__main__":
    main()
