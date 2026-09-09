from __future__ import annotations

import sys

from run_free_agent_pipeline import main


def inject_default_discovery(argv: list[str]) -> list[str]:
    has_candidate_source = any(
        arg == "--discover"
        or arg == "--candidate"
        or arg.startswith("--candidate=")
        for arg in argv[1:]
    )
    if has_candidate_source:
        return argv
    return [argv[0], "--discover", *argv[1:]]


if __name__ == "__main__":
    sys.argv = inject_default_discovery(sys.argv)
    main()
