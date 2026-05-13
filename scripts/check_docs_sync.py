from __future__ import annotations

import argparse
import fnmatch
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
MAP_PATH = ROOT / "PLAN" / "doc_sync_map.json"


def _run_git(args: list[str]) -> list[str]:
    result = subprocess.run(
        ["git", "-c", "core.quotePath=false", *args],
        cwd=ROOT,
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip() or result.stdout.strip())
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _changed_files(include_untracked: bool) -> list[str]:
    files: set[str] = set()
    for line in _run_git(["diff", "--name-only"]):
        files.add(line.replace("\\", "/"))
    for line in _run_git(["diff", "--cached", "--name-only"]):
        files.add(line.replace("\\", "/"))
    if include_untracked:
        for line in _run_git(["ls-files", "--others", "--exclude-standard"]):
            files.add(line.replace("\\", "/"))
    return sorted(files)


def _load_map(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Missing doc sync map: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _matches(path: str, pattern: str) -> bool:
    pattern = pattern.replace("\\", "/")
    return fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(path, pattern.rstrip("/") + "/**")


def suggest_docs(changed: list[str], config: dict[str, Any]) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    for rule in config.get("rules", []):
        patterns = rule.get("patterns", [])
        matched = [path for path in changed if any(_matches(path, pattern) for pattern in patterns)]
        if matched:
            suggestions.append(
                {
                    "id": rule.get("id", "unnamed"),
                    "reason": rule.get("reason", ""),
                    "matched": matched,
                    "docs": rule.get("docs", []),
                }
            )
    return suggestions


def main() -> int:
    parser = argparse.ArgumentParser(description="Suggest auto-invest docs to update after code changes.")
    parser.add_argument("--all", action="store_true", help="Also show default docs even when no rule matches.")
    parser.add_argument("--include-untracked", action="store_true", help="Include untracked generated files.")
    parser.add_argument("--max-files", type=int, default=80, help="Maximum changed files to print.")
    parser.add_argument("--map", default=str(MAP_PATH), help="Path to doc_sync_map.json.")
    parser.add_argument("--strict", action="store_true", help="Exit 1 when document review suggestions exist.")
    args = parser.parse_args()

    changed = _changed_files(include_untracked=args.include_untracked)
    config = _load_map(Path(args.map))
    suggestions = suggest_docs(changed, config)

    print("[docs-sync] changed files")
    if changed:
        for path in changed[: args.max_files]:
            print(f"- {path}")
        if len(changed) > args.max_files:
            print(f"- ... {len(changed) - args.max_files} more")
    else:
        print("- (none)")

    if args.all or suggestions:
        print("\n[docs-sync] default docs")
        for doc in config.get("default_docs", []):
            print(f"- {doc}")

    if not suggestions:
        print("\n[docs-sync] no mapped document updates required")
        return 0

    print("\n[docs-sync] document review suggestions")
    for item in suggestions:
        print(f"\n- {item['id']}: {item['reason']}")
        print("  matched:")
        for path in item["matched"]:
            print(f"  - {path}")
        print("  docs:")
        for doc in item["docs"]:
            print(f"  - {doc}")

    return 1 if args.strict else 0


if __name__ == "__main__":
    sys.exit(main())
