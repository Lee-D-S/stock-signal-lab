from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
MAP_PATH = ROOT / "PLAN" / "doc_sync_map.json"
TOKEN_RE = re.compile(r"[A-Za-z0-9_가-힣]{2,}")
GENERATED_MD_PREFIXES = (
    "ai 주가 변동 원인 분석/00_기업별분석/",
    "ai 주가 변동 원인 분석/10_일일요약/",
)
INSTRUCTION_MD = {"AGENTS.md"}
NOISY_TERMS = {
    "from",
    "import",
    "return",
    "class",
    "def",
    "true",
    "false",
    "none",
    "self",
    "path",
    "data",
    "with",
    "that",
    "this",
    "str",
    "int",
    "list",
    "dict",
    "set",
    "for",
    "if",
    "else",
    "elif",
    "and",
    "or",
    "not",
    "in",
    "is",
    "get",
    "add",
    "all",
    "any",
    "max",
    "min",
    "len",
    "print",
    "line",
    "lines",
    "args",
    "item",
    "items",
    "name",
    "text",
    "file",
    "files",
    "json",
}


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


def _tracked_markdown_files(include_untracked: bool) -> list[str]:
    files = set(_run_git(["ls-files", "*.md"]))
    if include_untracked:
        files.update(_run_git(["ls-files", "--others", "--exclude-standard", "*.md"]))
    return sorted(path.replace("\\", "/") for path in files)


def _load_map(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Missing doc sync map: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _text_for_path(path: str) -> str:
    full_path = ROOT / path
    if not full_path.exists() or full_path.is_dir():
        return ""
    try:
        return full_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def _diff_for_path(path: str) -> str:
    parts: list[str] = []
    for args in (["diff", "--", path], ["diff", "--cached", "--", path]):
        try:
            parts.extend(_run_git(args))
        except SystemExit:
            continue
    changed_lines = [
        line[1:]
        for line in parts
        if (line.startswith("+") or line.startswith("-"))
        and not line.startswith(("+++", "---"))
    ]
    return "\n".join(changed_lines)


def _tokens(text: str) -> set[str]:
    return {
        token.lower()
        for token in TOKEN_RE.findall(text)
        if token.lower() not in NOISY_TERMS
    }


def _terms_for_changed_file(path: str) -> set[str]:
    path_terms = _tokens(path.replace("/", " ").replace("\\", " ").replace(".", " "))
    diff_text = _diff_for_path(path)
    content_terms = _tokens(diff_text or _text_for_path(path))
    return path_terms | content_terms


def _is_generated_markdown(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in GENERATED_MD_PREFIXES)


def suggest_markdown_docs(
    changed: list[str],
    config: dict[str, Any],
    *,
    include_untracked: bool,
    include_generated: bool,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    if not changed:
        return [], 0

    changed_terms: set[str] = set()
    changed_path_terms: set[str] = set()
    for path in changed:
        changed_terms.update(_terms_for_changed_file(path))
        changed_path_terms.update(_tokens(path.replace("/", " ").replace("\\", " ").replace(".", " ")))

    mapped_docs = {
        doc.rstrip("/").replace("\\", "/")
        for doc in config.get("default_docs", [])
    }
    for rule in config.get("rules", []):
        mapped_docs.update(str(doc).rstrip("/").replace("\\", "/") for doc in rule.get("docs", []))

    candidates: list[dict[str, Any]] = []
    excluded_generated = 0
    for doc in _tracked_markdown_files(include_untracked=include_untracked):
        if not include_generated and _is_generated_markdown(doc):
            excluded_generated += 1
            continue
        if any(char in doc for char in "*?[]"):
            continue

        doc_text = _text_for_path(doc)
        doc_terms = _tokens(doc.replace("/", " ").replace("\\", " ").replace(".", " ")) | _tokens(doc_text)
        common = sorted(changed_terms & doc_terms)
        path_common = sorted(changed_path_terms & _tokens(doc.replace("/", " ").replace("\\", " ").replace(".", " ")))
        if not common and not path_common:
            continue

        score = len(common) + (len(path_common) * 3)
        if doc in mapped_docs:
            score += 5
        if doc.startswith("PLAN/") or "/01_" in doc or doc.startswith("docs/"):
            score += 2
        if doc in INSTRUCTION_MD:
            score -= 20

        if score > 0:
            candidates.append(
                {
                    "path": doc,
                    "score": score,
                    "matched_terms": common[:12],
                    "path_terms": path_common[:8],
                }
            )

    candidates.sort(key=lambda item: (-int(item["score"]), str(item["path"])))
    return candidates[:limit], excluded_generated


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
    parser.add_argument("--scan-md", action="store_true", help="Rank tracked Markdown docs that may mention changed code/behavior.")
    parser.add_argument("--include-generated-md", action="store_true", help="Include generated company reports and daily summaries in --scan-md.")
    parser.add_argument("--md-scan-limit", type=int, default=20, help="Maximum Markdown relevance suggestions to print.")
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
    else:
        print("\n[docs-sync] document review suggestions")
        for item in suggestions:
            print(f"\n- {item['id']}: {item['reason']}")
            print("  matched:")
            for path in item["matched"]:
                print(f"  - {path}")
            print("  docs:")
            for doc in item["docs"]:
                print(f"  - {doc}")

    if args.scan_md:
        md_suggestions, excluded_generated = suggest_markdown_docs(
            changed,
            config,
            include_untracked=args.include_untracked,
            include_generated=args.include_generated_md,
            limit=args.md_scan_limit,
        )
        print("\n[docs-sync] markdown relevance scan")
        if excluded_generated:
            print(f"- skipped generated markdown: {excluded_generated} files (use --include-generated-md to include)")
        if md_suggestions:
            for item in md_suggestions:
                terms = ", ".join(item["matched_terms"]) or "-"
                path_terms = ", ".join(item["path_terms"]) or "-"
                print(f"- score {item['score']}: {item['path']}")
                print(f"  terms: {terms}")
                print(f"  path_terms: {path_terms}")
        else:
            print("- no Markdown relevance candidates")

    return 1 if args.strict else 0


if __name__ == "__main__":
    sys.exit(main())
