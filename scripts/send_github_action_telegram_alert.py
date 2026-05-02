from __future__ import annotations

import argparse
import os
import sys
import urllib.parse
import urllib.request


STATUS_LABELS = {
    "success": "성공",
    "failure": "실패",
    "test": "테스트",
}


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def build_run_url() -> str:
    explicit = env("RUN_URL")
    if explicit:
        return explicit
    server = env("GITHUB_SERVER_URL", "https://github.com")
    repo = env("GITHUB_REPOSITORY")
    run_id = env("GITHUB_RUN_ID")
    if not repo or not run_id:
        return ""
    return f"{server}/{repo}/actions/runs/{run_id}"


def build_message(status: str, job_name: str) -> str:
    label = STATUS_LABELS.get(status, status)
    return "\n".join(
        [
            f"[auto-invest] GitHub Actions {label}",
            f"workflow: {env('GITHUB_WORKFLOW', '일별 자동 루틴')}",
            f"job: {job_name}",
            f"branch: {env('GITHUB_REF_NAME')}",
            f"run: {build_run_url()}",
        ]
    )


def send_message(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    request = urllib.request.Request(url, data=payload, method="POST")
    with urllib.request.urlopen(request, timeout=20) as response:
        response.read()


def main() -> int:
    parser = argparse.ArgumentParser(description="Send GitHub Actions result to Telegram.")
    parser.add_argument("--status", choices=sorted(STATUS_LABELS), required=True)
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--soft-fail", action="store_true", help="Log errors without failing the workflow.")
    args = parser.parse_args()

    token = env("TELEGRAM_BOT_TOKEN")
    chat_id = env("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("telegram secrets missing; skip notification")
        return 0 if args.soft_fail else 1

    try:
        send_message(token, chat_id, build_message(args.status, args.job_name))
    except Exception as exc:
        print(f"telegram notification failed: {type(exc).__name__}: {exc}")
        return 0 if args.soft_fail else 1

    print("telegram notification sent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
