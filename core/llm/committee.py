from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .schemas import AgentReview, FinalGate, LLMReview, ReviewStatus


AGENT_FILES = {
    "quant": ("QuantSignalAgent", "quant_signal.json"),
    "analyst": ("EquityResearchAnalystAgent", "equity_research_analyst.json"),
    "research": ("ResearchFileAgent", "research_file.json"),
    "portfolio": ("PortfolioManagerAgent", "portfolio_manager.json"),
    "risk": ("RiskManagerAgent", "risk_manager.json"),
    "compliance": ("ComplianceOfficerAgent", "compliance_officer.json"),
    "trader": ("TraderAgent", "trader_order_proposal.json"),
    "secretary": ("OperationsReportAgent", "operations_report.json"),
}

STATUS_PRIORITY = {"block": 3, "needs_review": 2, "approve": 1, "info": 0, "skipped": 0}


def find_latest_run(output_dir: Path, run_date: date | None = None) -> Path:
    if run_date:
        date_dirs = [output_dir / run_date.isoformat()]
    else:
        date_dirs = sorted(
            (path for path in output_dir.iterdir() if path.is_dir()),
            key=lambda path: path.name,
            reverse=True,
        ) if output_dir.exists() else []

    candidates: list[Path] = []
    for date_dir in date_dirs:
        if not date_dir.exists():
            continue
        for run_dir in date_dir.iterdir():
            if run_dir.is_dir() and is_agent_run_dir(run_dir):
                candidates.append(run_dir)
    if not candidates:
        raise FileNotFoundError(f"agent run not found under {output_dir}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def load_run(run_dir: Path) -> dict[str, Any]:
    manifest_path = run_dir / "pipeline_manifest.json"
    manifest = read_json(manifest_path) if manifest_path.exists() else {
        "run_id": run_dir.name,
        "run_date": run_dir.parent.name,
        "run_dir": str(run_dir),
    }
    agents = {
        key: read_json(run_dir / filename)
        for key, (_, filename) in AGENT_FILES.items()
        if (run_dir / filename).exists()
    }
    report_path = run_dir / "final_committee_report.md"
    final_report = report_path.read_text(encoding="utf-8") if report_path.exists() else ""
    return {
        "manifest": manifest,
        "agents": agents,
        "final_report": final_report,
    }


def build_skipped_review(run_dir: Path, reason: str = "local LLM is not configured") -> LLMReview:
    payload = load_run(run_dir)
    manifest = payload["manifest"]
    agents = payload["agents"]
    agent_reviews = {
        key: AgentReview(
            agent=key,
            source_agent=source_agent,
            status="skipped",
            summary=f"{source_agent} 결과는 로드했지만 로컬 LLM 리뷰는 건너뛰었습니다.",
            human_questions=build_human_questions(result),
        )
        for key, (source_agent, _) in AGENT_FILES.items()
        if (result := agents.get(key)) is not None
    }
    final_gate = calculate_final_gate(agents, llm_recommendation="skipped")
    return LLMReview(
        engine="none",
        status="skipped",
        model="",
        run_id=str(manifest.get("run_id", run_dir.name)),
        run_date=str(manifest.get("run_date", run_dir.parent.name)),
        run_dir=str(run_dir),
        agents=agent_reviews,
        final_gate=final_gate,
        skipped_reason=reason,
    )


def write_review_artifacts(review: LLMReview, run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "local_llm_review.json").write_text(
        json.dumps(review.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_dir / "investment_committee_minutes.md").write_text(
        render_minutes(review),
        encoding="utf-8",
    )
    (run_dir / "human_approval_brief.md").write_text(
        render_human_brief(review),
        encoding="utf-8",
    )


def calculate_final_gate(
    agents: dict[str, dict[str, Any]],
    llm_recommendation: ReviewStatus,
) -> FinalGate:
    statuses = [str(agent.get("status", "info")) for agent in agents.values()]
    python_status = combine_statuses(statuses)
    risk_status = str(agents.get("risk", {}).get("status", "info"))
    compliance_status = str(agents.get("compliance", {}).get("status", "info"))
    hard_block_agents = []
    if risk_status == "block":
        hard_block_agents.append("RiskManagerAgent")
    if compliance_status == "block":
        hard_block_agents.append("ComplianceOfficerAgent")

    python_execution_allowed = trader_execution_allowed(agents.get("trader", {}))
    notes = ["Python guardrail 결과가 LLM 의견보다 우선합니다."]
    if hard_block_agents:
        effective_status = "block"
        notes.append("Risk/Compliance hard block이 있어 최종 상태를 block으로 유지합니다.")
    elif python_status in {"block", "needs_review"}:
        effective_status = python_status
        notes.append(f"Python 결과가 {python_status}라서 LLM은 이를 완화할 수 없습니다.")
    elif llm_recommendation in {"block", "needs_review"}:
        effective_status = "needs_review"
        notes.append("LLM 의견은 승인 상태를 더 보수적으로 낮출 수만 있습니다.")
    else:
        effective_status = python_status

    if not python_execution_allowed:
        notes.append("Trader 제안은 사람 승인 전 실행 가능으로 취급하지 않습니다.")

    return FinalGate(
        python_status=python_status,
        python_execution_allowed=python_execution_allowed,
        llm_recommendation=llm_recommendation,
        effective_status=effective_status,
        hard_block_agents=hard_block_agents,
        notes=notes,
    )


def render_minutes(review: LLMReview) -> str:
    lines = [
        "# 로컬 LLM 투자위원회 회의록",
        "",
        f"- 실행일: {review.run_date}",
        f"- run_id: {review.run_id}",
        f"- 엔진: {review.engine or 'none'}",
        f"- 상태: {review.status}",
        f"- skipped_reason: {review.skipped_reason or '-'}",
        "",
        "## Final Gate",
        "",
        f"- Python 상태: {review.final_gate.python_status}",
        f"- LLM 의견: {review.final_gate.llm_recommendation}",
        f"- 최종 상태: {review.final_gate.effective_status}",
        f"- Python execution_allowed: {review.final_gate.python_execution_allowed}",
        "",
        "## Agent 리뷰",
        "",
    ]
    for agent, item in review.agents.items():
        lines.append(f"### {agent}")
        lines.append("")
        lines.append(f"- 원천 Agent: {item.source_agent}")
        lines.append(f"- 상태: {item.status}")
        lines.append(f"- 요약: {item.summary}")
        if item.human_questions:
            lines.append("- 사람 확인사항:")
            lines.extend(f"  - {question}" for question in item.human_questions)
        lines.append("")
    return "\n".join(lines)


def render_human_brief(review: LLMReview) -> str:
    gate = review.final_gate
    lines = [
        "# 사람 승인 브리프",
        "",
        "- 이 문서는 검토 보조 자료이며 투자 지시가 아닙니다.",
        "- 실제 주문 실행은 사람 승인 전까지 금지됩니다.",
        "",
        f"- 실행일: {review.run_date}",
        f"- run_id: {review.run_id}",
        f"- 최종 상태: {gate.effective_status}",
        f"- Python 상태: {gate.python_status}",
        f"- LLM 상태: {review.status}",
        f"- execution_allowed: {gate.python_execution_allowed}",
        "",
        "## 확인할 사항",
        "",
    ]
    questions = sorted({
        question
        for agent in review.agents.values()
        for question in agent.human_questions
    })
    if questions:
        lines.extend(f"- {question}" for question in questions)
    else:
        lines.append("- Python Agent 산출물과 최종 보고서를 수동으로 확인하세요.")

    if gate.hard_block_agents:
        lines.extend([
            "",
            "## Hard Block",
            "",
            *[f"- {agent}" for agent in gate.hard_block_agents],
        ])
    lines.extend(["", "## Notes", ""])
    lines.extend(f"- {note}" for note in gate.notes)
    return "\n".join(lines)


def build_human_questions(result: dict[str, Any]) -> list[str]:
    checks = [str(item) for item in result.get("required_human_checks", []) if item]
    warnings = [str(item) for item in result.get("warnings", []) if item]
    return [*checks[:5], *warnings[:5]]


def trader_execution_allowed(trader: dict[str, Any]) -> bool:
    signals = trader.get("signals", [])
    if not signals:
        return False
    return all(bool(item.get("execution_allowed", False)) for item in signals)


def combine_statuses(statuses: list[str]) -> str:
    if not statuses:
        return "info"
    return max(statuses, key=lambda status: STATUS_PRIORITY.get(status, 0))


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def is_agent_run_dir(path: Path) -> bool:
    if (path / "pipeline_manifest.json").exists():
        return True
    return any((path / filename).exists() for _, filename in AGENT_FILES.values())
