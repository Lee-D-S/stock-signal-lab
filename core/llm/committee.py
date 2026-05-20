from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .local_client import LocalLLMClient, LocalLLMConfig
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

LLM_REVIEW_AGENT_KEYS = ("quant", "analyst", "research", "risk", "compliance", "trader")
DEFAULT_LLM_ROLES = ("secretary",)
ALL_LLM_ROLES = (*LLM_REVIEW_AGENT_KEYS, "secretary")
SECRETARY_REPORT_EXCERPT_CHARS = 2800
MAX_AGENT_WARNINGS = 12
MAX_AGENT_HUMAN_CHECKS = 8
MAX_AGENT_SIGNALS = 6
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


def build_local_llm_review(
    run_dir: Path,
    config: LocalLLMConfig,
    roles: tuple[str, ...] = DEFAULT_LLM_ROLES,
) -> LLMReview:
    payload = load_run(run_dir)
    manifest = payload["manifest"]
    agents = payload["agents"]
    agent_reviews = build_base_agent_reviews(agents)
    final_gate = calculate_final_gate(agents, llm_recommendation="info")

    client = LocalLLMClient(config)
    stop_reason = ""
    selected_roles = normalize_roles(roles)
    merge_existing_agent_reviews(run_dir, agent_reviews, selected_roles)
    for agent_key in LLM_REVIEW_AGENT_KEYS:
        result = agents.get(agent_key)
        if result is None:
            continue
        if agent_key not in selected_roles:
            continue
        response = (
            client.chat(build_agent_review_messages(agent_key, result, final_gate))
            if not stop_reason
            else None
        )
        if response and response.ok:
            parsed_review = parse_agent_review_response(agent_key, response.text)
            parsed_review = apply_agent_review_guardrails(
                agent_key,
                parsed_review,
                final_gate,
            )
            agent_reviews[agent_key] = AgentReview(
                agent=agent_key,
                source_agent="LocalLLM",
                status="needs_review",
                summary=parsed_review["summary"],
                objections=parsed_review["objections"],
                red_flags=parsed_review["red_flags"],
                human_questions=parsed_review["human_questions"] + [
                    f"{AGENT_FILES[agent_key][0]} 원본 JSON과 LLM 리뷰가 일치하는지 확인하세요.",
                    "LLM 리뷰가 Python hard gate를 완화하거나 승인으로 바꾸지 않았는지 확인하세요.",
                ],
                raw_text=response.text,
            )
        else:
            reason = response.skipped_reason if response else stop_reason
            if response and should_stop_llm_attempts(response.skipped_reason):
                stop_reason = response.skipped_reason
            agent_reviews[agent_key] = AgentReview(
                agent=agent_key,
                source_agent=AGENT_FILES[agent_key][0],
                status="skipped",
                summary=f"{AGENT_FILES[agent_key][0]} LLM 리뷰를 건너뛰었습니다: {reason}",
                human_questions=build_human_questions(result),
            )

    if "secretary" not in selected_roles:
        return LLMReview(
            engine=config.backend,
            status=combine_review_statuses(agent_reviews),
            model=config.model,
            run_id=str(manifest.get("run_id", run_dir.name)),
            run_date=str(manifest.get("run_date", run_dir.parent.name)),
            run_dir=str(run_dir),
            agents=agent_reviews,
            final_gate=final_gate,
        )

    response = client.chat(build_secretary_messages(payload, final_gate)) if not stop_reason else None
    if not response or not response.ok:
        reason = response.skipped_reason if response else stop_reason
        agent_reviews["secretary"] = AgentReview(
            agent="secretary",
            source_agent="OperationsReportAgent",
            status="skipped",
            summary=f"Secretary LLM 요약을 건너뛰었습니다: {reason}",
            human_questions=build_human_questions(agents.get("secretary", {})),
        )
        review_status: ReviewStatus = combine_review_statuses(agent_reviews)
        return LLMReview(
            engine=config.backend,
            status=review_status,
            model=config.model,
            run_id=str(manifest.get("run_id", run_dir.name)),
            run_date=str(manifest.get("run_date", run_dir.parent.name)),
            run_dir=str(run_dir),
            agents=agent_reviews,
            final_gate=final_gate,
            skipped_reason=reason,
        )

    agent_reviews["secretary"] = AgentReview(
        agent="secretary",
        source_agent="LocalLLM",
        status="info",
        summary=response.text,
        human_questions=[
            "LLM 요약이 Python Agent 결과와 Final Gate를 왜곡하지 않았는지 확인하세요.",
            "Risk/Compliance/Trader의 hard gate 결과가 사람 검토 전에 완화되어 표현되지 않았는지 확인하세요.",
        ],
    )
    return LLMReview(
        engine=config.backend,
        status=combine_review_statuses(agent_reviews),
        model=config.model,
        run_id=str(manifest.get("run_id", run_dir.name)),
        run_date=str(manifest.get("run_date", run_dir.parent.name)),
        run_dir=str(run_dir),
        agents=agent_reviews,
        final_gate=final_gate,
    )


def normalize_roles(roles: tuple[str, ...]) -> set[str]:
    if not roles:
        roles = DEFAULT_LLM_ROLES
    normalized = {role.strip().lower() for role in roles if role.strip()}
    if "all" in normalized:
        return set(ALL_LLM_ROLES)
    unknown = sorted(normalized.difference(ALL_LLM_ROLES))
    if unknown:
        raise ValueError(f"unsupported LLM role(s): {', '.join(unknown)}")
    return normalized or set(DEFAULT_LLM_ROLES)


def merge_existing_agent_reviews(
    run_dir: Path,
    agent_reviews: dict[str, AgentReview],
    selected_roles: set[str],
) -> None:
    existing_path = run_dir / "local_llm_review.json"
    if not existing_path.exists():
        return
    try:
        existing = read_json(existing_path)
    except (OSError, json.JSONDecodeError):
        return
    existing_agents = existing.get("agents", {})
    if not isinstance(existing_agents, dict):
        return
    for agent_key, raw_review in existing_agents.items():
        if agent_key in selected_roles or agent_key not in agent_reviews:
            continue
        if not isinstance(raw_review, dict):
            continue
        agent_reviews[agent_key] = agent_review_from_dict(agent_key, raw_review)


def agent_review_from_dict(agent_key: str, data: dict[str, Any]) -> AgentReview:
    status = str(data.get("status", "skipped"))
    if status not in STATUS_PRIORITY:
        status = "skipped"
    return AgentReview(
        agent=str(data.get("agent") or agent_key),
        source_agent=str(data.get("source_agent", "")),
        status=status,  # type: ignore[arg-type]
        summary=str(data.get("summary", "")),
        objections=coerce_str_list(data.get("objections")),
        red_flags=coerce_str_list(data.get("red_flags")),
        human_questions=coerce_str_list(data.get("human_questions")),
        raw_text=str(data.get("raw_text", "")),
    )


def build_base_agent_reviews(agents: dict[str, dict[str, Any]]) -> dict[str, AgentReview]:
    return {
        key: AgentReview(
            agent=key,
            source_agent=source_agent,
            status="skipped",
            summary=f"{source_agent} 결과는 로드했지만 역할별 LLM 리뷰는 아직 생성하지 않았습니다.",
            human_questions=build_human_questions(result),
        )
        for key, (source_agent, _) in AGENT_FILES.items()
        if (result := agents.get(key)) is not None
    }


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


def build_secretary_messages(payload: dict[str, Any], final_gate: FinalGate) -> list[dict[str, str]]:
    manifest = payload["manifest"]
    agents = payload["agents"]
    report = str(payload.get("final_report", ""))[:SECRETARY_REPORT_EXCERPT_CHARS]
    agent_statuses = {
        key: {
            "status": value.get("status", "info"),
            "summary": value.get("summary", ""),
            "warning_count": len(value.get("warnings", [])),
            "human_check_count": len(value.get("required_human_checks", [])),
        }
        for key, value in agents.items()
    }
    user_payload = {
        "manifest": manifest,
        "agent_statuses": agent_statuses,
        "final_gate": final_gate.to_dict(),
        "final_committee_report_excerpt": report,
    }
    return [
        {
            "role": "system",
            "content": (
                "당신은 auto-invest의 로컬 LLM Secretary입니다. "
                "Python Agent 산출물을 사람이 검토하기 쉽게 요약하는 보조자입니다. "
                "투자 조언, 매수/매도 권고, 주문 실행 승인을 하지 마세요. "
                "Risk/Compliance/Trader hard gate를 완화하지 말고, Final Gate를 그대로 따르세요. "
                "한국어로 8줄 이내로만 답하세요."
            ),
        },
        {
            "role": "user",
            "content": (
                "아래 JSON을 바탕으로 한국어로 짧은 투자위원회 브리프를 작성하세요. "
                "형식은 1) 최종 상태, 2) 주요 차단/검토 사유, 3) 사람이 확인할 항목, 4) 주문 실행 관련 주의사항 순서로 작성하세요. "
                "새로운 투자 판단을 만들지 말고 입력 JSON에 있는 사실만 쓰세요.\n\n"
                + json.dumps(user_payload, ensure_ascii=False, indent=2)
            ),
        },
    ]


def build_agent_review_messages(
    agent_key: str,
    result: dict[str, Any],
    final_gate: FinalGate,
) -> list[dict[str, str]]:
    role_names = {
        "quant": "Quant",
        "analyst": "Analyst",
        "research": "Evidence",
        "risk": "Risk Manager",
        "compliance": "Compliance Officer",
        "trader": "Trader",
    }
    role_name = role_names.get(agent_key, agent_key)
    source_agent = AGENT_FILES[agent_key][0]
    compact_result = {
        "source_agent": source_agent,
        "status": result.get("status", "info"),
        "summary": result.get("summary", ""),
        "warnings": result.get("warnings", [])[:MAX_AGENT_WARNINGS],
        "required_human_checks": result.get("required_human_checks", [])[:MAX_AGENT_HUMAN_CHECKS],
        "artifacts": result.get("artifacts", {}),
        "signals": result.get("signals", [])[:MAX_AGENT_SIGNALS],
        "final_gate": final_gate.to_dict(),
    }
    output_contract = agent_output_contract(agent_key)
    return [
        {
            "role": "system",
            "content": (
                f"당신은 auto-invest의 로컬 LLM {role_name}입니다. "
                "Python Agent JSON을 사람이 검토하기 쉽게 설명하는 보조자입니다. "
                "투자 조언, 매수/매도 권고, 주문 실행 승인, hard gate 완화를 하지 마세요. "
                "입력 JSON에 없는 사실을 만들지 마세요. "
                "반드시 JSON 객체 하나만 반환하세요."
            ),
        },
        {
            "role": "user",
            "content": (
                f"아래 {source_agent} JSON을 검토하고 한국어로 작성하세요. "
                f"{agent_review_focus(agent_key)} "
                f"{output_contract} "
                "각 배열은 최대 3개, 각 문장은 80자 이내로 제한하세요. "
                "Python 결과가 block 또는 needs_review라면 그 상태를 완화하지 마세요.\n\n"
                + json.dumps(compact_result, ensure_ascii=False, indent=2)
            ),
        },
    ]


def parse_agent_review_response(agent_key: str, text: str) -> dict[str, Any]:
    parsed = parse_json_object(text)
    if parsed:
        summary = str(parsed.get("summary") or "").strip()
        return {
            "summary": summary or text.strip(),
            "red_flags": coerce_str_list(parsed.get("red_flags")),
            "objections": coerce_str_list(parsed.get("objections")),
            "human_questions": coerce_str_list(parsed.get("human_questions")),
        }
    return {
        "summary": text.strip(),
        "red_flags": [],
        "objections": [],
        "human_questions": [],
    }


def apply_agent_review_guardrails(
    agent_key: str,
    review: dict[str, Any],
    final_gate: FinalGate,
) -> dict[str, Any]:
    if agent_key != "trader":
        return review
    if final_gate.python_execution_allowed and final_gate.effective_status != "block":
        return review
    red_flags = list(review["red_flags"])
    questions = list(review["human_questions"])
    no_execution_flag = "execution_allowed=false이므로 주문 실행 금지 상태입니다."
    if no_execution_flag not in red_flags:
        red_flags.insert(0, no_execution_flag)
    check = "Trader 제안은 사람 검토용 초안이며 실행 지시가 아닌지 확인하세요."
    if check not in questions:
        questions.append(check)
    return {
        **review,
        "summary": "주문 실행 금지. Trader 제안은 사람 검토용 초안입니다.",
        "red_flags": red_flags[:3],
        "human_questions": questions[:3],
    }


def parse_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        return {}
    candidates = [stripped]
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start >= 0 and end > start:
        candidates.append(stripped[start:end + 1])
    for candidate in candidates:
        try:
            data = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            return data
    return {}


def coerce_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def agent_review_focus(agent_key: str) -> str:
    focus = {
        "quant": "후보가 어떤 신호로 올라왔는지, 신호 품질과 누락 데이터, 과최적화 위험을 설명하세요.",
        "analyst": "기업 리서치의 thesis, catalyst, risk, disconfirmation 누락과 보강 질문을 설명하세요.",
        "research": "리서치 파일 누락, 오래된 근거, Analyst 결과와 ResearchFile 결과의 불일치를 설명하세요.",
        "risk": "손실 시나리오, 유동성, 변동성, 포지션 리스크와 hard block 사유를 설명하세요.",
        "compliance": "준법/기록/증거 체인 누락과 사람이 확인해야 할 필수 항목을 설명하세요.",
        "trader": "주문 제안의 전제, 보류 조건, 체결 전 체크리스트를 설명하세요.",
    }
    return focus.get(agent_key, "Python Agent 결과의 핵심 검토 포인트를 설명하세요.")


def agent_output_contract(agent_key: str) -> str:
    if agent_key == "trader":
        return (
            'JSON 형식: {"summary":"주문 제안은 사람 검토용이며 실행 금지",'
            '"red_flags":["..."],"objections":["..."],"human_questions":["..."]}. '
            "execution_allowed=false이면 실행 가능하다고 쓰지 마세요."
        )
    if agent_key == "compliance":
        return (
            'JSON 형식: {"summary":"준법/기록/증거 체인 상태 요약",'
            '"red_flags":["..."],"objections":["..."],"human_questions":["..."]}. '
            "승인 가능성이나 주문 가능성을 새로 판단하지 마세요."
        )
    if agent_key == "risk":
        return (
            'JSON 형식: {"summary":"리스크 차단/검토 상태 요약",'
            '"red_flags":["..."],"objections":["..."],"human_questions":["..."]}. '
            "Final Gate가 block이면 block 유지 사유만 설명하세요."
        )
    return (
        'JSON 형식: {"summary":"핵심 검토 상태 요약",'
        '"red_flags":["..."],"objections":["..."],"human_questions":["..."]}.'
    )


def render_minutes(review: LLMReview) -> str:
    lines = [
        "# 로컬 LLM 투자위원회 회의록",
        "",
        f"- 실행일: {review.run_date}",
        f"- run_id: {review.run_id}",
        f"- 엔진: {review.engine or 'none'}",
        f"- 상태: {review.status}",
        f"- 모델: {review.model or '-'}",
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
        if item.red_flags:
            lines.append("- Red flags:")
            lines.extend(f"  - {red_flag}" for red_flag in item.red_flags)
        if item.objections:
            lines.append("- 반론/이견:")
            lines.extend(f"  - {objection}" for objection in item.objections)
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
        f"- LLM 모델: {review.model or '-'}",
        f"- execution_allowed: {gate.python_execution_allowed}",
        "",
        "## 확인할 사항",
        "",
    ]
    secretary = review.agents.get("secretary")
    if secretary and secretary.source_agent == "LocalLLM" and secretary.summary:
        lines[-2:] = [
            "## LLM Secretary 요약",
            "",
            secretary.summary,
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


def combine_review_statuses(reviews: dict[str, AgentReview]) -> ReviewStatus:
    statuses = [review.status for review in reviews.values() if review.status != "skipped"]
    if not statuses:
        return "skipped"
    combined = combine_statuses(statuses)
    return combined if combined in {"approve", "block", "needs_review", "info"} else "info"


def should_stop_llm_attempts(reason: str) -> bool:
    return (
        "not reachable" in reason
        or "timed out" in reason
        or "HTTP error: 404" in reason
        or "LOCAL_LLM_MODEL is empty" in reason
    )


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def is_agent_run_dir(path: Path) -> bool:
    if (path / "pipeline_manifest.json").exists():
        return True
    return any((path / filename).exists() for _, filename in AGENT_FILES.values())
