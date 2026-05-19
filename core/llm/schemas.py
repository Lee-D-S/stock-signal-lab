from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


ReviewStatus = Literal["approve", "block", "needs_review", "info", "skipped"]


@dataclass(frozen=True)
class AgentReview:
    agent: str
    status: ReviewStatus
    summary: str
    objections: list[str] = field(default_factory=list)
    human_questions: list[str] = field(default_factory=list)
    source_agent: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FinalGate:
    python_status: str
    python_execution_allowed: bool
    llm_recommendation: ReviewStatus
    effective_status: str
    hard_block_agents: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LLMReview:
    engine: str
    status: ReviewStatus
    run_id: str
    run_date: str
    run_dir: str
    model: str
    agents: dict[str, AgentReview]
    final_gate: FinalGate
    skipped_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["agents"] = {
            name: review.to_dict()
            for name, review in self.agents.items()
        }
        data["final_gate"] = self.final_gate.to_dict()
        return data

