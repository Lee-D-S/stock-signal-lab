from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Literal


AgentStatus = Literal["approve", "block", "needs_review", "info"]
OrderSide = Literal["buy", "sell", "hold"]


@dataclass(frozen=True)
class Candidate:
    ticker: str
    name: str = ""
    source: str = "manual"
    source_type: str = "manual"
    score: float | None = None
    suggested_amount: int | None = None
    signal_count: int = 1
    signal_details: list[dict[str, Any]] = field(default_factory=list)
    current_price: float | None = None
    trade_amount: float | None = None
    sector: str = ""


@dataclass(frozen=True)
class PortfolioPosition:
    ticker: str
    name: str = ""
    quantity: int = 0
    avg_price: float = 0.0
    current_price: float = 0.0
    sector: str = ""

    @property
    def market_value(self) -> float:
        price = self.current_price or self.avg_price
        return max(self.quantity, 0) * max(price, 0.0)


@dataclass
class AgentContext:
    run_date: date
    candidates: list[Candidate]
    run_id: str = ""
    portfolio: list[PortfolioPosition] = field(default_factory=list)
    cash: float = 0.0
    output_dir: Path = Path("legacy/data/agent_runs")
    research_root: Path = Path("legacy/research_data/ai 주가 변동 원인 분석/00_기업별분석")
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def run_dir(self) -> Path:
        if self.run_id:
            return self.output_dir / self.run_date.isoformat() / self.run_id
        return self.output_dir / self.run_date.isoformat()

    @property
    def portfolio_value(self) -> float:
        return self.cash + sum(position.market_value for position in self.portfolio)


@dataclass
class AgentResult:
    agent: str
    status: AgentStatus
    summary: str
    signals: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    required_human_checks: list[str] = field(default_factory=list)
    artifacts: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


@dataclass(frozen=True)
class OrderProposal:
    ticker: str
    name: str
    side: OrderSide
    suggested_amount: int
    suggested_quantity: int
    order_type_hint: str
    reason: str
    risk_status: AgentStatus
    compliance_status: AgentStatus
    human_checklist: list[str]
    portfolio_side: OrderSide = "hold"
    final_side_reason: str = ""
    price_used: float | None = None
    price_source: str = ""
    risk_warnings: list[str] = field(default_factory=list)
    compliance_warnings: list[str] = field(default_factory=list)
    execution_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
