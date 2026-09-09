from __future__ import annotations

from pathlib import Path
from typing import Any

from core.agents.free_base import AgentStatus, Candidate, OrderProposal, PortfolioPosition

RESEARCH_SECTION_LABELS = {
    "business_model": "사업모델",
    "investment_thesis": "투자 가설",
    "risk": "리스크",
    "disconfirmation": "반증 조건",
}


def label_research_sections(items: list[str]) -> list[str]:
    return [RESEARCH_SECTION_LABELS.get(item, item) for item in items]


def worst_status(statuses: list[AgentStatus]) -> AgentStatus:
    priority = {"block": 3, "needs_review": 2, "approve": 1, "info": 0}
    if not statuses:
        return "info"
    return max(statuses, key=lambda status: priority[status])


def check_risk_rules(
    *,
    candidate: Candidate,
    portfolio: list[PortfolioPosition],
    portfolio_value: float,
    cash: float,
    suggested_amount: int,
    max_position_pct: float,
    max_sector_pct: float,
    max_order_amount: int,
    max_daily_new_buy_amount: int,
    daily_new_buy_amount: int,
    min_liquidity_value: int | None = None,
) -> tuple[AgentStatus, list[str]]:
    warnings: list[str] = []
    statuses: list[AgentStatus] = ["approve"]

    if suggested_amount <= 0:
        statuses.append("block")
        warnings.append("suggested_amount must be positive.")

    if suggested_amount > max_order_amount:
        statuses.append("block")
        warnings.append(f"suggested_amount exceeds max_order_amount ({max_order_amount:,}).")

    if suggested_amount > cash:
        statuses.append("needs_review")
        warnings.append("현금이 부족하여 제안 금액을 충당할 수 없습니다.")

    if daily_new_buy_amount > max_daily_new_buy_amount:
        statuses.append("block")
        warnings.append(
            f"daily new buy amount {daily_new_buy_amount:,} exceeds limit {max_daily_new_buy_amount:,}."
        )

    if portfolio_value <= 0:
        statuses.append("needs_review")
        warnings.append("portfolio snapshot is missing; position weight could not be checked.")
    else:
        existing_value = sum(
            position.market_value for position in portfolio if position.ticker == candidate.ticker
        )
        projected_pct = (existing_value + suggested_amount) / portfolio_value
        if projected_pct > max_position_pct:
            statuses.append("block")
            warnings.append(
                f"projected position weight {projected_pct:.1%} exceeds limit {max_position_pct:.1%}."
            )
        sector = candidate.sector or next(
            (position.sector for position in portfolio if position.ticker == candidate.ticker),
            "",
        )
        if sector:
            sector_value = sum(
                position.market_value for position in portfolio if position.sector == sector
            )
            projected_sector_pct = (sector_value + suggested_amount) / portfolio_value
            if projected_sector_pct > max_sector_pct:
                statuses.append("block")
                warnings.append(
                    f"projected sector weight {projected_sector_pct:.1%} exceeds limit {max_sector_pct:.1%}."
                )
        else:
            statuses.append("needs_review")
            warnings.append("섹터 정보가 없어 섹터 집중도를 확인할 수 없습니다.")

    if min_liquidity_value is None or min_liquidity_value <= 0:
        statuses.append("needs_review")
        warnings.append("유동성 데이터가 없거나 유효하지 않습니다.")
    elif suggested_amount > min_liquidity_value * 0.01:
        statuses.append("needs_review")
        warnings.append("제안 금액이 관측 거래대금의 1%를 초과합니다.")

    return worst_status(statuses), warnings


def check_compliance_rules(
    *,
    candidate: Candidate,
    research_matches: list[Path],
    require_research_file: bool,
    research_quality: dict[str, Any] | None = None,
) -> tuple[AgentStatus, list[str]]:
    warnings: list[str] = []
    statuses: list[AgentStatus] = ["approve"]

    if require_research_file and not research_matches:
        statuses.append("needs_review")
        warnings.append("후보에 대한 리서치 파일을 찾지 못했습니다.")

    if not candidate.ticker or len(candidate.ticker) != 6 or not candidate.ticker.isdigit():
        statuses.append("block")
        warnings.append("종목코드는 6자리 숫자여야 합니다.")

    if candidate.source_type == "manual" and not candidate.source:
        statuses.append("needs_review")
        warnings.append("수동 후보는 원천 정보를 유지해야 합니다.")

    if research_quality:
        missing_sections = research_quality.get("missing_required_sections", [])
        if missing_sections:
            statuses.append("needs_review")
            warnings.append("리서치 파일에 필수 섹션이 없습니다: " + ", ".join(label_research_sections(missing_sections)))
        if research_quality.get("is_stale"):
            statuses.append("needs_review")
            warnings.append("리서치 파일이 오래됐습니다.")
        if research_quality.get("forbidden_keyword_hits"):
            statuses.append("needs_review")
            warnings.append("리서치 파일에 검증되지 않은 정보나 루머 키워드가 있습니다.")

    return worst_status(statuses), warnings


def check_order_proposal_rules(proposal: OrderProposal) -> tuple[AgentStatus, list[str]]:
    warnings: list[str] = []
    statuses: list[AgentStatus] = ["approve"]

    required: dict[str, Any] = proposal.to_dict()
    optional_fields = {
        "price_used",
        "price_source",
        "risk_warnings",
        "compliance_warnings",
    }
    for key, value in required.items():
        if key in optional_fields:
            continue
        if value in ("", None, []):
            statuses.append("needs_review")
            warnings.append(f"주문 초안 항목이 비어 있습니다: {key}")

    if proposal.side not in {"buy", "sell", "hold"}:
        statuses.append("block")
        warnings.append("주문 초안 방향은 buy, sell, hold 중 하나여야 합니다.")

    if proposal.side != "hold" and proposal.suggested_quantity <= 0:
        statuses.append("needs_review")
        warnings.append("제안 수량이 양수가 아닙니다.")

    if proposal.side == "hold" and (proposal.suggested_amount != 0 or proposal.suggested_quantity != 0):
        statuses.append("block")
        warnings.append("hold 초안은 금액과 수량이 모두 0이어야 합니다.")

    if proposal.execution_allowed:
        statuses.append("block")
        warnings.append("free pipeline에서는 execution_allowed가 false여야 합니다.")

    return worst_status(statuses), warnings
