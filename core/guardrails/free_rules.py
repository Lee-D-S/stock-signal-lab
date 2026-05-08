from __future__ import annotations

from pathlib import Path
from typing import Any

from core.agents.free_base import AgentStatus, Candidate, OrderProposal, PortfolioPosition


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
        warnings.append("cash is insufficient for suggested_amount.")

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
            warnings.append("sector is missing; sector concentration could not be checked.")

    if min_liquidity_value is None or min_liquidity_value <= 0:
        statuses.append("needs_review")
        warnings.append("liquidity data is missing or invalid.")
    elif suggested_amount > min_liquidity_value * 0.01:
        statuses.append("needs_review")
        warnings.append("suggested_amount exceeds 1% of observed trade amount.")

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
        warnings.append("no research file found for candidate.")

    if not candidate.ticker or len(candidate.ticker) != 6 or not candidate.ticker.isdigit():
        statuses.append("block")
        warnings.append("ticker must be a 6 digit code.")

    if candidate.source_type == "manual" and not candidate.source:
        statuses.append("needs_review")
        warnings.append("manual candidate must retain a source.")

    if research_quality:
        missing_sections = research_quality.get("missing_required_sections", [])
        if missing_sections:
            statuses.append("needs_review")
            warnings.append("research file is missing required sections: " + ", ".join(missing_sections))
        if research_quality.get("is_stale"):
            statuses.append("needs_review")
            warnings.append("research file is stale.")
        if research_quality.get("forbidden_keyword_hits"):
            statuses.append("needs_review")
            warnings.append("research file contains unverified/rumor keywords.")

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
            warnings.append(f"order proposal field is empty: {key}")

    if proposal.side not in {"buy", "sell", "hold"}:
        statuses.append("block")
        warnings.append("order proposal side must be buy, sell, or hold.")

    if proposal.side != "hold" and proposal.suggested_quantity <= 0:
        statuses.append("needs_review")
        warnings.append("suggested_quantity is not positive.")

    if proposal.side == "hold" and (proposal.suggested_amount != 0 or proposal.suggested_quantity != 0):
        statuses.append("block")
        warnings.append("hold proposal must have zero amount and zero quantity.")

    if proposal.execution_allowed:
        statuses.append("block")
        warnings.append("execution_allowed must remain false in the free pipeline.")

    return worst_status(statuses), warnings
