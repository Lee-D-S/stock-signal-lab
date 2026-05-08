from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from config import settings
from core.guardrails import (
    check_compliance_rules,
    check_order_proposal_rules,
    check_risk_rules,
)

from .free_base import (
    AgentContext,
    AgentResult,
    Candidate,
    OrderProposal,
    PortfolioPosition,
)


TICKER_RE = re.compile(r"\b\d{6}\b")
REQUIRED_RESEARCH_SECTIONS = {
    "business_model": ("사업모델", "business model", "BM", "매출 구조"),
    "investment_thesis": ("투자 가설", "investment thesis", "thesis", "상승 근거"),
    "risk": ("리스크", "risk", "위험"),
    "disconfirmation": ("반증 조건", "반증", "disconfirm", "무효화"),
}
FORBIDDEN_RESEARCH_KEYWORDS = ("루머", "찌라시", "미확인", "확인필요", "미공개")
RESEARCH_STALE_DAYS = 120
LIQUIDITY_RATIO_LIMIT = 0.01
ANALYST_SECTION_KEYWORDS = {
    "business_model": ("사업모델", "business model", "BM", "매출 구조", "수익 구조"),
    "earnings_quality": ("실적", "매출", "영업이익", "순이익", "earnings"),
    "balance_sheet": ("재무", "부채", "현금", "차입", "balance sheet"),
    "valuation": ("밸류에이션", "valuation", "PER", "PBR", "EV/EBITDA"),
    "catalyst": ("촉매", "catalyst", "수주", "공시", "정책", "업황"),
    "risk": ("리스크", "risk", "위험"),
    "disconfirmation": ("반증", "반증 조건", "disconfirm", "무효화"),
}
ANALYST_STRONG_SECTIONS = {
    "business_model",
    "earnings_quality",
    "valuation",
    "risk",
    "disconfirmation",
}
ANALYST_STALE_DAYS = 120


class QuantSignalAgent:
    name = "QuantSignalAgent"

    def run(self, context: AgentContext) -> AgentResult:
        signals: list[dict[str, Any]] = []
        warnings: list[str] = []
        stale_days = int(context.config.get("stale_signal_days", 3))
        for candidate in context.candidates:
            signal = asdict(candidate)
            source_path = Path(candidate.source)
            if source_path.exists():
                age_days = (datetime.now().timestamp() - source_path.stat().st_mtime) / 86400
                signal["source_age_days"] = round(age_days, 1)
                if age_days > stale_days:
                    warnings.append(
                        f"{candidate.ticker}: source signal is stale ({age_days:.1f} days)."
                    )
            elif candidate.source_type != "manual":
                warnings.append(f"{candidate.ticker}: source file does not exist: {candidate.source}")
            if not candidate.ticker or not TICKER_RE.fullmatch(candidate.ticker):
                warnings.append(f"{candidate.ticker or '<missing>'}: ticker is not a 6 digit code.")
            if not candidate.name:
                warnings.append(f"{candidate.ticker}: name is missing.")
            if candidate.current_price is None:
                warnings.append(f"{candidate.ticker}: current_price is missing.")
            if candidate.trade_amount is None:
                warnings.append(f"{candidate.ticker}: trade_amount is missing.")
            if candidate.signal_count <= 0:
                warnings.append(f"{candidate.ticker}: signal_count is missing.")
            if not candidate.signal_details:
                warnings.append(f"{candidate.ticker}: signal_details is empty.")
            signals.append(signal)
        if not signals:
            warnings.append("no candidates were provided or discovered.")

        return AgentResult(
            agent=self.name,
            status="info" if signals else "needs_review",
            summary=f"{len(signals)} candidate(s) prepared for rule-based review.",
            signals=signals,
            warnings=warnings,
            required_human_checks=[
                "Confirm the source signal is still valid before any manual order.",
                "Review candidates with missing price or liquidity data before Risk and Trader steps.",
            ],
            artifacts={
                "candidate_count": len(signals),
                "source_type_counts": count_by_key(signals, "source_type"),
                "stale_signal_days": stale_days,
            },
        )


class EquityResearchAnalystAgent:
    name = "EquityResearchAnalystAgent"

    def run(self, context: AgentContext) -> AgentResult:
        signals: list[dict[str, Any]] = []
        warnings: list[str] = []
        statuses: list[str] = []

        for candidate in context.candidates:
            matches = find_research_files(context.research_root, candidate)
            analysis = analyze_candidate_research(candidate, matches)
            status = analysis.pop("agent_status")
            statuses.append(status)
            if status != "approve":
                warnings.append(
                    f"{candidate.ticker}: analyst review is {analysis['analysis_status']} "
                    f"({', '.join(analysis['missing_items']) or 'no missing item detail'})."
                )
            if analysis["forbidden_keyword_hits"]:
                warnings.append(
                    f"{candidate.ticker}: analyst source contains caution keywords - "
                    + ", ".join(analysis["forbidden_keyword_hits"])
                )
            if analysis.get("readability") not in {"ok", "missing"}:
                warnings.append(
                    f"{candidate.ticker}: analyst source readability is {analysis['readability']}."
                )
            signals.append(analysis)

        if not context.candidates:
            warnings.append("no candidates available for analyst review.")

        return AgentResult(
            agent=self.name,
            status=combine_statuses(statuses),
            summary=f"{len(signals)} candidate research review(s) prepared.",
            signals=signals,
            warnings=warnings,
            required_human_checks=[
                "Fill missing business, earnings, valuation, risk, and disconfirmation items before treating a candidate as investable.",
                "Review stale or partially readable source files before Portfolio and Compliance treat the analysis as reliable.",
            ],
            artifacts={
                "research_root": str(context.research_root),
                "confidence_counts": count_by_key(signals, "confidence"),
                "analysis_status_counts": count_by_key(signals, "analysis_status"),
            },
        )


class ResearchFileAgent:
    name = "ResearchFileAgent"

    def run(self, context: AgentContext, analyst_result: AgentResult | None = None) -> AgentResult:
        signals: list[dict[str, Any]] = []
        warnings: list[str] = []
        analyst_by_ticker = result_by_ticker(analyst_result)
        if not context.research_root.exists():
            warnings.append(f"research root does not exist: {context.research_root}")
        for candidate in context.candidates:
            matches = find_research_files(context.research_root, candidate)
            quality = analyze_research_files(matches, context.run_date)
            analyst_signal = analyst_by_ticker.get(candidate.ticker, {})
            analyst_files = set(analyst_signal.get("source_files", []))
            matched_files = {str(path) for path in matches}
            missing_analyst_files = sorted(path for path in analyst_files if path not in matched_files)
            analyst_source_file_mismatch = bool(missing_analyst_files)
            analyst_missing_items = set(analyst_signal.get("missing_items", []))
            research_missing_items = set(quality["missing_required_sections"])
            missing_item_mismatch = bool(analyst_missing_items.symmetric_difference(research_missing_items))
            analyst_complete_research_incomplete = (
                analyst_signal.get("analysis_status") == "complete"
                and bool(quality["missing_required_sections"])
            )
            if not matches:
                warnings.append(f"{candidate.ticker}: no research file found.")
            if quality["missing_required_sections"]:
                warnings.append(
                    f"{candidate.ticker}: missing research sections - "
                    + ", ".join(quality["missing_required_sections"])
                )
            if quality["readability"] != "ok":
                warnings.append(f"{candidate.ticker}: research file readability is {quality['readability']}.")
            if quality["is_stale"]:
                warnings.append(f"{candidate.ticker}: research file is stale.")
            if quality["forbidden_keyword_hits"]:
                warnings.append(
                    f"{candidate.ticker}: research file contains caution keywords - "
                    + ", ".join(quality["forbidden_keyword_hits"])
                )
            if analyst_source_file_mismatch:
                warnings.append(f"{candidate.ticker}: analyst source files do not match research files.")
            if analyst_complete_research_incomplete:
                warnings.append(f"{candidate.ticker}: analyst marked complete but research file is incomplete.")
            signals.append({
                "ticker": candidate.ticker,
                "name": candidate.name,
                "research_files": [str(path) for path in matches[:5]],
                "latest_modified": quality["latest_modified"],
                "missing_required_sections": quality["missing_required_sections"],
                "forbidden_keyword_hits": quality["forbidden_keyword_hits"],
                "is_stale": quality["is_stale"],
                "readability": quality["readability"],
                "latest_file": quality["latest_file"],
                "latest_file_age_days": quality["latest_file_age_days"],
                "file_count": len(matches),
                "quality_status": research_quality_status(matches, quality),
                "analyst_source_file_mismatch": analyst_source_file_mismatch,
                "missing_analyst_source_files": missing_analyst_files,
                "analyst_analysis_status": analyst_signal.get("analysis_status", ""),
                "analyst_missing_items": sorted(analyst_missing_items),
                "analyst_research_missing_item_mismatch": missing_item_mismatch,
                "analyst_complete_research_incomplete": analyst_complete_research_incomplete,
            })

        status = "approve"
        if warnings:
            status = "needs_review"
        if not context.candidates:
            status = "needs_review"
            warnings.append("no candidates available for research file review.")

        return AgentResult(
            agent=self.name,
            status=status,
            summary="Research file presence check completed.",
            signals=signals,
            warnings=warnings,
            required_human_checks=[
                "Read the matched research files and update the investment thesis manually.",
                "If no usable report exists, run scripts/run_new_company_reports.py --include-existing-missing.",
                "Resolve Analyst and ResearchFile mismatches before Compliance treats the evidence chain as complete.",
            ],
            artifacts={
                "research_root": str(context.research_root),
                "quality_status_counts": count_by_key(signals, "quality_status"),
                "readability_counts": count_by_key(signals, "readability"),
                "stale_threshold_days": RESEARCH_STALE_DAYS,
            },
        )


class PortfolioManagerAgent:
    name = "PortfolioManagerAgent"

    def run(
        self,
        context: AgentContext,
        analyst_result: AgentResult | None = None,
        research_result: AgentResult | None = None,
    ) -> AgentResult:
        default_amount = int(context.config["default_suggested_amount"])
        signals: list[dict[str, Any]] = []
        warnings: list[str] = []
        portfolio_by_ticker = {position.ticker: position for position in context.portfolio}
        analyst_by_ticker = result_by_ticker(analyst_result)
        research_by_ticker = result_by_ticker(research_result)
        if context.portfolio_value <= 0:
            warnings.append("portfolio snapshot is missing or has zero value.")
        if not context.candidates:
            warnings.append("no candidates available for portfolio review.")
        for position in context.portfolio:
            if position.quantity < 0:
                warnings.append(f"{position.ticker}: portfolio position quantity is negative.")
            if position.quantity > 0 and position.market_value <= 0:
                warnings.append(f"{position.ticker}: portfolio position market value is zero.")
        for candidate in context.candidates:
            amount = candidate.suggested_amount or default_amount
            current_position = portfolio_by_ticker.get(candidate.ticker)
            analyst_signal = analyst_by_ticker.get(candidate.ticker, {})
            research_signal = research_by_ticker.get(candidate.ticker, {})
            analysis_status = str(analyst_signal.get("analysis_status", "missing"))
            analyst_confidence = str(analyst_signal.get("confidence", "none"))
            research_quality_status = str(research_signal.get("quality_status", "missing"))
            research_is_stale = bool(research_signal.get("is_stale", False))
            research_source_mismatch = bool(research_signal.get("analyst_source_file_mismatch", False))
            side = "hold"
            base_priority = candidate.score if candidate.score is not None else float(candidate.signal_count)
            priority = adjusted_portfolio_priority(
                base_priority,
                analysis_status,
                analyst_confidence,
                research_quality_status,
            )
            portfolio_constraints: list[str] = []
            if current_position:
                reason = "Existing position; review hold, add, or trim based on thesis and weight."
                action_detail = "review_existing_position"
                if analyst_signal.get("key_risks"):
                    portfolio_constraints.append("analyst_key_risks_present")
            elif context.portfolio_value <= 0:
                reason = "Portfolio snapshot is missing; allocation decision requires review."
                action_detail = "needs_portfolio_snapshot"
                portfolio_constraints.append("missing_portfolio_snapshot")
                warnings.append(f"{candidate.ticker}: portfolio snapshot is missing.")
            elif amount > context.cash:
                reason = "Cash is insufficient for a new buy draft."
                action_detail = "cash_limited"
                portfolio_constraints.append("cash_limited")
                warnings.append(f"{candidate.ticker}: cash is insufficient.")
            elif analysis_status != "complete":
                reason = "Equity research is incomplete; allocation decision requires analyst review."
                action_detail = "needs_equity_research"
                portfolio_constraints.append("incomplete_equity_research")
                warnings.append(f"{candidate.ticker}: equity research is {analysis_status}.")
            elif research_quality_status != "usable" or research_is_stale or research_source_mismatch:
                reason = "Research file quality is insufficient for a new buy draft."
                action_detail = "needs_research_file_quality"
                portfolio_constraints.append(f"research_quality_{research_quality_status}")
                if research_is_stale:
                    portfolio_constraints.append("stale_research")
                if research_source_mismatch:
                    portfolio_constraints.append("analyst_research_source_mismatch")
                warnings.append(f"{candidate.ticker}: research file quality is {research_quality_status}.")
            else:
                side = "buy"
                reason = "New candidate with available cash; draft only, subject to risk and compliance."
                action_detail = "new_buy_candidate"
            current_value = current_position.market_value if current_position else 0.0
            current_weight = current_value / context.portfolio_value if context.portfolio_value > 0 else 0.0
            signals.append({
                "ticker": candidate.ticker,
                "name": candidate.name,
                "side": side,
                "action_detail": action_detail,
                "suggested_amount": amount,
                "priority": priority,
                "base_priority": base_priority,
                "current_weight": current_weight,
                "reason": reason,
                "analysis_status": analysis_status,
                "analyst_confidence": analyst_confidence,
                "analyst_key_risks": analyst_signal.get("key_risks", []),
                "analyst_missing_items": analyst_signal.get("missing_items", []),
                "research_quality_status": research_quality_status,
                "research_is_stale": research_is_stale,
                "research_missing_required_sections": research_signal.get("missing_required_sections", []),
                "research_source_mismatch": research_source_mismatch,
                "portfolio_constraints": portfolio_constraints,
            })

        return AgentResult(
            agent=self.name,
            status="needs_review" if warnings or not signals else "info",
            summary=f"{len(signals)} portfolio action(s) drafted.",
            signals=signals,
            warnings=warnings,
            required_human_checks=[
                "Compare each action with current cash, allocation policy, and thesis quality."
            ],
            artifacts={
                "portfolio_value": context.portfolio_value,
                "cash": context.cash,
                "position_count": len(context.portfolio),
                "candidate_count": len(context.candidates),
                "sector_exposure": portfolio_sector_exposure(context.portfolio, context.portfolio_value),
                "side_counts": count_by_key(signals, "side"),
                "action_detail_counts": count_by_key(signals, "action_detail"),
            },
        )


class RiskManagerAgent:
    name = "RiskManagerAgent"

    def run(
        self,
        context: AgentContext,
        analyst_result: AgentResult | None = None,
        research_result: AgentResult | None = None,
    ) -> AgentResult:
        signals: list[dict[str, Any]] = []
        all_warnings: list[str] = []
        statuses: list[str] = []
        default_amount = int(context.config["default_suggested_amount"])
        analyst_by_ticker = result_by_ticker(analyst_result)
        research_by_ticker = result_by_ticker(research_result)
        daily_new_buy_amount = sum(
            int(candidate.suggested_amount or default_amount)
            for candidate in context.candidates
            if not any(position.ticker == candidate.ticker for position in context.portfolio)
        )
        if not context.candidates:
            all_warnings.append("no candidates available for risk review.")
        if context.portfolio_value <= 0:
            all_warnings.append("portfolio snapshot is missing or has zero value.")
        for candidate in context.candidates:
            amount = candidate.suggested_amount or default_amount
            projection = build_risk_projection(candidate, context.portfolio, context.portfolio_value, amount)
            status, warnings = check_risk_rules(
                candidate=candidate,
                portfolio=context.portfolio,
                portfolio_value=context.portfolio_value,
                cash=context.cash,
                suggested_amount=amount,
                max_position_pct=float(context.config["max_position_pct"]),
                max_sector_pct=float(context.config["max_sector_pct"]),
                max_order_amount=int(context.config["max_order_amount"]),
                max_daily_new_buy_amount=int(context.config["max_daily_new_buy_amount"]),
                daily_new_buy_amount=daily_new_buy_amount,
                min_liquidity_value=int(candidate.trade_amount) if candidate.trade_amount else None,
            )
            analyst_signal = analyst_by_ticker.get(candidate.ticker, {})
            research_signal = research_by_ticker.get(candidate.ticker, {})
            if analyst_signal.get("key_risks"):
                warnings.append("analyst key risks require manual risk review.")
                status = combine_statuses([status, "needs_review"])
            if research_signal.get("quality_status") in {"missing", "incomplete", "stale", "unreadable"}:
                warnings.append("research quality is weak; risk inputs may be incomplete.")
                status = combine_statuses([status, "needs_review"])
            statuses.append(status)
            all_warnings.extend(f"{candidate.ticker}: {warning}" for warning in warnings)
            signals.append({
                "ticker": candidate.ticker,
                "status": status,
                "suggested_amount": amount,
                "projected_position_pct": projection["projected_position_pct"],
                "projected_sector_pct": projection["projected_sector_pct"],
                "liquidity_ratio": projection["liquidity_ratio"],
                "sector": projection["sector"],
                "existing_position_value": projection["existing_position_value"],
                "existing_sector_value": projection["existing_sector_value"],
                "risk_constraints": {
                    "max_position_pct": context.config["max_position_pct"],
                    "max_sector_pct": context.config["max_sector_pct"],
                    "max_order_amount": context.config["max_order_amount"],
                    "max_daily_new_buy_amount": context.config["max_daily_new_buy_amount"],
                    "liquidity_ratio_limit": LIQUIDITY_RATIO_LIMIT,
                    "cash": context.cash,
                    "portfolio_value": context.portfolio_value,
                    "daily_new_buy_amount": daily_new_buy_amount,
                },
                "analyst_key_risks": analyst_signal.get("key_risks", []),
                "analyst_disconfirmation_conditions": analyst_signal.get("disconfirmation_conditions", []),
                "research_quality_status": research_signal.get("quality_status", ""),
                "warnings": warnings,
            })

        return AgentResult(
            agent=self.name,
            status=combine_statuses(statuses),
            summary="Risk guardrail checks completed.",
            signals=signals,
            warnings=all_warnings,
            required_human_checks=[
                "Do not place orders for blocked candidates.",
                "Manually confirm liquidity and event risk before any order.",
            ],
            artifacts={
                "max_position_pct": context.config["max_position_pct"],
                "max_sector_pct": context.config["max_sector_pct"],
                "max_order_amount": context.config["max_order_amount"],
                "max_daily_new_buy_amount": context.config["max_daily_new_buy_amount"],
                "daily_new_buy_amount": daily_new_buy_amount,
                "liquidity_ratio_limit": LIQUIDITY_RATIO_LIMIT,
                "status_counts": count_by_key(signals, "status"),
            },
        )


class ComplianceOfficerAgent:
    name = "ComplianceOfficerAgent"

    def run(
        self,
        context: AgentContext,
        analyst_result: AgentResult | None = None,
        research_result: AgentResult | None = None,
    ) -> AgentResult:
        signals: list[dict[str, Any]] = []
        all_warnings: list[str] = []
        statuses: list[str] = []
        require_research = bool(context.config["require_research_file"])
        analyst_by_ticker = result_by_ticker(analyst_result)
        research_by_ticker = result_by_ticker(research_result)
        if not context.candidates:
            all_warnings.append("no candidates available for compliance review.")
        for candidate in context.candidates:
            analyst_signal = analyst_by_ticker.get(candidate.ticker, {})
            research_signal = research_by_ticker.get(candidate.ticker, {})
            if research_signal:
                matches = [Path(path) for path in research_signal.get("research_files", [])]
                quality = {
                    "missing_required_sections": research_signal.get("missing_required_sections", []),
                    "forbidden_keyword_hits": research_signal.get("forbidden_keyword_hits", []),
                    "is_stale": research_signal.get("is_stale", False),
                    "readability": research_signal.get("readability", "missing"),
                }
            else:
                matches = find_research_files(context.research_root, candidate)
                quality = analyze_research_files(matches, context.run_date)
            status, warnings = check_compliance_rules(
                candidate=candidate,
                research_matches=matches,
                require_research_file=require_research,
                research_quality=quality,
            )
            record_status = compliance_record_status(candidate, analyst_signal, research_signal, matches)
            manual_source_status = manual_source_status_for(candidate)
            compliance_checks = {
                "ticker_format": "pass" if TICKER_RE.fullmatch(candidate.ticker or "") else "fail",
                "source_traceable": "pass" if candidate.source else "fail",
                "manual_source": manual_source_status,
                "research_file_present": "pass" if matches else "fail",
                "research_quality": str(research_signal.get("quality_status", research_quality_status(matches, quality))),
                "forbidden_keywords": "fail" if quality.get("forbidden_keyword_hits") else "pass",
                "analyst_result_present": "pass" if analyst_signal else "fail",
                "analyst_research_match": "fail"
                if research_signal.get("analyst_source_file_mismatch")
                or research_signal.get("analyst_research_missing_item_mismatch")
                else "pass",
                "record_status": record_status,
            }
            if analyst_signal:
                if analyst_signal.get("analysis_status") != "complete":
                    warnings.append("equity analyst review is incomplete.")
                    status = combine_statuses([status, "needs_review"])
                if analyst_signal.get("confidence") in {"low", "none"}:
                    warnings.append("equity analyst confidence is low.")
                    status = combine_statuses([status, "needs_review"])
                analyst_forbidden_hits = analyst_signal.get("forbidden_keyword_hits", [])
                if analyst_forbidden_hits:
                    warnings.append("equity analyst found caution keywords.")
                    status = combine_statuses([
                        status,
                        "block" if contains_blocking_information(analyst_forbidden_hits) else "needs_review",
                    ])
                if analyst_signal.get("analysis_status") == "complete" and research_signal.get("quality_status") in {
                    "missing",
                    "incomplete",
                    "stale",
                    "unreadable",
                }:
                    warnings.append("analyst marked complete but research file review is not usable.")
                    status = combine_statuses([status, "needs_review"])
            else:
                warnings.append("equity analyst result is missing.")
                status = combine_statuses([status, "needs_review"])
            if record_status != "complete":
                warnings.append(f"record status is {record_status}.")
                status = combine_statuses([status, "needs_review"])
            if manual_source_status == "missing":
                warnings.append("manual candidate source is missing.")
                status = combine_statuses([status, "needs_review"])
            if research_signal.get("analyst_source_file_mismatch"):
                warnings.append("analyst source files do not match ResearchFile review.")
                status = combine_statuses([status, "needs_review"])
            if research_signal.get("analyst_research_missing_item_mismatch"):
                warnings.append("analyst missing items conflict with ResearchFile missing sections.")
                status = combine_statuses([status, "needs_review"])
            if quality.get("forbidden_keyword_hits") and contains_blocking_information(quality["forbidden_keyword_hits"]):
                status = combine_statuses([status, "block"])
            statuses.append(status)
            all_warnings.extend(f"{candidate.ticker}: {warning}" for warning in warnings)
            signals.append({
                "ticker": candidate.ticker,
                "status": status,
                "research_file_count": len(matches),
                "missing_required_sections": quality["missing_required_sections"],
                "forbidden_keyword_hits": quality["forbidden_keyword_hits"],
                "record_status": record_status,
                "manual_source_status": manual_source_status,
                "analyst_status": analyst_signal.get("analysis_status", ""),
                "analyst_confidence": analyst_signal.get("confidence", ""),
                "analyst_source_files": analyst_signal.get("source_files", []),
                "research_quality_status": research_signal.get("quality_status", research_quality_status(matches, quality)),
                "source_file_mismatch": bool(research_signal.get("analyst_source_file_mismatch", False)),
                "missing_item_mismatch": bool(research_signal.get("analyst_research_missing_item_mismatch", False)),
                "compliance_checks": compliance_checks,
                "warnings": warnings,
            })

        return AgentResult(
            agent=self.name,
            status=combine_statuses(statuses),
            summary="Compliance checklist completed.",
            signals=signals,
            warnings=all_warnings,
            required_human_checks=[
                "Confirm there is no non-public information, rumor-only thesis, or missing record.",
                "Resolve Analyst and ResearchFile evidence mismatches before treating the candidate as compliant.",
            ],
            artifacts={
                "require_research_file": require_research,
                "status_counts": count_by_key(signals, "status"),
                "record_status_counts": count_by_key(signals, "record_status"),
                "research_quality_counts": count_by_key(signals, "research_quality_status"),
            },
        )


class TraderAgent:
    name = "TraderAgent"

    def run(
        self,
        context: AgentContext,
        portfolio_result: AgentResult,
        risk_result: AgentResult,
        compliance_result: AgentResult,
    ) -> AgentResult:
        default_amount = int(context.config["default_suggested_amount"])
        portfolio_by_ticker = {
            signal["ticker"]: signal for signal in portfolio_result.signals
        }
        risk_by_ticker = {signal["ticker"]: signal["status"] for signal in risk_result.signals}
        compliance_by_ticker = {
            signal["ticker"]: signal["status"] for signal in compliance_result.signals
        }

        proposals: list[OrderProposal] = []
        warnings: list[str] = []
        statuses: list[str] = []
        for candidate in context.candidates:
            risk_status = risk_by_ticker.get(candidate.ticker, "needs_review")
            compliance_status = compliance_by_ticker.get(candidate.ticker, "needs_review")
            portfolio_signal = portfolio_by_ticker.get(candidate.ticker, {})
            portfolio_side = portfolio_signal.get("side", "hold")
            side = portfolio_side if portfolio_side in {"buy", "sell", "hold"} else "hold"
            if risk_status != "approve" or compliance_status != "approve":
                side = "hold"
            amount = int(candidate.suggested_amount or default_amount)
            price = candidate.current_price or portfolio_signal.get("current_price")
            quantity = int(amount // price) if price and price > 0 and side != "hold" else 0
            proposal = OrderProposal(
                ticker=candidate.ticker,
                name=candidate.name or candidate.ticker,
                side=side,
                suggested_amount=0 if side == "hold" else amount,
                suggested_quantity=quantity,
                order_type_hint="manual_review_limit_order",
                reason=portfolio_signal.get(
                    "reason",
                    "Rule-based draft only; no broker API call was made.",
                ),
                risk_status=risk_status,
                compliance_status=compliance_status,
                human_checklist=[
                    "Confirm current price and calculate quantity manually.",
                    "Confirm ticker, side, amount, and order type before entering any order.",
                    "Skip if Risk or Compliance status is block or needs_review.",
                ],
            )
            status, proposal_warnings = check_order_proposal_rules(proposal)
            if risk_status != "approve" or compliance_status != "approve":
                status = "needs_review" if status != "block" else status
                proposal_warnings.append("risk or compliance is not approved; order proposal is hold.")
            statuses.append(status)
            warnings.extend(f"{candidate.ticker}: {warning}" for warning in proposal_warnings)
            proposals.append(proposal)

        return AgentResult(
            agent=self.name,
            status=combine_statuses(statuses),
            summary=f"{len(proposals)} draft order proposal(s) generated without execution.",
            signals=[proposal.to_dict() for proposal in proposals],
            warnings=warnings,
            required_human_checks=[
                "This pipeline never executes orders. Entering an order is a separate manual action."
            ],
            artifacts={"broker_api_called": False},
        )


class OperationsReportAgent:
    name = "OperationsReportAgent"

    def run(self, context: AgentContext, results: list[AgentResult]) -> AgentResult:
        report_path = context.run_dir / "final_committee_report.md"
        summary_path = context.run_dir / "telegram_summary.txt"
        report_path.write_text(render_markdown_report(context, results), encoding="utf-8")
        summary_path.write_text(render_short_summary(context, results), encoding="utf-8")
        return AgentResult(
            agent=self.name,
            status=combine_statuses([result.status for result in results]),
            summary=f"Final report written to {report_path}.",
            artifacts={"report_path": str(report_path), "telegram_summary_path": str(summary_path)},
        )


class FreeAgentPipeline:
    def __init__(self) -> None:
        self.quant = QuantSignalAgent()
        self.analyst = EquityResearchAnalystAgent()
        self.research = ResearchFileAgent()
        self.portfolio = PortfolioManagerAgent()
        self.risk = RiskManagerAgent()
        self.compliance = ComplianceOfficerAgent()
        self.trader = TraderAgent()
        self.operations = OperationsReportAgent()

    def run(self, context: AgentContext) -> list[AgentResult]:
        context.run_dir.mkdir(parents=True, exist_ok=True)

        results: list[AgentResult] = []
        for filename, result in [
            ("quant_signal.json", self.quant.run(context)),
        ]:
            result.write_json(context.run_dir / filename)
            results.append(result)

        analyst_result = self.analyst.run(context)
        analyst_result.write_json(context.run_dir / "equity_research_analyst.json")
        results.append(analyst_result)

        research_result = self.research.run(context, analyst_result)
        research_result.write_json(context.run_dir / "research_file.json")
        results.append(research_result)

        portfolio_result = self.portfolio.run(context, analyst_result, research_result)
        portfolio_result.write_json(context.run_dir / "portfolio_manager.json")
        results.append(portfolio_result)

        risk_result = self.risk.run(context, analyst_result, research_result)
        risk_result.write_json(context.run_dir / "risk_manager.json")
        results.append(risk_result)

        compliance_result = self.compliance.run(context, analyst_result, research_result)
        compliance_result.write_json(context.run_dir / "compliance_officer.json")
        results.append(compliance_result)

        trader_result = self.trader.run(context, portfolio_result, risk_result, compliance_result)
        trader_result.write_json(context.run_dir / "trader_order_proposal.json")
        results.append(trader_result)

        operations_result = self.operations.run(context, results)
        operations_result.write_json(context.run_dir / "operations_report.json")
        results.append(operations_result)
        return results


def combine_statuses(statuses: list[str]) -> str:
    priority = {"block": 3, "needs_review": 2, "approve": 1, "info": 0}
    if not statuses:
        return "info"
    return max(statuses, key=lambda status: priority.get(status, 0))


def count_by_key(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def result_by_ticker(result: AgentResult | None) -> dict[str, dict[str, Any]]:
    if result is None:
        return {}
    return {
        str(signal.get("ticker", "")): signal
        for signal in result.signals
        if signal.get("ticker")
    }


def adjusted_portfolio_priority(
    base_priority: float,
    analysis_status: str,
    analyst_confidence: str,
    research_quality_status: str,
) -> float:
    priority = float(base_priority)
    if analysis_status == "complete" and analyst_confidence == "high":
        priority = float(base_priority)
    elif analysis_status == "partial" or analyst_confidence in {"medium", "low"}:
        priority = float(base_priority) * 0.7
    elif analysis_status == "missing" or analyst_confidence == "none":
        priority = float(base_priority) * 0.3
    if research_quality_status in {"missing", "incomplete", "stale", "unreadable"}:
        priority = min(priority, float(base_priority) * 0.5)
    return round(priority, 4)


def portfolio_sector_exposure(
    portfolio: list[PortfolioPosition],
    portfolio_value: float,
) -> dict[str, float]:
    if portfolio_value <= 0:
        return {}
    exposure: dict[str, float] = {}
    for position in portfolio:
        sector = position.sector or "unknown"
        exposure[sector] = exposure.get(sector, 0.0) + position.market_value
    return {
        sector: round(value / portfolio_value, 4)
        for sector, value in sorted(exposure.items())
    }


def build_risk_projection(
    candidate: Candidate,
    portfolio: list[PortfolioPosition],
    portfolio_value: float,
    suggested_amount: int,
) -> dict[str, Any]:
    existing_position_value = sum(
        position.market_value for position in portfolio if position.ticker == candidate.ticker
    )
    sector = candidate.sector or next(
        (position.sector for position in portfolio if position.ticker == candidate.ticker),
        "",
    )
    existing_sector_value = (
        sum(position.market_value for position in portfolio if position.sector == sector)
        if sector
        else 0.0
    )
    projected_position_pct = None
    projected_sector_pct = None
    if portfolio_value > 0:
        projected_position_pct = round((existing_position_value + suggested_amount) / portfolio_value, 6)
        if sector:
            projected_sector_pct = round((existing_sector_value + suggested_amount) / portfolio_value, 6)
    liquidity_ratio = None
    if candidate.trade_amount and candidate.trade_amount > 0:
        liquidity_ratio = round(suggested_amount / candidate.trade_amount, 6)
    return {
        "projected_position_pct": projected_position_pct,
        "projected_sector_pct": projected_sector_pct,
        "liquidity_ratio": liquidity_ratio,
        "sector": sector,
        "existing_position_value": existing_position_value,
        "existing_sector_value": existing_sector_value,
    }


def compliance_record_status(
    candidate: Candidate,
    analyst_signal: dict[str, Any],
    research_signal: dict[str, Any],
    matches: list[Path],
) -> str:
    if not candidate.source:
        return "missing_source"
    if not analyst_signal:
        return "missing_analyst"
    if not matches:
        return "missing_research"
    if research_signal.get("analyst_source_file_mismatch") or research_signal.get(
        "analyst_research_missing_item_mismatch"
    ):
        return "mismatch"
    if analyst_signal.get("analysis_status") != "complete":
        return "incomplete_analysis"
    quality_status = str(research_signal.get("quality_status", ""))
    if quality_status and quality_status != "usable":
        return "incomplete_research"
    return "complete"


def manual_source_status_for(candidate: Candidate) -> str:
    if candidate.source_type != "manual":
        return "not_manual"
    return "present" if candidate.source else "missing"


def contains_blocking_information(keywords: list[str]) -> bool:
    blocking = {"루머", "찌라시", "미공개"}
    return any(keyword in blocking for keyword in keywords)


def analyze_candidate_research(candidate: Candidate, matches: list[Path]) -> dict[str, Any]:
    if not matches:
        missing_items = list(ANALYST_SECTION_KEYWORDS)
        return {
            "ticker": candidate.ticker,
            "name": candidate.name,
            "analysis_status": "missing",
            "confidence": "none",
            "business_model_summary": "",
            "earnings_check": "missing",
            "balance_sheet_check": "missing",
            "valuation_check": "missing",
            "catalysts": [],
            "key_risks": ["No research file found."],
            "disconfirmation_conditions": [],
            "missing_items": missing_items,
            "forbidden_keyword_hits": [],
            "source_files": [],
            "section_statuses": {
                section: "missing" for section in ANALYST_SECTION_KEYWORDS
            },
            "readability": "missing",
            "latest_modified": "",
            "source_file_count": 0,
            "analyst_notes": "No matching research file was found for this candidate.",
            "agent_status": "needs_review",
        }

    latest = max(matches, key=lambda path: path.stat().st_mtime)
    documents = [read_text_with_quality(path) for path in matches[:5]]
    readable_documents = [document for document in documents if document["readability"] != "unreadable"]
    text = "\n\n".join(str(document["text"]) for document in readable_documents)[:300_000]
    lowered = text.lower()
    if not readable_documents:
        section_status = {
            section: "unreadable" for section in ANALYST_SECTION_KEYWORDS
        }
    else:
        section_status = {
            section: classify_analyst_section(lowered, keywords)
            for section, keywords in ANALYST_SECTION_KEYWORDS.items()
        }
    missing_items = [
        section
        for section, status in section_status.items()
        if status in {"missing", "unreadable"} or (section in ANALYST_STRONG_SECTIONS and status == "weak")
    ]
    forbidden_hits = [
        keyword for keyword in FORBIDDEN_RESEARCH_KEYWORDS if keyword.lower() in lowered
    ]
    latest_dt = datetime.fromtimestamp(latest.stat().st_mtime)
    age_days = (datetime.now() - latest_dt).days
    readability = combine_readability([str(document["readability"]) for document in documents])
    if readability == "unreadable":
        analysis_status = "partial"
    elif not missing_items and age_days <= ANALYST_STALE_DAYS:
        analysis_status = "complete"
    else:
        analysis_status = "partial"
    if readability == "unreadable" or len(missing_items) >= 5:
        confidence = "low"
    elif missing_items:
        confidence = "medium"
    elif age_days > ANALYST_STALE_DAYS:
        confidence = "medium"
    else:
        confidence = "high"

    catalysts = extract_matching_keywords(
        lowered,
        ANALYST_SECTION_KEYWORDS["catalyst"],
    )
    key_risks = build_analyst_risks(section_status, age_days, readability)
    disconfirmation = (
        ["Disconfirmation condition section exists; verify details manually."]
        if section_status["disconfirmation"] in {"present", "weak"}
        else []
    )
    status = "approve" if analysis_status == "complete" and not forbidden_hits else "needs_review"
    if any(keyword in forbidden_hits for keyword in ("루머", "찌라시", "미공개")):
        status = "block"

    return {
        "ticker": candidate.ticker,
        "name": candidate.name,
        "analysis_status": analysis_status,
        "confidence": confidence,
        "business_model_summary": (
            "Business model section appears in source file; verify manually."
            if section_status["business_model"] == "present"
            else ""
        ),
        "earnings_check": section_status["earnings_quality"],
        "balance_sheet_check": section_status["balance_sheet"],
        "valuation_check": section_status["valuation"],
        "catalysts": catalysts,
        "key_risks": key_risks,
        "disconfirmation_conditions": disconfirmation,
        "missing_items": missing_items,
        "forbidden_keyword_hits": forbidden_hits,
        "source_files": [str(path) for path in matches[:5]],
        "section_statuses": section_status,
        "readability": readability,
        "latest_modified": latest_dt.isoformat(timespec="seconds"),
        "source_file_count": len(matches),
        "analyst_notes": (
            f"Reviewed {len(readable_documents)}/{min(len(matches), 5)} source files; "
            f"latest source: {latest}; age_days={age_days}."
        ),
        "agent_status": status,
    }


def extract_matching_keywords(text: str, keywords: tuple[str, ...]) -> list[str]:
    return [keyword for keyword in keywords if keyword.lower() in text]


def classify_analyst_section(text: str, keywords: tuple[str, ...]) -> str:
    hit_count = sum(1 for keyword in keywords if keyword.lower() in text)
    if hit_count >= 2:
        return "present"
    if hit_count == 1:
        return "weak"
    return "missing"


def build_analyst_risks(section_status: dict[str, str], age_days: int, readability: str) -> list[str]:
    risks: list[str] = []
    if section_status.get("risk") == "missing":
        risks.append("Risk section is missing.")
    elif section_status.get("risk") == "weak":
        risks.append("Risk section is weak and needs manual review.")
    if section_status.get("valuation") in {"missing", "weak"}:
        risks.append("Valuation support is insufficient.")
    if section_status.get("disconfirmation") in {"missing", "weak"}:
        risks.append("Disconfirmation condition is insufficient.")
    if age_days > ANALYST_STALE_DAYS:
        risks.append(f"Latest research file is stale ({age_days} days old).")
    if readability != "ok":
        risks.append(f"Research readability is {readability}.")
    return risks or ["Risk section exists; verify details manually."]


def combine_readability(values: list[str]) -> str:
    if not values:
        return "missing"
    if all(value == "unreadable" for value in values):
        return "unreadable"
    if any(value in {"decode_loss", "unreadable"} for value in values):
        return "partial"
    return "ok"


def research_quality_status(matches: list[Path], quality: dict[str, Any]) -> str:
    if not matches:
        return "missing"
    if quality.get("readability") == "unreadable":
        return "unreadable"
    if quality.get("is_stale"):
        return "stale"
    if quality.get("missing_required_sections"):
        return "incomplete"
    return "usable"


def find_research_files(research_root: Path, candidate: Candidate) -> list[Path]:
    if not research_root.exists():
        return []
    needles = [candidate.ticker]
    if candidate.name:
        needles.append(candidate.name)
    matches: list[Path] = []
    for path in research_root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in {".md", ".txt", ".csv"}:
            continue
        haystack = " ".join(part for part in path.parts[-4:])
        if any(needle and needle in haystack for needle in needles):
            matches.append(path)
    return sorted(matches)[:20]


def analyze_research_files(paths: list[Path], run_date: date) -> dict[str, Any]:
    if not paths:
        return {
            "latest_file": "",
            "latest_modified": "",
            "missing_required_sections": list(REQUIRED_RESEARCH_SECTIONS),
            "forbidden_keyword_hits": [],
            "is_stale": False,
            "readability": "missing",
            "latest_file_age_days": None,
        }
    latest = max(paths, key=lambda path: path.stat().st_mtime)
    latest_dt = datetime.fromtimestamp(latest.stat().st_mtime)
    document = read_text_with_quality(latest)
    text = str(document["text"])[:200_000].lower()
    missing = [
        section
        for section, keywords in REQUIRED_RESEARCH_SECTIONS.items()
        if document["readability"] == "unreadable"
        or not any(keyword.lower() in text for keyword in keywords)
    ]
    hits = [keyword for keyword in FORBIDDEN_RESEARCH_KEYWORDS if keyword.lower() in text]
    age_days = max(0, (datetime.combine(run_date, datetime.min.time()) - latest_dt).days)
    return {
        "latest_file": str(latest),
        "latest_modified": latest_dt.isoformat(timespec="seconds"),
        "missing_required_sections": missing,
        "forbidden_keyword_hits": hits,
        "is_stale": age_days > RESEARCH_STALE_DAYS,
        "readability": str(document["readability"]),
        "latest_file_age_days": age_days,
    }


def read_text_with_quality(path: Path) -> dict[str, str]:
    for encoding in ("utf-8", "utf-8-sig", "cp949"):
        try:
            return {
                "text": path.read_text(encoding=encoding),
                "encoding": encoding,
                "readability": "ok",
            }
        except UnicodeDecodeError:
            continue
        except OSError:
            return {"text": "", "encoding": "", "readability": "unreadable"}
    try:
        return {
            "text": path.read_text(encoding="utf-8", errors="ignore"),
            "encoding": "utf-8",
            "readability": "decode_loss",
        }
    except OSError:
        return {"text": "", "encoding": "", "readability": "unreadable"}


def discover_candidates_from_csv(root: Path, limit: int) -> list[Candidate]:
    if not root.exists():
        return []
    candidates: dict[str, dict[str, Any]] = {}
    csv_paths = sorted(root.rglob("*.csv"), key=lambda path: path.stat().st_mtime, reverse=True)
    for path in csv_paths[:50]:
        if path.stat().st_size == 0:
            continue
        for row in read_csv_rows(path, max_rows=500):
            ticker = extract_ticker(row)
            if not ticker:
                continue
            detail = build_signal_detail(row, path)
            if ticker not in candidates:
                candidates[ticker] = {
                    "ticker": ticker,
                    "name": extract_name(row, ticker),
                    "source": str(path),
                    "source_type": classify_source_type(path),
                    "score": extract_score(row),
                    "current_price": extract_float(row, ("event_close", "close", "price")),
                    "trade_amount": extract_float(row, ("trade_amount", "avg_trade_amount")),
                    "signal_details": [],
                }
            candidates[ticker]["signal_details"].append(detail)
            if candidates[ticker].get("score") is None:
                candidates[ticker]["score"] = extract_score(row)
            if not candidates[ticker].get("trade_amount"):
                candidates[ticker]["trade_amount"] = extract_float(row, ("trade_amount", "avg_trade_amount"))
            if len(candidates) >= limit:
                return materialize_candidates(candidates)
    return materialize_candidates(candidates)


def read_csv_rows(path: Path, max_rows: int) -> list[dict[str, str]]:
    for encoding in ("utf-8-sig", "cp949", "utf-8"):
        try:
            with path.open("r", encoding=encoding, newline="") as file:
                reader = csv.DictReader(file)
                return [row for _, row in zip(range(max_rows), reader)]
        except (UnicodeDecodeError, csv.Error):
            continue
        except OSError:
            return []
    return []


def materialize_candidates(items: dict[str, dict[str, Any]]) -> list[Candidate]:
    candidates: list[Candidate] = []
    for item in items.values():
        details = item["signal_details"]
        candidates.append(Candidate(
            ticker=item["ticker"],
            name=item["name"],
            source=item["source"],
            source_type=item["source_type"],
            score=item["score"],
            signal_count=len(details),
            signal_details=details[:20],
            current_price=item.get("current_price"),
            trade_amount=item.get("trade_amount"),
        ))
    return sorted(candidates, key=lambda candidate: (candidate.score or 0, candidate.signal_count), reverse=True)


def extract_ticker(row: dict[str, str]) -> str:
    for key in ("ticker", "종목코드", "code"):
        value = str(row.get(key, "")).strip()
        if TICKER_RE.fullmatch(value):
            return value
    text_values = [str(value).strip() for value in row.values() if value is not None]
    return next((value for value in text_values if TICKER_RE.fullmatch(value)), "")


def extract_name(row: dict[str, str], ticker: str) -> str:
    for key in ("name", "종목명", "company_name"):
        value = str(row.get(key, "")).strip()
        if value:
            return value
    text_values = [str(value).strip() for value in row.values() if value is not None]
    return next((value for value in text_values if value and value != ticker), "")


def extract_score(row: dict[str, str]) -> float | None:
    return extract_float(row, ("score", "confidence", "점수", "신뢰도", "change_rate", "chg_pct"))


def extract_float(row: dict[str, str], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        raw = str(row.get(key, "")).strip().replace(",", "")
        if not raw:
            continue
        try:
            return float(raw)
        except ValueError:
            continue
    return None


def classify_source_type(path: Path) -> str:
    text = str(path)
    if "관심종목" in text:
        return "watchlist"
    if "외국인" in text:
        return "foreign_flow"
    if "신규조건" in text:
        return "new_condition"
    if "캔들" in text:
        return "candlestick"
    if "scoring" in text.lower() or "점수" in text:
        return "scoring"
    return "csv_signal"


def build_signal_detail(row: dict[str, str], path: Path) -> dict[str, Any]:
    return {
        "source_file": str(path),
        "source_type": classify_source_type(path),
        "signal_date": row.get("signal_date") or row.get("basis_date") or "",
        "condition": row.get("condition_id") or row.get("pattern_id") or row.get("matched_conditions") or "",
        "score": extract_score(row),
    }


def load_portfolio(path: Path | None) -> tuple[list[PortfolioPosition], float]:
    if path is None or not path.exists():
        return [], 0.0
    data = json.loads(path.read_text(encoding="utf-8"))
    cash = float(data.get("cash", 0))
    positions = [
        PortfolioPosition(
            ticker=str(item.get("ticker", "")),
            name=str(item.get("name", "")),
            quantity=int(item.get("quantity", 0)),
            avg_price=float(item.get("avg_price", 0)),
            current_price=float(item.get("current_price", 0)),
            sector=str(item.get("sector", "")),
        )
        for item in data.get("positions", [])
    ]
    return positions, cash


def render_markdown_report(context: AgentContext, results: list[AgentResult]) -> str:
    status_counts: dict[str, int] = {}
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
    lines = [
        "# Daily Investment Committee Report",
        "",
        f"- Run date: {context.run_date.isoformat()}",
        f"- Candidate count: {len(context.candidates)}",
        f"- Portfolio value: {context.portfolio_value:,.0f}",
        f"- Status counts: block={status_counts.get('block', 0)}, needs_review={status_counts.get('needs_review', 0)}, approve={status_counts.get('approve', 0)}, info={status_counts.get('info', 0)}",
        f"- Trader JSON: {context.run_dir / 'trader_order_proposal.json'}",
        "",
        "## Agent Status",
        "",
    ]
    for result in results:
        lines.append(f"- {result.agent}: {result.status} - {result.summary}")

    analyst = next((result for result in results if result.agent == "EquityResearchAnalystAgent"), None)
    if analyst:
        lines.extend([
            "",
            "## Equity Research Review",
            "",
            "| Ticker | Name | Analysis | Confidence | Missing Items |",
            "|---|---|---|---|---|",
        ])
        for signal in analyst.signals:
            lines.append(
                "| {ticker} | {name} | {analysis} | {confidence} | {missing} |".format(
                    ticker=signal.get("ticker", ""),
                    name=signal.get("name", ""),
                    analysis=signal.get("analysis_status", ""),
                    confidence=signal.get("confidence", ""),
                    missing=", ".join(signal.get("missing_items", [])),
                )
            )

    trader = next((result for result in results if result.agent == "TraderAgent"), None)
    if trader:
        lines.extend([
            "",
            "## Candidate Table",
            "",
            "| Ticker | Name | Side | Amount | Risk | Compliance |",
            "|---|---|---:|---:|---|---|",
        ])
        for proposal in trader.signals:
            lines.append(
                "| {ticker} | {name} | {side} | {amount:,} | {risk} | {compliance} |".format(
                    ticker=proposal["ticker"],
                    name=proposal["name"],
                    side=proposal["side"],
                    amount=int(proposal["suggested_amount"]),
                    risk=proposal["risk_status"],
                    compliance=proposal["compliance_status"],
                )
            )
        lines.extend(["", "## Draft Order Proposals", ""])
        for proposal in trader.signals:
            lines.append(
                "- {ticker} {name}: {side}, amount={amount:,}, quantity={quantity}, risk={risk}, compliance={compliance}".format(
                    ticker=proposal["ticker"],
                    name=proposal["name"],
                    side=proposal["side"],
                    amount=int(proposal["suggested_amount"]),
                    quantity=int(proposal["suggested_quantity"]),
                    risk=proposal["risk_status"],
                    compliance=proposal["compliance_status"],
                )
            )

    warnings = [
        f"{result.agent}: {warning}"
        for result in results
        for warning in result.warnings
    ]
    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)

    checks = [
        f"{result.agent}: {check}"
        for result in results
        for check in result.required_human_checks
    ]
    if checks:
        lines.extend(["", "## Required Human Checks", ""])
        lines.extend(f"- {check}" for check in checks)

    lines.extend([
        "",
        "## Notes",
        "",
        "- No broker API call was made.",
        "- Actual order execution is prohibited by this pipeline.",
        "- This report is a review aid, not an investment instruction.",
        "",
    ])
    return "\n".join(lines)


def render_short_summary(context: AgentContext, results: list[AgentResult]) -> str:
    final_status = combine_statuses([result.status for result in results])
    trader = next((result for result in results if result.agent == "TraderAgent"), None)
    buy_count = 0
    hold_count = 0
    if trader:
        buy_count = sum(1 for proposal in trader.signals if proposal.get("side") == "buy")
        hold_count = sum(1 for proposal in trader.signals if proposal.get("side") == "hold")
    return "\n".join([
        f"{context.run_date.isoformat()} investment committee: {final_status}",
        f"candidates={len(context.candidates)}, draft_buys={buy_count}, holds={hold_count}",
        "No broker API call was made. Manual review is required.",
        str(context.run_dir / "final_committee_report.md"),
        "",
    ])
