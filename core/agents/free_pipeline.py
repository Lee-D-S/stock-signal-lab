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
REQUIRED_PIPELINE_AGENTS = [
    "QuantSignalAgent",
    "EquityResearchAnalystAgent",
    "ResearchFileAgent",
    "PortfolioManagerAgent",
    "RiskManagerAgent",
    "ComplianceOfficerAgent",
    "TraderAgent",
]


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
                        f"{candidate.ticker}: 원천 신호가 오래됐습니다 ({age_days:.1f}일)."
                    )
            elif candidate.source_type != "manual":
                warnings.append(f"{candidate.ticker}: 원천 파일이 없습니다: {candidate.source}")
            if not candidate.ticker or not TICKER_RE.fullmatch(candidate.ticker):
                warnings.append(f"{candidate.ticker or '<missing>'}: 종목코드가 6자리 형식이 아닙니다.")
            if not candidate.name:
                warnings.append(f"{candidate.ticker}: 종목명이 없습니다.")
            if candidate.current_price is None:
                warnings.append(f"{candidate.ticker}: 현재가가 없습니다.")
            if candidate.trade_amount is None:
                warnings.append(f"{candidate.ticker}: 거래대금이 없습니다.")
            if candidate.signal_count <= 0:
                warnings.append(f"{candidate.ticker}: 신호 개수가 없습니다.")
            if not candidate.signal_details:
                warnings.append(f"{candidate.ticker}: 신호 상세가 비어 있습니다.")
            signals.append(signal)
        if not signals:
            warnings.append("후보가 제공되거나 발견되지 않았습니다.")

        return AgentResult(
            agent=self.name,
            status="info" if signals else "needs_review",
            summary=f"{len(signals)}개 후보를 규칙 기반 검토용으로 정리했습니다.",
            signals=signals,
            warnings=warnings,
            required_human_checks=[
                "수동 주문 전에 원천 신호가 아직 유효한지 확인하세요.",
                "가격이나 유동성 데이터가 없는 후보는 Risk와 Trader 단계 전에 검토하세요.",
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
                    f"{candidate.ticker}: 애널리스트 검토 상태가 {analysis['analysis_status']} 입니다 "
                    f"({', '.join(analysis['missing_items']) or '누락 항목 상세 없음'})."
                )
            if analysis["forbidden_keyword_hits"]:
                warnings.append(
                    f"{candidate.ticker}: 애널리스트 원천에 주의 키워드가 있습니다 - "
                    + ", ".join(analysis["forbidden_keyword_hits"])
                )
            if analysis.get("readability") not in {"ok", "missing"}:
                warnings.append(
                    f"{candidate.ticker}: 애널리스트 원천 가독성 상태는 {analysis['readability']} 입니다."
                )
            signals.append(analysis)

        if not context.candidates:
            warnings.append("애널리스트 검토할 후보가 없습니다.")

        return AgentResult(
            agent=self.name,
            status=combine_statuses(statuses),
            summary=f"{len(signals)}개 후보의 리서치 검토를 준비했습니다.",
            signals=signals,
            warnings=warnings,
            required_human_checks=[
                "후보를 투자 가능 대상으로 보기 전에 사업, 실적, 밸류에이션, 리스크, 반증 조건 누락 항목을 채우세요.",
                "오래됐거나 부분적으로만 읽히는 원천 파일은 Portfolio와 Compliance가 신뢰하기 전에 다시 확인하세요.",
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
            warnings.append(f"리서치 루트가 없습니다: {context.research_root}")
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
                warnings.append(f"{candidate.ticker}: 리서치 파일을 찾지 못했습니다.")
            if quality["missing_required_sections"]:
                warnings.append(
                    f"{candidate.ticker}: 리서치 누락 섹션이 있습니다 - "
                    + ", ".join(quality["missing_required_sections"])
                )
            if quality["readability"] != "ok":
                warnings.append(f"{candidate.ticker}: 리서치 파일 가독성 상태는 {quality['readability']} 입니다.")
            if quality["is_stale"]:
                warnings.append(f"{candidate.ticker}: 리서치 파일이 오래됐습니다.")
            if quality["forbidden_keyword_hits"]:
                warnings.append(
                    f"{candidate.ticker}: 리서치 파일에 주의 키워드가 있습니다 - "
                    + ", ".join(quality["forbidden_keyword_hits"])
                )
            if analyst_source_file_mismatch:
                warnings.append(f"{candidate.ticker}: 애널리스트 원천 파일과 리서치 파일이 일치하지 않습니다.")
            if analyst_complete_research_incomplete:
                warnings.append(f"{candidate.ticker}: 애널리스트는 complete로 표시했지만 리서치 파일은 불완전합니다.")
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
            warnings.append("리서치 파일 검토할 후보가 없습니다.")

        return AgentResult(
            agent=self.name,
            status=status,
            summary="리서치 파일 존재 및 품질 검사를 완료했습니다.",
            signals=signals,
            warnings=warnings,
            required_human_checks=[
                "매칭된 리서치 파일을 읽고 투자 가설을 수동으로 보강하세요.",
                "쓸 수 있는 보고서가 없다면 scripts/run_new_company_reports.py --include-existing-missing 를 실행하세요.",
                "Compliance가 증거 체인을 완전하다고 보기 전에 Analyst와 ResearchFile 불일치를 해결하세요.",
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
            warnings.append("포트폴리오 스냅샷이 없거나 가치가 0입니다.")
        if not context.candidates:
            warnings.append("포트폴리오 검토할 후보가 없습니다.")
        for position in context.portfolio:
            if position.quantity < 0:
                warnings.append(f"{position.ticker}: portfolio position quantity is negative.")
            if position.quantity > 0 and position.market_value <= 0:
                warnings.append(f"{position.ticker}: 포트폴리오 보유 종목의 평가금액이 0입니다.")
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
                reason = "포트폴리오 스냅샷이 없어 배분 결정을 검토해야 합니다."
                action_detail = "needs_portfolio_snapshot"
                portfolio_constraints.append("missing_portfolio_snapshot")
                warnings.append(f"{candidate.ticker}: 포트폴리오 스냅샷이 없습니다.")
            elif amount > context.cash:
                reason = "새 매수 초안을 만들 현금이 부족합니다."
                action_detail = "cash_limited"
                portfolio_constraints.append("cash_limited")
                warnings.append(f"{candidate.ticker}: 현금이 부족합니다.")
            elif analysis_status != "complete":
                reason = "기업 리서치가 불완전하여 배분 결정을 다시 검토해야 합니다."
                action_detail = "needs_equity_research"
                portfolio_constraints.append("incomplete_equity_research")
                warnings.append(f"{candidate.ticker}: 기업 리서치 상태가 {analysis_status} 입니다.")
            elif research_quality_status != "usable" or research_is_stale or research_source_mismatch:
                reason = "새 매수 초안을 만들기엔 리서치 파일 품질이 부족합니다."
                action_detail = "needs_research_file_quality"
                portfolio_constraints.append(f"research_quality_{research_quality_status}")
                if research_is_stale:
                    portfolio_constraints.append("stale_research")
                if research_source_mismatch:
                    portfolio_constraints.append("analyst_research_source_mismatch")
                warnings.append(f"{candidate.ticker}: 리서치 파일 품질 상태가 {research_quality_status} 입니다.")
            else:
                side = "buy"
                reason = "현금이 있어 신규 후보를 초안으로 제시합니다. Risk와 Compliance 검토가 필요합니다."
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
            summary=f"{len(signals)}개 포트폴리오 조치를 초안으로 작성했습니다.",
            signals=signals,
            warnings=warnings,
            required_human_checks=[
                "각 조치를 현재 현금, 배분 정책, 투자 가설 품질과 비교하세요."
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
            all_warnings.append("리스크 검토할 후보가 없습니다.")
        if context.portfolio_value <= 0:
            all_warnings.append("포트폴리오 스냅샷이 없거나 가치가 0입니다.")
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
                warnings.append("애널리스트 핵심 리스크는 수동 리스크 검토가 필요합니다.")
                status = combine_statuses([status, "needs_review"])
            if research_signal.get("quality_status") in {"missing", "incomplete", "stale", "unreadable"}:
                warnings.append("리서치 품질이 약해 Risk 입력이 불완전할 수 있습니다.")
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
            summary="리스크 가드레일 검사를 완료했습니다.",
            signals=signals,
            warnings=all_warnings,
            required_human_checks=[
                "차단된 후보는 주문하지 마세요.",
                "주문 전에 유동성과 이벤트 리스크를 수동으로 확인하세요.",
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
            all_warnings.append("준법 검토할 후보가 없습니다.")
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
                    warnings.append("기업 애널리스트 검토가 불완전합니다.")
                    status = combine_statuses([status, "needs_review"])
                if analyst_signal.get("confidence") in {"low", "none"}:
                    warnings.append("기업 애널리스트 신뢰도가 낮습니다.")
                    status = combine_statuses([status, "needs_review"])
                analyst_forbidden_hits = analyst_signal.get("forbidden_keyword_hits", [])
                if analyst_forbidden_hits:
                    warnings.append("기업 애널리스트가 주의 키워드를 발견했습니다.")
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
                    warnings.append("애널리스트는 complete로 표시했지만 리서치 파일 검토는 사용할 수 없습니다.")
                    status = combine_statuses([status, "needs_review"])
            else:
                warnings.append("기업 애널리스트 결과가 없습니다.")
                status = combine_statuses([status, "needs_review"])
            if record_status != "complete":
                warnings.append(f"기록 상태가 {record_status} 입니다.")
                status = combine_statuses([status, "needs_review"])
            if manual_source_status == "missing":
                warnings.append("수동 후보 원천이 없습니다.")
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
            summary="준법 체크리스트를 완료했습니다.",
            signals=signals,
            warnings=all_warnings,
            required_human_checks=[
                "미공개 정보, 루머만 있는 가설, 누락된 기록이 없는지 확인하세요.",
                "후보를 준법 승인으로 보기 전에 Analyst와 ResearchFile 증거 불일치를 해결하세요.",
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
        risk_by_ticker = {signal["ticker"]: signal for signal in risk_result.signals}
        compliance_by_ticker = {signal["ticker"]: signal for signal in compliance_result.signals}

        proposals: list[OrderProposal] = []
        warnings: list[str] = []
        statuses: list[str] = []
        if not context.candidates:
            statuses.append("info")
        for candidate in context.candidates:
            risk_signal = risk_by_ticker.get(candidate.ticker, {})
            compliance_signal = compliance_by_ticker.get(candidate.ticker, {})
            risk_status = risk_signal.get("status", "needs_review")
            compliance_status = compliance_signal.get("status", "needs_review")
            portfolio_signal = portfolio_by_ticker.get(candidate.ticker, {})
            portfolio_side = portfolio_signal.get("side", "hold")
            side = portfolio_side if portfolio_side in {"buy", "sell", "hold"} else "hold"
            final_side_reason = "portfolio side retained after Risk and Compliance approval."
            if risk_status != "approve" or compliance_status != "approve":
                side = "hold"
                final_side_reason = build_final_side_reason(risk_status, compliance_status, portfolio_side)
            elif portfolio_side not in {"buy", "sell", "hold"}:
                final_side_reason = f"invalid portfolio side {portfolio_side}; defaulted to hold."
            amount = int(candidate.suggested_amount or default_amount)
            price = candidate.current_price or portfolio_signal.get("current_price")
            quantity = int(amount // price) if price and price > 0 and side != "hold" else 0
            if side != "hold" and quantity <= 0:
                final_side_reason = "price is missing or invalid; quantity could not be calculated."
            risk_warnings = list(risk_signal.get("warnings", []))
            compliance_warnings = list(compliance_signal.get("warnings", []))
            proposal = OrderProposal(
                ticker=candidate.ticker,
                name=candidate.name or candidate.ticker,
                side=side,
                suggested_amount=0 if side == "hold" else amount,
                suggested_quantity=quantity,
                order_type_hint="manual_review_limit_order",
                reason=portfolio_signal.get(
                    "reason",
                    "규칙 기반 초안일 뿐이며 브로커 API 호출은 수행하지 않았습니다.",
                ),
                risk_status=risk_status,
                compliance_status=compliance_status,
                human_checklist=[
                    "현재가를 확인하고 수량을 수동 계산하세요.",
                    "주문 입력 전에 종목코드, 방향, 금액, 주문 유형을 다시 확인하세요.",
                    "Risk 또는 Compliance 상태가 block 또는 needs_review이면 건너뛰세요.",
                ],
                portfolio_side=portfolio_side if portfolio_side in {"buy", "sell", "hold"} else "hold",
                final_side_reason=final_side_reason,
                price_used=float(price) if price else None,
                price_source="candidate.current_price" if candidate.current_price else (
                    "portfolio_signal.current_price" if portfolio_signal.get("current_price") else ""
                ),
                risk_warnings=risk_warnings,
                compliance_warnings=compliance_warnings,
                execution_allowed=False,
            )
            status, proposal_warnings = check_order_proposal_rules(proposal)
            if risk_status != "approve" or compliance_status != "approve":
                status = "needs_review" if status != "block" else status
                proposal_warnings.append("Risk 또는 Compliance가 승인되지 않아 주문 초안은 hold입니다.")
            if portfolio_side == "hold":
                status = "needs_review" if status != "block" else status
            if side != "hold" and quantity <= 0:
                status = "needs_review" if status != "block" else status
                proposal_warnings.append("현재가가 없거나 유효하지 않아 수량을 계산할 수 없습니다.")
            statuses.append(status)
            warnings.extend(f"{candidate.ticker}: {warning}" for warning in proposal_warnings)
            proposals.append(proposal)

        return AgentResult(
            agent=self.name,
            status=combine_statuses(statuses),
            summary=f"{len(proposals)}개의 주문 초안을 실행 없이 생성했습니다.",
            signals=[proposal.to_dict() for proposal in proposals],
            warnings=warnings,
            required_human_checks=[
                "이 파이프라인은 절대 주문을 실행하지 않습니다. 주문 입력은 별도의 수동 작업입니다."
            ],
            artifacts={
                "broker_api_called": False,
                "execution_allowed": False,
                "side_counts": count_by_key([proposal.to_dict() for proposal in proposals], "side"),
                "blocked_by_risk": sum(1 for proposal in proposals if proposal.risk_status != "approve"),
                "blocked_by_compliance": sum(
                    1 for proposal in proposals if proposal.compliance_status != "approve"
                ),
            },
        )


class OperationsReportAgent:
    name = "OperationsReportAgent"

    def run(self, context: AgentContext, results: list[AgentResult]) -> AgentResult:
        report_path = context.run_dir / "final_committee_report.md"
        summary_path = context.run_dir / "telegram_summary.txt"
        metadata = build_operations_metadata(context, results)
        report_path.write_text(render_markdown_report(context, results, metadata), encoding="utf-8")
        summary_path.write_text(render_short_summary(context, results, metadata), encoding="utf-8")
        warnings = []
        if metadata["missing_agents"]:
            warnings.append("누락된 필수 에이전트: " + ", ".join(metadata["missing_agents"]))
        if metadata["report_integrity_status"] != "ok":
            warnings.append("보고서 무결성 상태가 " + metadata["report_integrity_status"] + " 입니다.")
        return AgentResult(
            agent=self.name,
            status=combine_statuses([result.status for result in results] + (["needs_review"] if warnings else [])),
            summary=f"최종 보고서를 {report_path}에 작성했습니다.",
            warnings=warnings,
            required_human_checks=[
                "Markdown 보고서와 Trader JSON의 후보, 방향, 보유 사유가 같은지 확인하세요."
            ],
            artifacts={
                "report_path": str(report_path),
                "telegram_summary_path": str(summary_path),
                **metadata,
            },
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
        quant_result = self._run_step(context, "quant_signal.json", self.quant.run, context)
        results.append(quant_result)

        analyst_result = self._run_step(
            context,
            "equity_research_analyst.json",
            self.analyst.run,
            context,
        )
        results.append(analyst_result)

        research_result = self._run_step(
            context,
            "research_file.json",
            self.research.run,
            context,
            analyst_result,
        )
        results.append(research_result)

        portfolio_result = self._run_step(
            context,
            "portfolio_manager.json",
            self.portfolio.run,
            context,
            analyst_result,
            research_result,
        )
        results.append(portfolio_result)

        risk_result = self._run_step(
            context,
            "risk_manager.json",
            self.risk.run,
            context,
            analyst_result,
            research_result,
        )
        results.append(risk_result)

        compliance_result = self._run_step(
            context,
            "compliance_officer.json",
            self.compliance.run,
            context,
            analyst_result,
            research_result,
        )
        results.append(compliance_result)

        trader_result = self._run_step(
            context,
            "trader_order_proposal.json",
            self.trader.run,
            context,
            portfolio_result,
            risk_result,
            compliance_result,
        )
        results.append(trader_result)

        operations_result = self._run_step(
            context,
            "operations_report.json",
            self.operations.run,
            context,
            results,
        )
        results.append(operations_result)
        try:
            write_pipeline_manifest(context, results)
        except Exception as exc:  # noqa: BLE001
            operations_result.warnings.append(
                f"failed to write pipeline_manifest.json: {type(exc).__name__}: {exc}"
            )
            operations_result.status = combine_statuses([operations_result.status, "needs_review"])
            operations_result.write_json(context.run_dir / "operations_report.json")
        return results

    def _run_step(self, context: AgentContext, filename: str, func: Any, *args: Any) -> AgentResult:
        try:
            result = func(*args)
        except Exception as exc:  # noqa: BLE001
            agent_name = infer_agent_name(func)
            result = AgentResult(
                agent=agent_name,
                status="block",
                summary=f"{agent_name} 실패가 발생해 실패 결과를 기록했습니다.",
                warnings=[f"{type(exc).__name__}: {exc}"],
                required_human_checks=[
                    f"이 실행을 투자 판단에 쓰기 전에 {agent_name} 실패 원인을 확인하세요."
                ],
                artifacts={"failed_step_file": filename},
            )
        try:
            result.write_json(context.run_dir / filename)
        except Exception as exc:  # noqa: BLE001
            result.warnings.append(f"failed to write {filename}: {type(exc).__name__}: {exc}")
            result.status = combine_statuses([result.status, "block"])
        return result


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


def build_final_side_reason(risk_status: str, compliance_status: str, portfolio_side: str) -> str:
    reasons: list[str] = []
    if portfolio_side == "hold":
        reasons.append("PortfolioManager가 hold를 제안했습니다")
    if risk_status != "approve":
        reasons.append(f"RiskManager 상태는 {risk_status} 입니다")
    if compliance_status != "approve":
        reasons.append(f"ComplianceOfficer 상태는 {compliance_status} 입니다")
    return "; ".join(reasons) or "규칙 기반 게이트에서 hold 처리했습니다"


def build_operations_metadata(context: AgentContext, results: list[AgentResult]) -> dict[str, Any]:
    result_by_agent = {result.agent: result for result in results}
    missing_agents = [
        agent for agent in REQUIRED_PIPELINE_AGENTS if agent not in result_by_agent
    ]
    status_counts = count_by_key(
        [{"status": result.status} for result in results],
        "status",
    )
    trader = result_by_agent.get("TraderAgent")
    trader_signals = trader.signals if trader else []
    buy_count = sum(1 for signal in trader_signals if signal.get("side") == "buy")
    hold_count = sum(1 for signal in trader_signals if signal.get("side") == "hold")
    candidate_counts = {
        "context_candidates": len(context.candidates),
        "trader_proposals": len(trader_signals),
        "draft_buys": buy_count,
        "holds": hold_count,
    }
    blocked_by_risk = sum(
        1 for signal in trader_signals if signal.get("risk_status") != "approve"
    )
    blocked_by_compliance = sum(
        1 for signal in trader_signals if signal.get("compliance_status") != "approve"
    )
    report_integrity_status = "ok"
    if missing_agents or len(trader_signals) != len(context.candidates):
        report_integrity_status = "mismatch"
    return {
        "missing_agents": missing_agents,
        "status_counts": status_counts,
        "candidate_counts": candidate_counts,
        "blocked_by_risk": blocked_by_risk,
        "blocked_by_compliance": blocked_by_compliance,
        "report_integrity_status": report_integrity_status,
    }


def infer_agent_name(func: Any) -> str:
    owner = getattr(func, "__self__", None)
    return str(getattr(owner, "name", getattr(func, "__name__", "UnknownAgent")))


def write_pipeline_manifest(context: AgentContext, results: list[AgentResult]) -> None:
    trader = next((result for result in results if result.agent == "TraderAgent"), None)
    broker_api_called = bool(
        trader and trader.artifacts.get("broker_api_called", False)
    )
    manifest = {
        "run_date": context.run_date.isoformat(),
        "run_id": context.run_id,
        "run_dir": str(context.run_dir),
        "agent_order": [result.agent for result in results],
        "final_status": combine_statuses([result.status for result in results]),
        "candidate_count": len(context.candidates),
        "broker_api_called": broker_api_called,
        "actual_order_execution_prohibited": True,
        "artifacts": {
            "quant_signal": str(context.run_dir / "quant_signal.json"),
            "equity_research_analyst": str(context.run_dir / "equity_research_analyst.json"),
            "research_file": str(context.run_dir / "research_file.json"),
            "portfolio_manager": str(context.run_dir / "portfolio_manager.json"),
            "risk_manager": str(context.run_dir / "risk_manager.json"),
            "compliance_officer": str(context.run_dir / "compliance_officer.json"),
            "trader_order_proposal": str(context.run_dir / "trader_order_proposal.json"),
            "operations_report": str(context.run_dir / "operations_report.json"),
            "final_committee_report": str(context.run_dir / "final_committee_report.md"),
            "telegram_summary": str(context.run_dir / "telegram_summary.txt"),
        },
    }
    (context.run_dir / "pipeline_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


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
        ["반증 조건 섹션이 있어 세부 내용은 수동으로 확인하세요."]
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
        risks.append("리스크 섹션이 없습니다.")
    elif section_status.get("risk") == "weak":
        risks.append("리스크 섹션이 약해서 수동 검토가 필요합니다.")
    if section_status.get("valuation") in {"missing", "weak"}:
        risks.append("밸류에이션 근거가 충분하지 않습니다.")
    if section_status.get("disconfirmation") in {"missing", "weak"}:
        risks.append("반증 조건이 충분하지 않습니다.")
    if age_days > ANALYST_STALE_DAYS:
        risks.append(f"최신 리서치 파일이 오래됐습니다 ({age_days}일).")
    if readability != "ok":
        risks.append(f"리서치 가독성 상태는 {readability} 입니다.")
    return risks or ["리스크 섹션이 있어 세부 내용은 수동으로 확인하세요."]


def build_analyst_risks(section_status: dict[str, str], age_days: int, readability: str) -> list[str]:
    risks: list[str] = []
    if section_status.get("risk") == "missing":
        risks.append("리스크 섹션이 없습니다.")
    elif section_status.get("risk") == "weak":
        risks.append("리스크 섹션이 약해서 수동 검토가 필요합니다.")
    if section_status.get("valuation") in {"missing", "weak"}:
        risks.append("밸류에이션 근거가 충분하지 않습니다.")
    if section_status.get("disconfirmation") in {"missing", "weak"}:
        risks.append("반증 조건이 충분하지 않습니다.")
    if age_days > ANALYST_STALE_DAYS:
        risks.append(f"최신 리서치 파일이 오래됐습니다 ({age_days}일).")
    if readability != "ok":
        risks.append(f"리서치 가독성 상태는 {readability} 입니다.")
    return risks or ["리스크 섹션이 있어 세부 내용은 수동으로 확인하세요."]


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


def render_markdown_report(
    context: AgentContext,
    results: list[AgentResult],
    metadata: dict[str, Any],
) -> str:
    status_counts = metadata["status_counts"]
    candidate_counts = metadata["candidate_counts"]
    lines = [
        "# 일일 투자위원회 보고서",
        "",
        "- 브로커 API 호출은 수행하지 않았습니다.",
        "- 이 파이프라인에서는 실제 주문 실행이 금지됩니다.",
        "- 이 보고서는 검토 보조 자료이며 투자 지시가 아닙니다.",
        "",
        f"- 실행 기준일: {context.run_date.isoformat()}",
        f"- 후보 수: {len(context.candidates)}",
        f"- 포트폴리오 가치: {context.portfolio_value:,.0f}",
        f"- 상태 집계: block={status_counts.get('block', 0)}, needs_review={status_counts.get('needs_review', 0)}, approve={status_counts.get('approve', 0)}, info={status_counts.get('info', 0)}",
        f"- 매수 초안: {candidate_counts['draft_buys']}, 보유/대기: {candidate_counts['holds']}",
        f"- Risk 차단: {metadata['blocked_by_risk']}, Compliance 차단: {metadata['blocked_by_compliance']}",
        f"- 보고서 무결성: {metadata['report_integrity_status']}",
        f"- Trader JSON: {context.run_dir / 'trader_order_proposal.json'}",
        "",
        "## 에이전트 상태",
        "",
    ]
    for result in results:
        lines.append(f"- {result.agent}: {result.status} - {result.summary}")
    if metadata["missing_agents"]:
        lines.extend(["", "누락된 필수 에이전트: " + ", ".join(metadata["missing_agents"])])

    analyst = next((result for result in results if result.agent == "EquityResearchAnalystAgent"), None)
    if analyst:
        lines.extend([
            "",
            "## 기업 리서치 검토",
            "",
            "| 종목코드 | 종목명 | 분석 | 신뢰도 | 누락 항목 |",
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
            "## 후보 표",
            "",
            "| 종목코드 | 종목명 | 방향 | 금액 | 수량 | Risk | Compliance | 주요 사유 |",
            "|---|---|---:|---:|---:|---|---|---|",
        ])
        for proposal in trader.signals:
            lines.append(
                "| {ticker} | {name} | {side} | {amount:,} | {quantity:,} | {risk} | {compliance} | {reason} |".format(
                    ticker=proposal["ticker"],
                    name=proposal["name"],
                    side=proposal["side"],
                    amount=int(proposal["suggested_amount"]),
                    quantity=int(proposal["suggested_quantity"]),
                    risk=proposal["risk_status"],
                    compliance=proposal["compliance_status"],
                    reason=proposal.get("final_side_reason") or proposal.get("reason", ""),
                )
            )
        lines.extend(["", "## 주문 초안", ""])
        for proposal in trader.signals:
            lines.append(
                "- {ticker} {name}: {side}, 금액={amount:,}, 수량={quantity}, risk={risk}, compliance={compliance}".format(
                    ticker=proposal["ticker"],
                    name=proposal["name"],
                    side=proposal["side"],
                    amount=int(proposal["suggested_amount"]),
                    quantity=int(proposal["suggested_quantity"]),
                    risk=proposal["risk_status"],
                    compliance=proposal["compliance_status"],
                )
            )
    else:
        lines.extend(["", "## 후보 표", "", "Trader 결과가 없어 후보 표를 생성하지 못했습니다."])

    warning_groups = {
        result.agent: result.warnings
        for result in results
        if result.warnings
    }
    if warning_groups:
        lines.extend(["", "## 경고", ""])
        for agent, warnings in warning_groups.items():
            lines.append(f"### {agent}")
            lines.extend(f"- {warning}" for warning in warnings)
            lines.append("")

    checks = sorted({
        f"{result.agent}: {check}"
        for result in results
        for check in result.required_human_checks
    })
    if checks:
        lines.extend(["", "## 사람이 확인할 항목", ""])
        lines.extend(f"- {check}" for check in checks)

    lines.extend([
        "",
        "## 참고",
        "",
        "- 브로커 API 호출은 수행하지 않았습니다.",
        "- 이 파이프라인에서는 실제 주문 실행이 금지됩니다.",
        "- 이 보고서는 검토 보조 자료이며 투자 지시가 아닙니다.",
        "",
    ])
    return "\n".join(lines)


def render_short_summary(
    context: AgentContext,
    results: list[AgentResult],
    metadata: dict[str, Any],
) -> str:
    final_status = combine_statuses([result.status for result in results])
    candidate_counts = metadata["candidate_counts"]
    return "\n".join([
        f"{context.run_date.isoformat()} 투자위원회: {final_status}",
        (
            f"candidates={len(context.candidates)}, "
            f"매수초안={candidate_counts['draft_buys']}, "
            f"보유대기={candidate_counts['holds']}, "
            f"blocked_by_risk={metadata['blocked_by_risk']}, "
            f"blocked_by_compliance={metadata['blocked_by_compliance']}"
        ),
        "브로커 API 호출은 수행하지 않았습니다. 수동 검토가 필요합니다.",
        str(context.run_dir / "final_committee_report.md"),
        "",
    ])
