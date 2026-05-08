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


class QuantSignalAgent:
    name = "QuantSignalAgent"

    def run(self, context: AgentContext) -> AgentResult:
        signals = []
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
            if candidate.signal_count <= 0:
                warnings.append(f"{candidate.ticker}: signal_count is missing.")
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
                "Confirm the source signal is still valid before any manual order."
            ],
            artifacts={"candidate_count": len(signals)},
        )


class ResearchFileAgent:
    name = "ResearchFileAgent"

    def run(self, context: AgentContext) -> AgentResult:
        signals: list[dict[str, Any]] = []
        warnings: list[str] = []
        for candidate in context.candidates:
            matches = find_research_files(context.research_root, candidate)
            quality = analyze_research_files(matches, context.run_date)
            if not matches:
                warnings.append(f"{candidate.ticker}: no research file found.")
            if quality["missing_required_sections"]:
                warnings.append(
                    f"{candidate.ticker}: missing research sections - "
                    + ", ".join(quality["missing_required_sections"])
                )
            if quality["is_stale"]:
                warnings.append(f"{candidate.ticker}: research file is stale.")
            signals.append({
                "ticker": candidate.ticker,
                "name": candidate.name,
                "research_files": [str(path) for path in matches[:5]],
                "latest_modified": quality["latest_modified"],
                "missing_required_sections": quality["missing_required_sections"],
                "forbidden_keyword_hits": quality["forbidden_keyword_hits"],
                "is_stale": quality["is_stale"],
            })

        status = "approve"
        if warnings:
            status = "needs_review"
        if not context.candidates:
            status = "needs_review"

        return AgentResult(
            agent=self.name,
            status=status,
            summary="Research file presence check completed.",
            signals=signals,
            warnings=warnings,
            required_human_checks=[
                "Read the matched research files and update the investment thesis manually.",
                "If no usable report exists, run scripts/run_new_company_reports.py --include-existing-missing.",
            ],
            artifacts={"research_root": str(context.research_root)},
        )


class PortfolioManagerAgent:
    name = "PortfolioManagerAgent"

    def run(self, context: AgentContext) -> AgentResult:
        default_amount = int(context.config["default_suggested_amount"])
        signals: list[dict[str, Any]] = []
        warnings: list[str] = []
        portfolio_by_ticker = {position.ticker: position for position in context.portfolio}
        for candidate in context.candidates:
            amount = candidate.suggested_amount or default_amount
            current_position = portfolio_by_ticker.get(candidate.ticker)
            side = "hold"
            priority = candidate.score if candidate.score is not None else float(candidate.signal_count)
            if current_position:
                reason = "Existing position; review hold, add, or trim based on thesis and weight."
                action_detail = "review_existing_position"
            elif context.portfolio_value <= 0:
                reason = "Portfolio snapshot is missing; allocation decision requires review."
                action_detail = "needs_portfolio_snapshot"
                warnings.append(f"{candidate.ticker}: portfolio snapshot is missing.")
            elif amount > context.cash:
                reason = "Cash is insufficient for a new buy draft."
                action_detail = "cash_limited"
                warnings.append(f"{candidate.ticker}: cash is insufficient.")
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
                "current_weight": current_weight,
                "reason": reason,
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
            },
        )


class RiskManagerAgent:
    name = "RiskManagerAgent"

    def run(self, context: AgentContext) -> AgentResult:
        signals: list[dict[str, Any]] = []
        all_warnings: list[str] = []
        statuses: list[str] = []
        default_amount = int(context.config["default_suggested_amount"])
        daily_new_buy_amount = sum(
            int(candidate.suggested_amount or default_amount)
            for candidate in context.candidates
            if not any(position.ticker == candidate.ticker for position in context.portfolio)
        )
        for candidate in context.candidates:
            amount = candidate.suggested_amount or default_amount
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
            statuses.append(status)
            all_warnings.extend(f"{candidate.ticker}: {warning}" for warning in warnings)
            signals.append({
                "ticker": candidate.ticker,
                "status": status,
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
            },
        )


class ComplianceOfficerAgent:
    name = "ComplianceOfficerAgent"

    def run(self, context: AgentContext) -> AgentResult:
        signals: list[dict[str, Any]] = []
        all_warnings: list[str] = []
        statuses: list[str] = []
        require_research = bool(context.config["require_research_file"])
        for candidate in context.candidates:
            matches = find_research_files(context.research_root, candidate)
            quality = analyze_research_files(matches, context.run_date)
            status, warnings = check_compliance_rules(
                candidate=candidate,
                research_matches=matches,
                require_research_file=require_research,
                research_quality=quality,
            )
            statuses.append(status)
            all_warnings.extend(f"{candidate.ticker}: {warning}" for warning in warnings)
            signals.append({
                "ticker": candidate.ticker,
                "status": status,
                "research_file_count": len(matches),
                "missing_required_sections": quality["missing_required_sections"],
                "forbidden_keyword_hits": quality["forbidden_keyword_hits"],
                "warnings": warnings,
            })

        return AgentResult(
            agent=self.name,
            status=combine_statuses(statuses),
            summary="Compliance checklist completed.",
            signals=signals,
            warnings=all_warnings,
            required_human_checks=[
                "Confirm there is no non-public information, rumor-only thesis, or missing record."
            ],
            artifacts={"require_research_file": require_research},
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
            ("research_file.json", self.research.run(context)),
        ]:
            result.write_json(context.run_dir / filename)
            results.append(result)

        portfolio_result = self.portfolio.run(context)
        portfolio_result.write_json(context.run_dir / "portfolio_manager.json")
        results.append(portfolio_result)

        risk_result = self.risk.run(context)
        risk_result.write_json(context.run_dir / "risk_manager.json")
        results.append(risk_result)

        compliance_result = self.compliance.run(context)
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
            "latest_modified": "",
            "missing_required_sections": list(REQUIRED_RESEARCH_SECTIONS),
            "forbidden_keyword_hits": [],
            "is_stale": False,
        }
    latest = max(paths, key=lambda path: path.stat().st_mtime)
    latest_dt = datetime.fromtimestamp(latest.stat().st_mtime)
    text = read_text_best_effort(latest)[:200_000].lower()
    missing = [
        section
        for section, keywords in REQUIRED_RESEARCH_SECTIONS.items()
        if not any(keyword.lower() in text for keyword in keywords)
    ]
    hits = [keyword for keyword in FORBIDDEN_RESEARCH_KEYWORDS if keyword.lower() in text]
    age_days = (datetime.combine(run_date, datetime.min.time()) - latest_dt).days
    return {
        "latest_modified": latest_dt.isoformat(timespec="seconds"),
        "missing_required_sections": missing,
        "forbidden_keyword_hits": hits,
        "is_stale": age_days > 120,
    }


def read_text_best_effort(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "cp949"):
        try:
            return path.read_text(encoding=encoding, errors="ignore")
        except OSError:
            return ""
    return ""


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
