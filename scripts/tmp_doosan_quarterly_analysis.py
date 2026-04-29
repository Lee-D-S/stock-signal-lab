from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.tmp_doosan_analysis import (  # noqa: E402
    END,
    NAME,
    TICKER,
    event_context,
    fetch_dart_disclosures,
    fetch_dart_structured,
    fetch_financials,
    fetch_investor_range,
    fetch_ohlcv,
    fetch_price_snapshot,
    fetch_short_sale,
    fmt_int,
    fmt_pct,
    fmt_won,
)
from scripts.screener_lib.dart import get_corp_code_map  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

OUT_DIR = ROOT / "ai 주가 변동 원인 분석"

PERIODS = [
    ("2026_Q1", "2026년 1분기", "2026-01-01", "2026-03-31"),
    ("2025_Q4", "2025년 4분기", "2025-10-01", "2025-12-31"),
    ("2025_Q3", "2025년 3분기", "2025-07-01", "2025-09-30"),
    ("2025_Q2", "2025년 2분기", "2025-04-01", "2025-06-30"),
    ("2025_Q1", "2025년 1분기", "2025-01-01", "2025-03-31"),
]


def period_financial_note(label: str) -> str:
    notes = {
        "2026_Q1": "2025 사업보고서와 2026년 1분기 중 발생한 공시/수급을 중심으로 해석한다.",
        "2025_Q4": "2025년 연간 실적 확정 전 구간이므로, 2025 사업보고서는 사후 확인용 수치로만 본다.",
        "2025_Q3": "2025 3분기보고서와 해당 분기 수급/시세 반응을 연결한다.",
        "2025_Q2": "2025 반기보고서가 별도 조회 대상이지만, 이 스크립트는 주요 계정 API에서 확인된 연간/3분기/사업보고서 수치를 보조 기준으로 사용한다.",
        "2025_Q1": "2024 사업보고서 제출 전후 수치와 2025년 초 수급/시세 반응을 연결한다.",
    }
    return notes.get(label, "")


def select_events(df: pd.DataFrame, n: int = 6) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
    up = df[df["chg_pct"] > 0].sort_values(["chg_pct", "trade_amount"], ascending=[False, False]).head(n)
    down = df[df["chg_pct"] < 0].sort_values(["chg_pct", "trade_amount"], ascending=[True, False]).head(n)
    return up, down


def summarize_reasons(
    qdf: pd.DataFrame,
    investor: pd.DataFrame,
    disclosures: list[dict[str, Any]],
    structured: dict[str, list[dict[str, Any]]],
    financials: dict[str, dict[str, Any]],
) -> tuple[list[str], list[str]]:
    inv_foreign = investor["foreign_qty"].sum(min_count=1) if not investor.empty else None
    inv_inst = investor["institution_qty"].sum(min_count=1) if not investor.empty else None
    inv_indiv = investor["individual_qty"].sum(min_count=1) if not investor.empty else None
    avg_amount = qdf["trade_amount"].mean() if not qdf.empty else None
    max_amount = qdf["trade_amount"].max() if not qdf.empty else None
    disclosure_names = " / ".join([d.get("report_nm", "") for d in disclosures[:5]]) or "해당 분기 주요 공시 제한적"
    latest_fin = next((v for k, v in financials.items() if "2025 사업" in k), None) or next(iter(financials.values()), {})

    up = [
        f"1. 수급 유입 후보: 분기 누적 외국인 {fmt_int(inv_foreign)}주, 기관 {fmt_int(inv_inst)}주, 개인 {fmt_int(inv_indiv)}주. 상승일 전후 외국인/기관 합산이 양수이면 우선순위를 높인다.",
        f"2. 거래대금 재평가 후보: 분기 평균 거래대금 {fmt_won(avg_amount)}, 최대 거래대금 {fmt_won(max_amount)}. 평균 대비 큰 거래대금이 붙은 상승일은 설명력이 높다.",
        f"3. DART 이벤트 후보: {disclosure_names}. 급등일 전후 5거래일 안에 공시가 있으면 보조 원인으로 본다.",
        f"4. 재무 체력 후보: 최신 확인 수치 기준 매출 {fmt_won(latest_fin.get('revenue'))}, 영업이익 {fmt_won(latest_fin.get('op_income'))}, 순이익 {fmt_won(latest_fin.get('net_income'))}. 단독 원인보다는 배경 체력으로 본다.",
    ]
    if structured.get("자기주식취득결정"):
        up.insert(0, "0. 자기주식 취득 결정 구조화 데이터가 분기 안에서 확인되어, 공시일 주변 상승의 최상위 후보로 둔다.")
    if structured.get("단일판매공급계약"):
        up.insert(0, "0. 단일판매ㆍ공급계약 구조화 데이터가 분기 안에서 확인되어, 계약 공시 주변 상승의 최상위 후보로 둔다.")

    down = [
        "1. 수급 이탈 후보: 하락일 전후 외국인/기관 합산 순매도가 확인되면 수급성 하락 후보로 우선 분류한다.",
        "2. 고거래대금 이후 차익실현 후보: 직전 상승 구간에서 거래대금이 크게 붙은 뒤 하락하면 악재보다 매물 출회 가능성을 먼저 본다.",
        "3. 공시 부재 하락 후보: 하락일 주변 DART 악재성 공시가 없으면 내부 수치만으로는 원인 확정이 어렵고, 수급/변동성 후보로 낮춰 분류한다.",
        f"4. 재무 부담 후보: 최신 확인 수치의 부채비율 {latest_fin.get('debt_ratio', float('nan')):.2f}%와 낮은 순이익률은 상승 구간의 조정 민감도를 높이는 보조 후보로 본다.",
    ]
    return up, down


def append_event_rows(
    lines: list[str],
    events: pd.DataFrame,
    investor: pd.DataFrame,
    disclosures: list[dict[str, Any]],
) -> None:
    for _, r in events.iterrows():
        ctx = event_context(r, investor, disclosures)
        disc = "<br>".join(ctx["disclosures"][:3]) if ctx["disclosures"] else "-"
        lines.append(
            f"| {r['date'].strftime('%Y-%m-%d')} | {fmt_int(r['close'])}원 | {fmt_pct(r['chg_pct'])} | {fmt_won(r['trade_amount'])} | {fmt_int(ctx['foreign_11d'])}주 | {fmt_int(ctx['institution_11d'])}주 | {disc} |"
        )


def make_report(
    code: str,
    title: str,
    start: str,
    end: str,
    ohlcv: pd.DataFrame,
    investor: pd.DataFrame,
    short_df: pd.DataFrame,
    snapshot: dict[str, Any],
    disclosures: list[dict[str, Any]],
    financials: dict[str, dict[str, Any]],
    structured: dict[str, list[dict[str, Any]]],
) -> str:
    qdf = ohlcv[(ohlcv["date"] >= pd.Timestamp(start)) & (ohlcv["date"] <= pd.Timestamp(end))].copy()
    if qdf.empty:
        return f"# {NAME} {title} 분석\n\nKIS 일봉 데이터가 비어 있어 분석하지 못했다.\n"

    qdf = qdf.sort_values("date").reset_index(drop=True)
    start_row = qdf.iloc[0]
    end_row = qdf.iloc[-1]
    high = qdf.loc[qdf["close"].idxmax()]
    low = qdf.loc[qdf["close"].idxmin()]
    ret = (end_row["close"] / start_row["close"] - 1) * 100
    up, down = select_events(qdf)
    up_reasons, down_reasons = summarize_reasons(qdf, investor, disclosures, structured, financials)

    inv_sum = {
        "foreign": investor["foreign_qty"].sum(min_count=1) if not investor.empty else None,
        "institution": investor["institution_qty"].sum(min_count=1) if not investor.empty else None,
        "individual": investor["individual_qty"].sum(min_count=1) if not investor.empty else None,
    }

    lines = [
        f"# {NAME}(034020) {title} 주가 변동 원인 후보 분석",
        "",
        "## 분석 전제",
        "",
        f"- 분석 기간: {start}~{end}",
        f"- 실제 KIS 거래일 범위: {qdf['date'].min().strftime('%Y-%m-%d')}~{qdf['date'].max().strftime('%Y-%m-%d')} ({len(qdf)}거래일)",
        f"- DART 연결 기준: {period_financial_note(code)}",
        "- 외부 뉴스, 정책, 금리, 환율, 테마 요인은 제외",
        "- 결론은 확정 원인이 아니라 DART/KIS 수치로 설명 가능한 원인 후보",
        "",
        "## 기간 주가 요약",
        "",
        "| 항목 | 수치 |",
        "|---|---:|",
        f"| 시작 종가 | {fmt_int(start_row['close'])}원 ({start_row['date'].strftime('%Y-%m-%d')}) |",
        f"| 종료 종가 | {fmt_int(end_row['close'])}원 ({end_row['date'].strftime('%Y-%m-%d')}) |",
        f"| 기간 수익률 | {ret:+.2f}% |",
        f"| 기간 고점 | {fmt_int(high['close'])}원 ({high['date'].strftime('%Y-%m-%d')}) |",
        f"| 기간 저점 | {fmt_int(low['close'])}원 ({low['date'].strftime('%Y-%m-%d')}) |",
        f"| 평균 거래대금 | {fmt_won(qdf['trade_amount'].mean())} |",
        f"| 최대 거래대금 | {fmt_won(qdf['trade_amount'].max())} |",
        "",
        "## 주요 상승일",
        "",
        "| 날짜 | 종가 | 등락률 | 거래대금 | 전후 5거래일 외국인 | 전후 5거래일 기관 | 주변 DART 공시 |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    append_event_rows(lines, up, investor, disclosures)
    lines.extend([
        "",
        "## 주요 하락일",
        "",
        "| 날짜 | 종가 | 등락률 | 거래대금 | 전후 5거래일 외국인 | 전후 5거래일 기관 | 주변 DART 공시 |",
        "|---|---:|---:|---:|---:|---:|---|",
    ])
    append_event_rows(lines, down, investor, disclosures)

    lines.extend([
        "",
        "## KIS 수급 요약",
        "",
        "| 구분 | 분기 누적 순매수 |",
        "|---|---:|",
        f"| 외국인 | {fmt_int(inv_sum['foreign'])}주 |",
        f"| 기관 | {fmt_int(inv_sum['institution'])}주 |",
        f"| 개인 | {fmt_int(inv_sum['individual'])}주 |",
        "",
        "## KIS 현재 참고 지표",
        "",
        "| 항목 | 수치 |",
        "|---|---:|",
        f"| 현재가 | {fmt_int(pd.to_numeric(snapshot.get('stck_prpr'), errors='coerce'))}원 |",
        f"| PER | {snapshot.get('per') or snapshot.get('hts_per') or 'N/A'} |",
        f"| PBR | {snapshot.get('pbr') or 'N/A'} |",
        f"| EPS | {snapshot.get('eps') or 'N/A'} |",
        f"| BPS | {snapshot.get('bps') or 'N/A'} |",
        "",
    ])

    if not short_df.empty:
        lines.extend([
            "## KIS 공매도 요약",
            "",
            "| 항목 | 수치 |",
            "|---|---:|",
            f"| 공매도 거래대금 합계 | {fmt_won(short_df['short_amount'].sum(min_count=1))} |",
            f"| 일평균 공매도 거래대금 | {fmt_won(short_df['short_amount'].mean())} |",
            f"| 최대 공매도 거래대금 | {fmt_won(short_df['short_amount'].max())} |",
            "",
        ])

    lines.extend([
        "## DART 공시 요약",
        "",
        "| 날짜 | 공시명 |",
        "|---|---|",
    ])
    for d in disclosures[:25]:
        dt = pd.to_datetime(d.get("rcept_dt"), format="%Y%m%d", errors="coerce")
        lines.append(f"| {dt.strftime('%Y-%m-%d') if pd.notna(dt) else d.get('rcept_dt')} | {d.get('report_nm')} |")

    lines.extend([
        "",
        "## DART 주요 재무 수치",
        "",
        "| 기준 | 매출액 | 영업이익 | 순이익 | 영업이익률 | 부채비율 | ROE |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])
    for label, f in financials.items():
        op_margin = f"{f['op_margin']:.2f}%" if f.get("op_margin") is not None else "N/A"
        debt_ratio = f"{f['debt_ratio']:.2f}%" if f.get("debt_ratio") is not None else "N/A"
        roe = f"{f['roe']:.2f}%" if f.get("roe") is not None else "N/A"
        lines.append(
            f"| {label} | {fmt_won(f.get('revenue'))} | {fmt_won(f.get('op_income'))} | {fmt_won(f.get('net_income'))} | {op_margin} | {debt_ratio} | {roe} |"
        )

    lines.extend(["", "## DART 구조화 이벤트 조회 결과", ""])
    for label, rows in structured.items():
        lines.append(f"- {label}: {len(rows)}건")
        for r in rows[:3]:
            keys = ["rcept_no", "rcept_dt", "aqpln_stk_ostk", "aqexpd_bgd", "aqexpd_edd", "cntrct_cncls_de", "cntrct_amount", "hd_stock_qota_rt"]
            vals = [f"{k}={r.get(k)}" for k in keys if r.get(k)]
            if vals:
                lines.append(f"  - {'; '.join(vals)}")

    lines.extend(["", "## 상승 원인 후보 우선순위", ""])
    lines.extend(up_reasons)
    lines.extend(["", "## 하락 원인 후보 우선순위", ""])
    lines.extend(down_reasons)
    lines.extend([
        "",
        "## 종합 판단",
        "",
        "이 분기의 주가 변동은 DART 재무 수치 하나로 확정하기보다, 거래대금이 동반된 가격 변동일과 외국인/기관 수급, 그리고 주변 공시 이벤트가 함께 맞는지를 우선순위로 봐야 한다.",
        "",
        "수급과 공시가 같은 방향이면 상위 후보로 두고, 공시 근거가 약하거나 수급이 엇갈리면 차익실현/변동성 후보로 낮춰 분류한다.",
        "",
    ])
    return "\n".join(lines)


async def build_period(
    code: str,
    title: str,
    start: str,
    end: str,
    corp_code: str,
    financials: dict[str, dict[str, Any]],
    snapshot: dict[str, Any],
) -> Path:
    print(f"{title} 수집 중: {start}~{end}")
    ohlcv, investor, short_df, disclosures, structured = await asyncio.gather(
        fetch_ohlcv(start, end),
        fetch_investor_range(start, end),
        fetch_short_sale(start, end),
        fetch_dart_disclosures(corp_code, start, end),
        fetch_dart_structured(corp_code, start, end),
    )
    md = make_report(code, title, start, end, ohlcv, investor, short_df, snapshot, disclosures, financials, structured)
    path = OUT_DIR / f"두산에너빌리티_{code}_원인후보_실제분석.md"
    path.write_text(md, encoding="utf-8")
    print(f"  저장: {path.name} (ohlcv={len(ohlcv)}, investor={len(investor)}, disclosures={len(disclosures)})")
    return path


async def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    corp_map = await get_corp_code_map()
    corp_code = corp_map[TICKER]
    print(f"corp_code={corp_code}, reference_end={END}")
    financials, snapshot = await asyncio.gather(fetch_financials(corp_code), fetch_price_snapshot())
    paths = []
    for code, title, start, end in PERIODS:
        paths.append(await build_period(code, title, start, end, corp_code, financials, snapshot))
        await asyncio.sleep(0.5)
    print("생성 완료")
    for p in paths:
        print(p)


if __name__ == "__main__":
    asyncio.run(main())
