from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
BACKTEST_DIR = BASE_DIR / "06_백테스트"
STRATEGY_DIR = BASE_DIR / "07_전략신호"
CACHE_DIR = ROOT / "data" / "ohlcv_cache"

TRADES_CSV = BACKTEST_DIR / "가설_실전_백테스트_거래.csv"
ALL_CONFIGS_CSV = BACKTEST_DIR / "가설_실전_백테스트_전체_설정.csv"
BACKTEST_INPUTS_CSV = BACKTEST_DIR / "가설_백테스트_입력값.csv"

GAP_CSV = BACKTEST_DIR / "가설_백테스트_갭_분류.csv"
GAP_MD = BACKTEST_DIR / "가설_백테스트_갭_분류.md"
STRATEGY_CSV = STRATEGY_DIR / "전략_조건_초안.csv"
STRATEGY_MD = STRATEGY_DIR / "전략_조건_초안.md"

KEEP_HYPOTHESES = ["H02", "H01", "H03", "H06", "H04"]


def load_cache(ticker: str) -> pd.DataFrame:
    for suffix, reader in ((".parquet", pd.read_parquet), (".pkl", pd.read_pickle)):
        path = CACHE_DIR / f"{ticker}{suffix}"
        if path.is_file():
            try:
                df = reader(path)
            except Exception:
                continue
            if not df.empty and "date" in df.columns:
                df = df.copy()
                df["date"] = pd.to_datetime(df["date"])
                return df.sort_values("date").reset_index(drop=True)
    return pd.DataFrame()


def classify_gap(row: pd.Series) -> dict[str, Any]:
    ticker = str(row["ticker"]).zfill(6)
    event_date = pd.Timestamp(row["date"])
    df = load_cache(ticker)
    if df.empty:
        return {
            "gap_type": "cache_missing",
            "cache_start": "",
            "cache_end": "",
            "nearest_prev_date": "",
            "nearest_next_date": "",
            "note": "해당 종목 OHLCV 캐시 파일 없음",
        }

    dates = df["date"]
    cache_start, cache_end = dates.min(), dates.max()
    prev_dates = dates[dates < event_date]
    next_dates = dates[dates > event_date]
    prev_date = "" if prev_dates.empty else prev_dates.max().strftime("%Y-%m-%d")
    next_date = "" if next_dates.empty else next_dates.min().strftime("%Y-%m-%d")

    if event_date < cache_start:
        gap_type = "before_cache_start"
        note = "캐시 시작일보다 이벤트가 빠름"
    elif event_date > cache_end:
        gap_type = "after_cache_end"
        note = "캐시 종료일보다 이벤트가 늦음"
    else:
        gap_type = "missing_inside_cache_range"
        note = "캐시 범위 안에 있으나 해당 날짜 행이 없음. 휴장일/잘못된 이벤트 날짜/캐시 병합 문제 확인 필요"

    return {
        "gap_type": gap_type,
        "cache_start": cache_start.strftime("%Y-%m-%d"),
        "cache_end": cache_end.strftime("%Y-%m-%d"),
        "nearest_prev_date": prev_date,
        "nearest_next_date": next_date,
        "note": note,
    }


def markdown_table(df: pd.DataFrame, max_rows: int = 80) -> str:
    if df.empty:
        return "_데이터 없음_"
    view = df.head(max_rows).copy()
    for col in view.columns:
        view[col] = view[col].map(lambda x: "N/A" if pd.isna(x) else x)
    headers = [str(col) for col in view.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for _, row in view.iterrows():
        values = [str(row[col]).replace("|", "\\|") for col in view.columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):+.2f}%"


def fmt_rate(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.2%}"


def build_gap_report(gaps: pd.DataFrame) -> str:
    if gaps.empty:
        return "\n".join(
            [
                "# 백테스트 잔여 누락 이벤트 분류",
                "",
                "## 요약",
                "",
                "- 잔여 `event_date_not_in_cache`: 0건",
                "- OHLCV 캐시 내부 공백 보강 후 이벤트 날짜 기준 누락은 해소됐다.",
                "- 남은 백테스트 제외 이벤트는 동일 후보/동일 종목 중복 신호 제거에 따른 것이다.",
                "",
            ]
        )
    counts = gaps.groupby(["gap_type"], as_index=False).size().sort_values("size", ascending=False)
    by_hypothesis = gaps.groupby(["hypothesis_id", "gap_type"], as_index=False).size().sort_values(["hypothesis_id", "gap_type"])
    return "\n".join(
        [
            "# 백테스트 잔여 누락 이벤트 분류",
            "",
            "## 요약",
            "",
            f"- 잔여 `event_date_not_in_cache`: {len(gaps):,}건",
            "- 대부분 캐시 종료일 이후 이벤트이거나 캐시 범위 내부 날짜 누락인지 여부를 이 파일에서 분류한다.",
            "",
            "## 누락 유형",
            "",
            markdown_table(counts),
            "",
            "## 후보별 누락",
            "",
            markdown_table(by_hypothesis),
            "",
            "## 상세",
            "",
            markdown_table(
                gaps[
                    [
                        "hypothesis_id",
                        "ticker",
                        "name",
                        "date",
                        "gap_type",
                        "cache_start",
                        "cache_end",
                        "nearest_prev_date",
                        "nearest_next_date",
                        "note",
                    ]
                ],
                max_rows=120,
            ),
            "",
        ]
    )


def best_config_for(hypothesis_id: str, configs: pd.DataFrame) -> pd.Series:
    subset = configs[configs["hypothesis_id"] == hypothesis_id].copy()
    if subset.empty:
        raise ValueError(f"missing config for {hypothesis_id}")
    subset = subset.sort_values(["avg_score_return_pct", "hit_rate", "tested_trades"], ascending=[False, False, False])
    return subset.iloc[0]


def build_strategy_draft() -> tuple[pd.DataFrame, str]:
    inputs = pd.read_csv(BACKTEST_INPUTS_CSV, encoding="utf-8-sig")
    configs = pd.read_csv(ALL_CONFIGS_CSV, encoding="utf-8-sig")
    rows = []
    for rank, hid in enumerate(KEEP_HYPOTHESES, 1):
        base = inputs[inputs["hypothesis_id"] == hid].iloc[0]
        best = best_config_for(hid, configs)
        if base["action_hint"] == "진입 후보":
            use_type = "매수 후보"
            response = "다음 거래일 분할 진입 검토"
        elif base["action_hint"] == "반등 관찰 후보":
            use_type = "반등 감시 후보"
            response = "하락 이벤트 다음 거래일 반등 확인 후 단기 진입 검토"
        elif base["action_hint"] == "추격매수 회피 후보":
            use_type = "회피 후보"
            response = "급등 당일/익일 추격매수 금지 또는 보유 물량 리스크 점검"
        else:
            use_type = "검토 후보"
            response = "추가 검토"
        rows.append(
            {
                "priority": rank,
                "hypothesis_id": hid,
                "use_type": use_type,
                "market_regime": base["market_regime"],
                "direction": base["direction"],
                "amount_tag": base["amount_tag"],
                "flow_category": base["flow_category"],
                "dart_tag": base["dart_tag"],
                "window_category": base["window_category"],
                "action_hint": base["action_hint"],
                "suggested_response": response,
                "preferred_entry_mode": best["entry_mode"],
                "preferred_hold_days": int(best["hold_days"]),
                "tested_trades": int(best["tested_trades"]),
                "avg_score_return_pct": best["avg_score_return_pct"],
                "hit_rate": best["hit_rate"],
                "risk_note": risk_note(hid),
            }
        )
    draft = pd.DataFrame(rows)
    return draft, build_strategy_markdown(draft)


def risk_note(hypothesis_id: str) -> str:
    notes = {
        "H02": "외부 자료 확인 필요 이벤트가 많아 뉴스/업황 필터를 추가해야 자동화 가능",
        "H01": "수익률에 큰 아웃라이어가 포함될 수 있어 최대 손실과 종목 쏠림 관리 필요",
        "H03": "20일보다 5~10일 단기 반등 관찰이 안정적",
        "H06": "매수 전략이 아니라 추격매수 회피/리스크 경고로 사용",
        "H04": "과거 유동성/저금리 후반장 전용 회피 후보라 현재 장세 적용성은 낮음",
    }
    return notes.get(hypothesis_id, "")


def build_strategy_markdown(draft: pd.DataFrame) -> str:
    view = draft.copy()
    view["avg_score_return_pct"] = view["avg_score_return_pct"].map(fmt_pct)
    view["hit_rate"] = view["hit_rate"].map(fmt_rate)
    lines = [
        "# 전략 감시 조건 초안",
        "",
        "## 사용 원칙",
        "",
        "- 아래 조건은 확정 매매 전략이 아니라 실시간 감시 조건 초안이다.",
        "- `dominant_followup` 같은 결과 라벨은 진입 조건으로 쓰지 않는다.",
        "- 조건은 이벤트 발생 전 또는 당일 확인 가능한 거래대금, 수급, 공시, 원인 창 분류만 사용한다.",
        "- H02/H03은 하락 후 반등 관찰 조건이고, H06은 매수 조건이 아니라 추격매수 회피 조건이다.",
        "",
        "## 우선 적용 후보",
        "",
        markdown_table(
            view[
                [
                    "priority",
                    "hypothesis_id",
                    "use_type",
                    "market_regime",
                    "direction",
                    "amount_tag",
                    "flow_category",
                    "dart_tag",
                    "window_category",
                    "suggested_response",
                    "preferred_entry_mode",
                    "preferred_hold_days",
                    "tested_trades",
                    "avg_score_return_pct",
                    "hit_rate",
                    "risk_note",
                ]
            ]
        ),
        "",
        "## 조건 해석",
        "",
        "- H02: 변동성 장세에서 약한 거래대금 하락이지만 외국인/기관 동반매수와 DART 공시가 같이 있으면 반등 후보로 감시한다.",
        "- H01: 변동성 장세에서 약한 거래대금 상승이어도 외국인/기관 동반매수와 DART 공시가 같이 있으면 진입 후보로 본다.",
        "- H03: AI/전력기기 테마장에서는 공시가 없어도 누적 배경이 있고 수급이 엇갈린 평균상회 거래대금 하락은 단기 반등 후보로 본다.",
        "- H06: 반도체 반등장에서 거래대금 급증 상승이 나왔는데 외국인/기관이 동반매도이면 추격매수 회피 신호로 본다.",
        "- H04: 유동성/저금리 후반장에서 거래대금 급증 상승과 외국인/기관 동반매도가 같이 나오면 추격매수 회피 신호로 참고한다.",
        "",
        "## 다음 구현",
        "",
        "- 실시간 또는 일별 배치에서 위 조건을 감지해 `관심종목_시그널_후보.csv` 형태로 저장한다.",
        "- H02는 외부 뉴스/업황 필터가 붙기 전까지 자동 매수보다 관찰 알림으로 제한한다.",
        "- H06은 매수 후보가 아니라 경고/회피 후보로 분리 저장한다.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    trades = pd.read_csv(TRADES_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    gaps = trades[trades["skip_reason"] == "event_date_not_in_cache"].copy()
    if not gaps.empty:
        gaps = gaps.reset_index(drop=True)
        classified = gaps.apply(classify_gap, axis=1, result_type="expand")
        gaps = pd.concat([gaps, classified.reset_index(drop=True)], axis=1)
    gaps.to_csv(GAP_CSV, index=False, encoding="utf-8-sig")
    GAP_MD.write_text(build_gap_report(gaps), encoding="utf-8")

    draft, draft_md = build_strategy_draft()
    draft.to_csv(STRATEGY_CSV, index=False, encoding="utf-8-sig")
    STRATEGY_MD.write_text(draft_md, encoding="utf-8")

    print(f"gaps={len(gaps)}")
    if not gaps.empty:
        print(gaps["gap_type"].value_counts().to_string())
    print(f"gap_csv={GAP_CSV}")
    print(f"gap_md={GAP_MD}")
    print(f"strategy_csv={STRATEGY_CSV}")
    print(f"strategy_md={STRATEGY_MD}")


if __name__ == "__main__":
    main()
