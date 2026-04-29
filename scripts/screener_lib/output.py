import argparse

from .utils import _fmt_amount


def _needs_valuation(args: argparse.Namespace) -> bool:
    return any([
        getattr(args, "per_max", None) is not None,
        getattr(args, "pbr_max", None) is not None,
        getattr(args, "eps_min", None) is not None,
        getattr(args, "bps_min", None) is not None,
    ])


def print_results(results: list[dict], args: argparse.Namespace) -> None:
    if not results:
        print("조건을 만족하는 종목 없음")
        return

    print(f"통과 종목: {len(results)}개\n")

    show_val = _needs_valuation(args)

    ma_header = ""
    if args.ma_align:
        ma_header = "  " + "  ".join(f"{'MA' + str(p):>9}" for p in args.ma_align)

    val_header = "  {'PER':>6}  {'PBR':>5}" if show_val else ""

    header = (
        f"{'코드':>8}  {'종목명':^14}  {'현재가':>9}  {'등락률':>7}  "
        f"{'거래대금':>9}  {'RSI':>6}  {'MACD-H':>7}  {'Stoch':>6}  OBV  Vol"
        + ("  {'PER':>6}  {'PBR':>5}" if show_val else "")
        + ma_header
    )
    # f-string 안에서 dict-key 스타일 포맷이 불가해 직접 구성
    base = (
        f"{'코드':>8}  {'종목명':^14}  {'현재가':>9}  {'등락률':>7}  "
        f"{'거래대금':>9}  {'RSI':>6}  {'MACD-H':>7}  {'Stoch':>6}  OBV  Vol"
    )
    val_h   = f"  {'PER':>6}  {'PBR':>5}" if show_val else ""
    header  = base + val_h + ma_header
    print(header)
    print("-" * len(header))

    for r in results:
        ind = r["ind"]

        rsi_s   = f"{ind['rsi']:6.1f}"     if ind.get("rsi")     is not None else "   N/A"
        macd_h  = ind.get("macd_hist")
        macd_s  = f"{macd_h:+7.2f}"        if macd_h             is not None else "    N/A"
        stoch_s = f"{ind['stoch_k']:6.1f}" if ind.get("stoch_k") is not None else "   N/A"
        obv_s   = "↑" if ind.get("obv_rising")   else "↓"
        vol_s   = "↑" if ind.get("vol_above_ma") else "↓"

        val_s = ""
        if show_val:
            val = ind.get("valuation") or {}
            per = val.get("per")
            pbr = val.get("pbr")
            per_s = f"{per:6.1f}" if per else "   N/A"
            pbr_s = f"{pbr:5.2f}" if pbr else "  N/A"
            val_s = f"  {per_s}  {pbr_s}"

        ma_vals = ""
        if args.ma_align:
            ma_vals = "  " + "  ".join(
                f"{ind[f'ma{p}']:9,.0f}" if ind.get(f"ma{p}") is not None else "      N/A"
                for p in args.ma_align
            )

        print(
            f"{r['ticker']:>8}  {r['name']:^14}  {r['price']:>9,}  "
            f"{float(r['change_rate']):>+6.2f}%  "
            f"{_fmt_amount(r['trade_amount']):>9}  "
            f"{rsi_s}  {macd_s}  {stoch_s}  {obv_s:>3}  {vol_s:>3}"
            + val_s + ma_vals
        )
