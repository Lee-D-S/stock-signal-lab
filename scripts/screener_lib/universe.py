import asyncio
from core.api.client import get_marketdata


def _parse_rows(rows: list[dict]) -> list[dict]:
    return [
        {
            "ticker":      r.get("mksc_shrn_iscd") or r.get("stck_shrn_iscd", ""),
            "name":        r.get("hts_kor_isnm", ""),
            "price":       int(r.get("stck_prpr", 0) or 0),
            "change_rate": r.get("prdy_ctrt", "0"),
            "market_cap":   int(r.get("stck_avls", 0) or 0),
            "trade_amount": int(r.get("acml_tr_pbmn", 0) or r.get("avrg_tr_pbmn", 0) or 0),
        }
        for r in rows
        if r.get("mksc_shrn_iscd") or r.get("stck_shrn_iscd")
    ]


async def _fetch_volume_rank(market_code: str, rank_code: str = "0") -> list[dict]:
    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/volume-rank",
        params={
            "FID_COND_MRKT_DIV_CODE":  "J",
            "FID_COND_SCR_DIV_CODE":   "20171",
            "FID_INPUT_ISCD":          market_code,
            "FID_DIV_CLS_CODE":        "0",
            "FID_BLNG_CLS_CODE":       rank_code,
            "FID_TRGT_CLS_CODE":       "111111111",
            "FID_TRGT_EXLS_CLS_CODE":  "000000",
            "FID_INPUT_PRICE_1":       "",
            "FID_INPUT_PRICE_2":       "",
            "FID_VOL_CNT":             "",
            "FID_INPUT_DATE_1":        "",
        },
        tr_id="FHPST01710000",
    )
    return _parse_rows(data.get("output", []))


async def _fetch_market_cap(market_code: str) -> list[dict]:
    result, tr_cont = [], ""
    while True:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/ranking/market-cap",
            params={
                "fid_cond_mrkt_div_code":  "J",
                "fid_cond_scr_div_code":   "20174",
                "fid_div_cls_code":        "0",
                "fid_input_iscd":          market_code,
                "fid_trgt_cls_code":       "0",
                "fid_trgt_exls_cls_code":  "0",
                "fid_input_price_1":       "",
                "fid_input_price_2":       "",
                "fid_vol_cnt":             "",
            },
            tr_id="FHPST01740000",
            tr_cont=tr_cont,
        )
        result.extend(_parse_rows(data.get("output", [])))
        if data.get("__tr_cont__", "") != "M":
            break
        tr_cont = "N"
        await asyncio.sleep(0.2)
    return result


async def get_stock_universe(by: str) -> list[dict]:
    if by == "marcap":
        kospi, kosdaq = await asyncio.gather(
            _fetch_market_cap("0001"),
            _fetch_market_cap("1001"),
        )
        seen, result = set(), []
        for stock in kospi + kosdaq:
            if stock["ticker"] and stock["ticker"] not in seen:
                seen.add(stock["ticker"])
                result.append(stock)
        return sorted(result, key=lambda s: s.get("market_cap", 0), reverse=True)
    if by == "amount":
        kospi, kosdaq = await asyncio.gather(
            _fetch_volume_rank("0001", rank_code="3"),
            _fetch_volume_rank("1001", rank_code="3"),
        )
        seen, result = set(), []
        for stock in kospi + kosdaq:
            if stock["ticker"] and stock["ticker"] not in seen:
                seen.add(stock["ticker"])
                result.append(stock)
        return sorted(result, key=lambda s: s.get("trade_amount", 0), reverse=True)
    kospi, kosdaq = await asyncio.gather(
        _fetch_volume_rank("0001"),
        _fetch_volume_rank("1001"),
    )
    seen, result = set(), []
    for stock in kospi + kosdaq:
        if stock["ticker"] and stock["ticker"] not in seen:
            seen.add(stock["ticker"])
            result.append(stock)
    return result
