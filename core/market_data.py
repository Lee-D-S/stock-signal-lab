import pandas as pd

from .api import client


async def get_current_price(ticker: str) -> dict:
    """현재가 조회"""
    data = await client.get(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
        tr_id="FHKST01010100",
    )
    output = data["output"]
    return {
        "ticker": ticker,
        "price": int(output["stck_prpr"]),       # 현재가
        "open": int(output["stck_oprc"]),         # 시가
        "high": int(output["stck_hgpr"]),         # 고가
        "low": int(output["stck_lwpr"]),          # 저가
        "volume": int(output["acml_vol"]),         # 누적 거래량
        "name": output.get("hts_kor_isnm", ""),
        "per": output.get("per", ""),             # PER
        "pbr": output.get("pbr", ""),             # PBR
        "eps": output.get("eps", ""),             # EPS
    }


async def get_volume_rank(top_n: int = 10) -> list[dict]:
    """거래량 상위 종목 조회 (KRX 전체, 보통주)"""
    data = await client.get(
        "/uapi/domestic-stock/v1/quotations/volume-rank",
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000",      # 전체 종목
            "FID_DIV_CLS_CODE": "0",       # 전체 (보통주+우선주)
            "FID_BLNG_CLS_CODE": "0",      # 평균거래량 기준
            "FID_TRGT_CLS_CODE": "111111111",
            "FID_TRGT_EXLS_CLS_CODE": "000000",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
            "FID_INPUT_DATE_1": "",
        },
        tr_id="FHPST01710000",
    )
    rows = data.get("output", [])[:top_n]
    return [
        {
            "rank": int(row["data_rank"]),
            "ticker": row["mksc_shrn_iscd"],
            "name": row["hts_kor_isnm"],
            "price": int(row["stck_prpr"]),
            "volume": int(row["acml_vol"]),
            "change_rate": float(row["prdy_ctrt"]),
        }
        for row in rows
    ]


async def get_sector_changes(market: str = "K") -> list[dict]:
    """업종별 등락률 조회

    market: "K"=코스피, "Q"=코스닥
    """
    iscd = "0001" if market == "K" else "1001"
    data = await client.get(
        "/uapi/domestic-stock/v1/quotations/inquire-index-category-price",
        params={
            "FID_COND_MRKT_DIV_CODE": "U",
            "FID_INPUT_ISCD": iscd,
            "FID_COND_SCR_DIV_CODE": "20214",
            "FID_MRKT_CLS_CODE": market,
            "FID_BLNG_CLS_CODE": "0",
        },
        tr_id="FHPUP02140000",
    )
    rows = data.get("output2", [])
    return [
        {
            "name": row.get("hts_kor_isnm", ""),
            "change_rate": float(row.get("bstp_nmix_prdy_ctrt", 0)),
            "change_point": float(row.get("bstp_nmix_prdy_vrss", 0)),
            "current": float(row.get("bstp_nmix_prpr", 0)),
            "up_count": int(row.get("ascn_issu_cnt", 0)),
            "down_count": int(row.get("down_issu_cnt", 0)),
            "flat_count": int(row.get("stnr_issu_cnt", 0)),
        }
        for row in rows
        if row.get("hts_kor_isnm")
    ]


async def get_ohlcv(ticker: str, period: str = "D", count: int = 60) -> pd.DataFrame:
    """일봉/주봉/월봉 OHLCV 조회

    period: "D"=일봉, "W"=주봉, "M"=월봉
    """
    data = await client.get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_INPUT_DATE_1": "",
            "FID_INPUT_DATE_2": "",
            "FID_PERIOD_DIV_CODE": period,
            "FID_ORG_ADJ_PRC": "0",
        },
        tr_id="FHKST03010100",
    )

    rows = data.get("output2", [])[:count]
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.rename(columns={
        "stck_bsop_date": "date",
        "stck_oprc": "open",
        "stck_hgpr": "high",
        "stck_lwpr": "low",
        "stck_clpr": "close",
        "acml_vol": "volume",
    })
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col])
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df
