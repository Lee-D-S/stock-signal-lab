from config import settings
from .api import client


async def buy(ticker: str, quantity: int, price: int = 0, order_type: str = "00") -> dict:
    """매수 주문

    order_type: "00"=지정가, "01"=시장가
    price=0 이면 시장가로 처리
    """
    tr_id = "TTTC0011U" if not settings.kis_is_mock else "VTTC0011U"

    body = {
        "CANO": settings.kis_account_no[:8],
        "ACNT_PRDT_CD": settings.kis_account_no[8:],
        "PDNO": ticker,
        "ORD_DVSN": order_type,
        "ORD_QTY": str(quantity),
        "ORD_UNPR": str(price),
    }

    data = await client.post(
        "/uapi/domestic-stock/v1/trading/order-cash",
        body=body,
        tr_id=tr_id,
    )
    return {
        "order_id": data["output"].get("ODNO", ""),
        "ticker": ticker,
        "side": "buy",
        "quantity": quantity,
        "price": price,
    }


async def sell(ticker: str, quantity: int, price: int = 0, order_type: str = "00") -> dict:
    """매도 주문"""
    tr_id = "TTTC0012U" if not settings.kis_is_mock else "VTTC0012U"

    body = {
        "CANO": settings.kis_account_no[:8],
        "ACNT_PRDT_CD": settings.kis_account_no[8:],
        "PDNO": ticker,
        "ORD_DVSN": order_type,
        "ORD_QTY": str(quantity),
        "ORD_UNPR": str(price),
    }

    data = await client.post(
        "/uapi/domestic-stock/v1/trading/order-cash",
        body=body,
        tr_id=tr_id,
    )
    return {
        "order_id": data["output"].get("ODNO", ""),
        "ticker": ticker,
        "side": "sell",
        "quantity": quantity,
        "price": price,
    }


async def get_balance() -> list[dict]:
    """보유 잔고 조회"""
    tr_id = "TTTC8434R" if not settings.kis_is_mock else "VTTC8434R"

    data = await client.get(
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        params={
            "CANO": settings.kis_account_no[:8],
            "ACNT_PRDT_CD": settings.kis_account_no[8:],
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        },
        tr_id=tr_id,
    )

    holdings = []
    for item in data.get("output1", []):
        qty = int(item.get("hldg_qty", 0))
        if qty <= 0:
            continue
        holdings.append({
            "ticker": item["pdno"],
            "name": item["prdt_name"],
            "quantity": qty,
            "avg_price": float(item.get("pchs_avg_pric", 0)),
            "current_price": int(item.get("prpr", 0)),
            "profit_rate": float(item.get("evlu_pfls_rt", 0)),
        })
    return holdings
