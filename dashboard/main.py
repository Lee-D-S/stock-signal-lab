from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import market, positions, strategies, trades

app = FastAPI(
    title="Auto Invest Dashboard",
    description="자동 매매 모니터링 API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(trades.router)
app.include_router(positions.router)
app.include_router(strategies.router)
app.include_router(market.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
