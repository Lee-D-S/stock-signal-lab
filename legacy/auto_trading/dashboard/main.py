from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from .routers import market, positions, strategies, trades

app = FastAPI(
    title="Auto Invest Dashboard",
    description="자동 매매 모니터링 API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000", "http://127.0.0.1:8000"],
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-API-Key"],
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next) -> Response:
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    return response

app.include_router(trades.router)
app.include_router(positions.router)
app.include_router(strategies.router)
app.include_router(market.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
