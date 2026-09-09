from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class SectorSignal(Base):
    __tablename__ = "sector_signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    sector_name: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    sentiment: Mapped[str] = mapped_column(String(10), nullable=False)   # "positive" | "negative"
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=True)
    predicted_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    actual_change_rate: Mapped[float] = mapped_column(Float, nullable=True)   # 장 마감 후 채워짐
    hit: Mapped[bool] = mapped_column(Boolean, nullable=True)                 # 예측 적중 여부
    verified_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
