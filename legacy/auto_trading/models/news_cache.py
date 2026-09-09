from datetime import datetime

from sqlalchemy import DateTime, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class NewsCache(Base):
    """크롤링한 뉴스 중복 처리 및 분석 결과 캐싱"""

    __tablename__ = "news_cache"

    url_hash: Mapped[str] = mapped_column(String(64), primary_key=True)  # SHA256 해시
    title: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    analysis: Mapped[str] = mapped_column(Text, nullable=True)           # AI 분석 결과 JSON
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )
