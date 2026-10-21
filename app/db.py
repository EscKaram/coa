"""Подключение к PostgreSQL. Схемой управляет Flyway (db/migrations), не ORM."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings

engine = create_async_engine(settings.database_url, pool_pre_ping=True)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def transaction() -> AsyncIterator[AsyncSession]:
    """Сессия с одной транзакцией: commit при успехе, rollback при любом исключении."""
    async with SessionFactory() as session, session.begin():
        yield session
