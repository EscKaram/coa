"""Таблица bookings (SQLAlchemy Core). Схемой управляет Flyway (migrations/), не ORM."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import CHAR, Column, DateTime, Integer, MetaData, Numeric, String, Table
from sqlalchemy.dialects.postgresql import ENUM, UUID
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

metadata = MetaData()

booking_status = ENUM("CONFIRMED", "CANCELLED", name="booking_status", create_type=False)

bookings = Table(
    "bookings",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("user_id", String(64), nullable=False),
    Column("flight_id", UUID(as_uuid=True), nullable=False),
    Column("passenger_name", String(200), nullable=False),
    Column("passenger_email", String(254), nullable=False),
    Column("seat_count", Integer, nullable=False),
    Column("total_price", Numeric(15, 2), nullable=False),
    Column("currency", CHAR(3), nullable=False),
    Column("status", booking_status, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)


class Database:
    def __init__(self, url: str) -> None:
        self.engine: AsyncEngine = create_async_engine(url, pool_pre_ping=True)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncConnection]:
        """Одна транзакция: commit при выходе без исключения, иначе rollback."""
        async with self.engine.begin() as conn:
            yield conn

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[AsyncConnection]:
        async with self.engine.connect() as conn:
            yield conn
