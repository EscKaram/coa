"""Таблицы Flight Service (SQLAlchemy Core). Схемой управляет Flyway (migrations/), не ORM."""

from sqlalchemy import CHAR, Column, DateTime, Integer, MetaData, Numeric, SmallInteger, String, Table
from sqlalchemy.dialects.postgresql import ENUM, UUID

metadata = MetaData()

flight_status = ENUM("SCHEDULED", "DEPARTED", "CANCELLED", "COMPLETED", name="flight_status", create_type=False)
reservation_status = ENUM("ACTIVE", "RELEASED", "EXPIRED", name="reservation_status", create_type=False)

airlines = Table(
    "airlines",
    metadata,
    Column("iata_code", CHAR(2), primary_key=True),
    Column("name", String(100), nullable=False),
)

airports = Table(
    "airports",
    metadata,
    Column("iata_code", CHAR(3), primary_key=True),
    Column("name", String(100), nullable=False),
    Column("city", String(100), nullable=False),
)

flights = Table(
    "flights",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("airline_code", CHAR(2), nullable=False),
    Column("flight_no", SmallInteger, nullable=False),
    Column("origin", CHAR(3), nullable=False),
    Column("destination", CHAR(3), nullable=False),
    Column("departure_time", DateTime(timezone=True), nullable=False),
    Column("arrival_time", DateTime(timezone=True), nullable=False),
    Column("total_seats", Integer, nullable=False),
    Column("available_seats", Integer, nullable=False),
    Column("price", Numeric(12, 2), nullable=False),
    Column("currency", CHAR(3), nullable=False),
    Column("status", flight_status, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

seat_reservations = Table(
    "seat_reservations",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("flight_id", UUID(as_uuid=True), nullable=False),
    Column("booking_id", UUID(as_uuid=True), nullable=False, unique=True),
    Column("seat_count", Integer, nullable=False),
    Column("status", reservation_status, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
