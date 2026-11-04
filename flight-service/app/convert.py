"""Преобразования между строками БД, значениями Python и сообщениями proto. Плюс проверка входных полей."""

import re
import uuid
from datetime import date, datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any

from google.protobuf.timestamp_pb2 import Timestamp
from google.type import date_pb2, money_pb2

from app.errors import invalid_argument
from flight.v1 import flight_service_pb2 as pb

NANOS_PER_UNIT = Decimal(1_000_000_000)
CENT = Decimal("0.01")
MAX_PRICE = Decimal("9999999999.99")  # предел NUMERIC(12, 2)
_IATA_AIRPORT = re.compile(r"^[A-Z]{3}$")

FLIGHT_STATUS_TO_PB = {
    "SCHEDULED": pb.FLIGHT_STATUS_SCHEDULED,
    "DEPARTED": pb.FLIGHT_STATUS_DEPARTED,
    "CANCELLED": pb.FLIGHT_STATUS_CANCELLED,
    "COMPLETED": pb.FLIGHT_STATUS_COMPLETED,
}
FLIGHT_STATUS_FROM_PB = {v: k for k, v in FLIGHT_STATUS_TO_PB.items()}

RESERVATION_STATUS_TO_PB = {
    "ACTIVE": pb.RESERVATION_STATUS_ACTIVE,
    "RELEASED": pb.RESERVATION_STATUS_RELEASED,
    "EXPIRED": pb.RESERVATION_STATUS_EXPIRED,
}


# --- входные поля ---------------------------------------------------------------------------


def parse_uuid(value: str, field: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        raise invalid_argument(f"{field} must be a UUID", field=field) from None


def parse_airport(value: str, field: str) -> str:
    code = value.strip().upper()
    if not _IATA_AIRPORT.match(code):
        raise invalid_argument(f"{field} must be a 3-letter IATA airport code", field=field)
    return code


def parse_date(value: date_pb2.Date, field: str) -> date:
    try:
        return date(value.year, value.month, value.day)
    except ValueError:
        raise invalid_argument(f"{field} must be a full calendar date", field=field) from None


def parse_timestamp(value: Timestamp, field: str) -> datetime:
    try:
        return value.ToDatetime(tzinfo=timezone.utc)
    except (ValueError, OverflowError):
        raise invalid_argument(f"{field} is out of range", field=field) from None


def parse_money(value: money_pb2.Money, field: str) -> tuple[Decimal, str]:
    """Цена: положительная, не больше двух знаков после запятой, в пределах NUMERIC(12, 2)."""
    currency = value.currency_code.strip().upper()
    if not re.fullmatch(r"[A-Z]{3}", currency):
        raise invalid_argument(f"{field}.currency_code must be an ISO 4217 code", field=field)
    if not -999_999_999 <= value.nanos <= 999_999_999 or (value.units > 0 and value.nanos < 0) or (
        value.units < 0 and value.nanos > 0
    ):
        raise invalid_argument(f"{field}.nanos is inconsistent with units", field=field)
    amount = Decimal(value.units) + Decimal(value.nanos) / NANOS_PER_UNIT
    if amount <= 0 or amount > MAX_PRICE or amount != amount.quantize(CENT):
        raise invalid_argument(f"{field} must be in (0, {MAX_PRICE}] with at most 2 decimal places", field=field)
    return amount, currency


# --- исходящие сообщения ----------------------------------------------------------------------


def money(amount: Decimal, currency: str) -> money_pb2.Money:
    units = int(amount.to_integral_value(rounding=ROUND_DOWN))
    nanos = int((amount - units) * NANOS_PER_UNIT)
    return money_pb2.Money(currency_code=currency, units=units, nanos=nanos)


def timestamp(value: datetime) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(value.astimezone(timezone.utc))
    return ts


def flight_to_pb(row: Any) -> pb.Flight:
    """row — строка flights, соединённая с airlines и airports (см. FlightStore.FLIGHT_VIEW)."""
    return pb.Flight(
        id=str(row["id"]),
        flight_number=f"{row['airline_code']}{row['flight_no']}",
        airline=pb.Airline(iata_code=row["airline_code"], name=row["airline_name"]),
        origin=pb.Airport(iata_code=row["origin"], name=row["origin_name"], city=row["origin_city"]),
        destination=pb.Airport(
            iata_code=row["destination"], name=row["destination_name"], city=row["destination_city"]
        ),
        departure_time=timestamp(row["departure_time"]),
        arrival_time=timestamp(row["arrival_time"]),
        total_seats=row["total_seats"],
        available_seats=row["available_seats"],
        price=money(row["price"], row["currency"]),
        status=FLIGHT_STATUS_TO_PB[row["status"]],
    )


def reservation_to_pb(row: Any) -> pb.SeatReservation:
    return pb.SeatReservation(
        id=str(row["id"]),
        flight_id=str(row["flight_id"]),
        booking_id=str(row["booking_id"]),
        seat_count=row["seat_count"],
        status=RESERVATION_STATUS_TO_PB[row["status"]],
        created_at=timestamp(row["created_at"]),
        updated_at=timestamp(row["updated_at"]),
    )
