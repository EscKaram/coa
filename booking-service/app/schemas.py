"""Модели запросов и ответов REST API."""

import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from app.flight_client import money_to_decimal
from flight.v1 import flight_service_pb2 as pb

USER_ID_PATTERN = r"^[A-Za-z0-9_.@-]{1,64}$"
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
MAX_SEAT_COUNT = 1000

FLIGHT_STATUS = {
    pb.FLIGHT_STATUS_SCHEDULED: "SCHEDULED",
    pb.FLIGHT_STATUS_DEPARTED: "DEPARTED",
    pb.FLIGHT_STATUS_CANCELLED: "CANCELLED",
    pb.FLIGHT_STATUS_COMPLETED: "COMPLETED",
}


class BookingCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    user_id: str = Field(pattern=USER_ID_PATTERN, examples=["user-42"])
    flight_id: uuid.UUID
    passenger_name: str = Field(min_length=1, max_length=200, examples=["Иван Петров"])
    passenger_email: str = Field(max_length=254, pattern=EMAIL_PATTERN, examples=["ivan@example.com"])
    seat_count: int = Field(strict=True, ge=1, le=MAX_SEAT_COUNT)


class BookingOut(BaseModel):
    id: uuid.UUID
    user_id: str
    flight_id: uuid.UUID
    passenger_name: str
    passenger_email: str
    seat_count: int
    total_price: str = Field(description="seat_count * цена места на момент бронирования, строка с 2 знаками")
    currency: str = Field(description="ISO 4217")
    status: Literal["CONFIRMED", "CANCELLED"]
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: Any) -> "BookingOut":
        return cls(
            id=row["id"], user_id=row["user_id"], flight_id=row["flight_id"],
            passenger_name=row["passenger_name"], passenger_email=row["passenger_email"],
            seat_count=row["seat_count"], total_price=f"{row['total_price']:.2f}", currency=row["currency"].strip(),
            status=row["status"], created_at=row["created_at"], updated_at=row["updated_at"],
        )


class AirlineOut(BaseModel):
    code: str
    name: str


class AirportOut(BaseModel):
    code: str
    name: str
    city: str


class FlightOut(BaseModel):
    id: uuid.UUID
    flight_number: str
    airline: AirlineOut
    origin: AirportOut
    destination: AirportOut
    departure_time: datetime
    arrival_time: datetime
    total_seats: int
    available_seats: int
    price: str = Field(description="цена одного места, строка с 2 знаками")
    currency: str = Field(description="ISO 4217")
    status: Literal["SCHEDULED", "DEPARTED", "CANCELLED", "COMPLETED"]

    @classmethod
    def from_pb(cls, flight: pb.Flight) -> "FlightOut":
        return cls(
            id=flight.id,
            flight_number=flight.flight_number,
            airline=AirlineOut(code=flight.airline.iata_code, name=flight.airline.name),
            origin=AirportOut(code=flight.origin.iata_code, name=flight.origin.name, city=flight.origin.city),
            destination=AirportOut(
                code=flight.destination.iata_code, name=flight.destination.name, city=flight.destination.city
            ),
            departure_time=flight.departure_time.ToDatetime(tzinfo=timezone.utc),
            arrival_time=flight.arrival_time.ToDatetime(tzinfo=timezone.utc),
            total_seats=flight.total_seats,
            available_seats=flight.available_seats,
            price=f"{money_to_decimal(flight.price):.2f}",
            currency=flight.price.currency_code,
            status=FLIGHT_STATUS[flight.status],
        )


class ErrorOut(BaseModel):
    error_code: str
    message: str
    details: dict[str, Any] | None = None
