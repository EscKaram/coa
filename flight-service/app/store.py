"""Операции над рейсами и резервациями в PostgreSQL. Каждая мутация — одна транзакция.

Блокировки: строка рейса берётся SELECT ... FOR UPDATE раньше строки резервации во всех
операциях, поэтому параллельные ReserveSeats / ReleaseReservation / UpdateFlight одного рейса
выполняются по очереди и не взаимоблокируются. Последнее место не продаётся дважды: второй
ReserveSeats ждёт блокировку и видит уже уменьшенный available_seats. CHECK (available_seats >= 0)
в схеме — вторая линия защиты.
"""

import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

import grpc
from sqlalchemy import and_, insert, select, update
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.db import airlines, airports, flights, seat_reservations
from app.errors import FlightError, flight_not_found, invalid_argument

_origin = airports.alias("origin_airport")
_destination = airports.alias("destination_airport")

# Рейс вместе с названиями авиакомпании и аэропортов — всё, что нужно для сообщения Flight.
FLIGHT_VIEW = select(
    flights,
    airlines.c.name.label("airline_name"),
    _origin.c.name.label("origin_name"),
    _origin.c.city.label("origin_city"),
    _destination.c.name.label("destination_name"),
    _destination.c.city.label("destination_city"),
).select_from(
    flights.join(airlines, airlines.c.iata_code == flights.c.airline_code)
    .join(_origin, _origin.c.iata_code == flights.c.origin)
    .join(_destination, _destination.c.iata_code == flights.c.destination)
)

# Допустимые переходы статуса рейса. Остальные (в том числе из CANCELLED/COMPLETED) запрещены.
FLIGHT_TRANSITIONS = {
    "SCHEDULED": {"DEPARTED", "CANCELLED"},
    "DEPARTED": {"COMPLETED"},
}


@dataclass(frozen=True)
class FlightChange:
    """Что изменилось: по этим данным сбрасывается кеш."""

    flight_id: str
    origin: str
    destination: str
    departure_dates: tuple[date, ...]


@dataclass(frozen=True)
class ReservationResult:
    reservation: RowMapping
    change: FlightChange | None  # None — повтор, данные не менялись
    available_seats: int

    @property
    def replayed(self) -> bool:
        return self.change is None


def utc_date(value: datetime) -> date:
    return value.astimezone(timezone.utc).date()


def _change(flight: RowMapping, *extra_dates: date) -> FlightChange:
    return FlightChange(
        flight_id=str(flight["id"]),
        origin=flight["origin"],
        destination=flight["destination"],
        departure_dates=(utc_date(flight["departure_time"]), *extra_dates),
    )


def _constraint(exc: IntegrityError) -> str | None:
    return getattr(getattr(exc.orig, "__cause__", None), "constraint_name", None)


async def _lock_flight(conn: AsyncConnection, flight_id: uuid.UUID) -> RowMapping:
    row = (
        await conn.execute(select(flights).where(flights.c.id == flight_id).with_for_update())
    ).mappings().first()
    if row is None:
        raise flight_not_found(flight_id)
    return row


class FlightStore:
    def __init__(self, engine: AsyncEngine, search_limit: int) -> None:
        self._engine = engine
        self._search_limit = search_limit

    async def get_flight(self, flight_id: uuid.UUID) -> RowMapping | None:
        async with self._engine.connect() as conn:
            return (await conn.execute(FLIGHT_VIEW.where(flights.c.id == flight_id))).mappings().first()

    async def search(self, origin: str, destination: str, departure_date: date | None) -> list[RowMapping]:
        conditions = [
            flights.c.origin == origin,
            flights.c.destination == destination,
            flights.c.status == "SCHEDULED",
        ]
        if departure_date is not None:
            day_start = datetime.combine(departure_date, time.min, tzinfo=timezone.utc)
            conditions += [flights.c.departure_time >= day_start, flights.c.departure_time < day_start + timedelta(days=1)]
        query = FLIGHT_VIEW.where(and_(*conditions)).order_by(flights.c.departure_time, flights.c.id)
        if departure_date is None:
            query = query.limit(self._search_limit)
        async with self._engine.connect() as conn:
            return list((await conn.execute(query)).mappings().all())

    async def reserve(
        self,
        flight_id: uuid.UUID,
        booking_id: uuid.UUID,
        seat_count: int,
        expected_price: tuple[Decimal, str] | None,
    ) -> ReservationResult:
        try:
            async with self._engine.begin() as conn:
                flight = await _lock_flight(conn, flight_id)

                # Идемпотентность: проверка под блокировкой рейса, поэтому параллельный повтор
                # с тем же booking_id ждёт первый вызов и видит созданную им резервацию.
                existing = (
                    await conn.execute(select(seat_reservations).where(seat_reservations.c.booking_id == booking_id))
                ).mappings().first()
                if existing is not None:
                    if existing["flight_id"] != flight_id or existing["seat_count"] != seat_count:
                        raise FlightError(
                            grpc.StatusCode.ALREADY_EXISTS, "BOOKING_ID_CONFLICT",
                            f"booking {booking_id} already has a reservation with other parameters",
                            booking_id=booking_id, flight_id=existing["flight_id"], seat_count=existing["seat_count"],
                        )
                    if existing["status"] != "ACTIVE":
                        raise FlightError(
                            grpc.StatusCode.FAILED_PRECONDITION, "RESERVATION_NOT_ACTIVE",
                            f"reservation of booking {booking_id} is {existing['status']}",
                            booking_id=booking_id, status=existing["status"],
                        )
                    return ReservationResult(existing, None, flight["available_seats"])

                if flight["status"] != "SCHEDULED" or flight["departure_time"] <= datetime.now(timezone.utc):
                    raise FlightError(
                        grpc.StatusCode.FAILED_PRECONDITION, "FLIGHT_NOT_BOOKABLE",
                        f"flight {flight_id} is {flight['status']} and cannot be booked",
                        flight_id=flight_id, status=flight["status"],
                    )
                if expected_price is not None and expected_price != (flight["price"], flight["currency"]):
                    raise FlightError(
                        grpc.StatusCode.ABORTED, "PRICE_CHANGED",
                        f"flight price is {flight['price']} {flight['currency']}, "
                        f"expected {expected_price[0]} {expected_price[1]}",
                        current_price=flight["price"], currency=flight["currency"],
                        expected_price=expected_price[0], expected_currency=expected_price[1],
                    )
                if flight["available_seats"] < seat_count:
                    raise FlightError(
                        grpc.StatusCode.RESOURCE_EXHAUSTED, "NOT_ENOUGH_SEATS",
                        f"requested {seat_count} seats, {flight['available_seats']} available",
                        requested=seat_count, available=flight["available_seats"],
                    )

                await conn.execute(
                    update(flights)
                    .where(flights.c.id == flight_id)
                    .values(available_seats=flights.c.available_seats - seat_count)
                )
                reservation = (
                    await conn.execute(
                        insert(seat_reservations)
                        .values(flight_id=flight_id, booking_id=booking_id, seat_count=seat_count, status="ACTIVE")
                        .returning(*seat_reservations.c)
                    )
                ).mappings().one()
        except IntegrityError as exc:
            # Параллельный ReserveSeats с тем же booking_id, но другим рейсом: блокировки разных рейсов
            # не упорядочивают такие вызовы, их разделяет UNIQUE (booking_id).
            if _constraint(exc) == "seat_reservations_booking_id_key":
                raise FlightError(
                    grpc.StatusCode.ALREADY_EXISTS, "BOOKING_ID_CONFLICT",
                    f"booking {booking_id} already has a reservation", booking_id=booking_id,
                ) from None
            raise
        return ReservationResult(reservation, _change(flight), flight["available_seats"] - seat_count)

    async def release(self, booking_id: uuid.UUID) -> ReservationResult:
        async with self._engine.begin() as conn:
            flight_id = (
                await conn.execute(
                    select(seat_reservations.c.flight_id).where(seat_reservations.c.booking_id == booking_id)
                )
            ).scalar_one_or_none()
            if flight_id is None:
                raise FlightError(
                    grpc.StatusCode.NOT_FOUND, "RESERVATION_NOT_FOUND",
                    f"no reservation for booking {booking_id}", booking_id=booking_id,
                )
            # Порядок блокировок тот же, что в reserve: сначала рейс, потом резервация.
            flight = await _lock_flight(conn, flight_id)
            reservation = (
                await conn.execute(
                    select(seat_reservations).where(seat_reservations.c.booking_id == booking_id).with_for_update()
                )
            ).mappings().one()

            if reservation["status"] == "RELEASED":
                return ReservationResult(reservation, None, flight["available_seats"])
            if reservation["status"] != "ACTIVE":
                raise FlightError(
                    grpc.StatusCode.FAILED_PRECONDITION, "RESERVATION_NOT_ACTIVE",
                    f"reservation of booking {booking_id} is {reservation['status']}",
                    booking_id=booking_id, status=reservation["status"],
                )
            if flight["status"] in ("DEPARTED", "COMPLETED"):
                raise FlightError(
                    grpc.StatusCode.FAILED_PRECONDITION, "FLIGHT_ALREADY_DEPARTED",
                    f"flight {flight_id} is {flight['status']}, seats cannot be released",
                    flight_id=flight_id, status=flight["status"],
                )

            await conn.execute(
                update(flights)
                .where(flights.c.id == flight_id)
                .values(available_seats=flights.c.available_seats + reservation["seat_count"])
            )
            reservation = (
                await conn.execute(
                    update(seat_reservations)
                    .where(seat_reservations.c.id == reservation["id"])
                    .values(status="RELEASED")
                    .returning(*seat_reservations.c)
                )
            ).mappings().one()
        return ReservationResult(reservation, _change(flight), flight["available_seats"] + reservation["seat_count"])

    async def update(
        self,
        flight_id: uuid.UUID,
        status: str | None,
        departure_time: datetime | None,
        arrival_time: datetime | None,
        price: tuple[Decimal, str] | None,
    ) -> tuple[RowMapping, FlightChange | None]:
        try:
            async with self._engine.begin() as conn:
                flight = await _lock_flight(conn, flight_id)
                values: dict[str, Any] = {}

                if status is not None and status != flight["status"]:
                    if status not in FLIGHT_TRANSITIONS.get(flight["status"], set()):
                        raise FlightError(
                            grpc.StatusCode.FAILED_PRECONDITION, "INVALID_STATUS_TRANSITION",
                            f"flight status cannot change from {flight['status']} to {status}",
                            current=flight["status"], requested=status,
                        )
                    values["status"] = status

                if (departure_time or arrival_time or price) and flight["status"] != "SCHEDULED":
                    raise FlightError(
                        grpc.StatusCode.FAILED_PRECONDITION, "FLIGHT_NOT_BOOKABLE",
                        f"only SCHEDULED flights can be rescheduled or repriced, flight is {flight['status']}",
                        status=flight["status"],
                    )
                if departure_time or arrival_time:
                    new_departure = departure_time or flight["departure_time"]
                    new_arrival = arrival_time or flight["arrival_time"]
                    if new_arrival <= new_departure:
                        raise invalid_argument("arrival_time must be after departure_time")
                    values["departure_time"] = new_departure
                    values["arrival_time"] = new_arrival
                if price is not None:
                    amount, currency = price
                    if currency != flight["currency"]:
                        raise invalid_argument(
                            f"flight is priced in {flight['currency']}; currency change is not supported",
                            field="price",
                        )
                    values["price"] = amount

                if values:
                    await conn.execute(update(flights).where(flights.c.id == flight_id).values(**values))
                view = (await conn.execute(FLIGHT_VIEW.where(flights.c.id == flight_id))).mappings().one()
        except IntegrityError as exc:
            if _constraint(exc) == "uq_flights_number_departure_date":
                raise FlightError(
                    grpc.StatusCode.ALREADY_EXISTS, "FLIGHT_ALREADY_EXISTS",
                    "another flight with this number departs on the same date",
                ) from None
            raise

        if not values:
            return view, None
        return view, _change(flight, utc_date(view["departure_time"]))
