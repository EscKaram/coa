"""Бизнес-логика бронирований: создание (GetFlight -> ReserveSeats -> запись в БД) и отмена.

Целостность между двумя БД без распределённой транзакции:
- ID бронирования генерируется до ReserveSeats и служит ключом идемпотентности, поэтому
  повтор ReserveSeats (retry) не создаёт вторую резервацию.
- Ошибка ReserveSeats -> в БД ничего не пишется. Если исход неизвестен (UNAVAILABLE,
  DEADLINE_EXCEEDED, ... после всех повторов), резервация могла успеть создаться: вызывается
  компенсирующий ReleaseReservation (NOT_FOUND означает, что снимать нечего).
- Ошибка записи в БД после успешного ReserveSeats -> компенсация ReleaseReservation.
- Отмена: ReleaseReservation внутри транзакции, держащей строку бронирования FOR UPDATE.
  Ошибка Flight Service -> rollback, бронирование остаётся CONFIRMED. Ошибка commit после
  ReleaseReservation -> повторная отмена безопасна: ReleaseReservation идемпотентен.
Критические участки запускаются в asyncio.shield: обрыв HTTP-соединения клиентом
не прерывает их между резервированием и записью в БД.
"""

import asyncio
import logging
import uuid
from decimal import Decimal

import grpc
from sqlalchemy import insert, select, update

from app.circuit_breaker import CircuitOpenError
from app.db import Database, bookings
from app.errors import ApiError
from app.flight_client import FlightClient, error_info, money_to_decimal
from app.logging_setup import log_event
from app.schemas import BookingCreate
from flight.v1 import flight_service_pb2 as pb

logger = logging.getLogger("booking.bookings")

CENT = Decimal("0.01")
# Исход вызова неизвестен: запрос мог дойти до Flight Service и выполниться.
UNCERTAIN_OUTCOME = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.CANCELLED,
    }
)


def booking_not_found(booking_id: uuid.UUID | str) -> ApiError:
    return ApiError(404, "BOOKING_NOT_FOUND", f"Booking {booking_id} not found")


class BookingService:
    def __init__(self, db: Database, flights: FlightClient) -> None:
        self._db = db
        self._flights = flights

    async def create(self, data: BookingCreate):
        # 1. Рейс и цена.
        flight = await self._flights.get_flight(data.flight_id)
        if flight.status != pb.FLIGHT_STATUS_SCHEDULED:
            raise ApiError(
                409, "FLIGHT_NOT_BOOKABLE", f"Flight {flight.id} is not open for booking",
                {"status": pb.FlightStatus.Name(flight.status).removeprefix("FLIGHT_STATUS_")},
            )
        # 3. Снимок цены. Decimal: без ошибок округления float.
        total_price = (money_to_decimal(flight.price) * data.seat_count).quantize(CENT)
        booking_id = uuid.uuid4()
        return await asyncio.shield(self._reserve_and_store(booking_id, data, flight, total_price))

    async def _reserve_and_store(self, booking_id: uuid.UUID, data: BookingCreate, flight: pb.Flight, total_price: Decimal):
        # 2. Резервирование. expected_price: если цена изменилась после GetFlight, Flight Service
        # откажет (PRICE_CHANGED) и бронирование не зафиксирует устаревшую цену.
        try:
            await self._flights.reserve_seats(data.flight_id, booking_id, data.seat_count, flight.price)
        except CircuitOpenError:
            raise  # вызов не отправлялся
        except grpc.aio.AioRpcError as exc:
            if exc.code() in UNCERTAIN_OUTCOME:
                await self._compensate(booking_id, data.flight_id, reason=f"ReserveSeats {exc.code().name}")
            raise

        # 4. Запись бронирования.
        try:
            async with self._db.transaction() as conn:
                row = (
                    await conn.execute(
                        insert(bookings)
                        .values(
                            id=booking_id, user_id=data.user_id, flight_id=data.flight_id,
                            passenger_name=data.passenger_name, passenger_email=data.passenger_email,
                            seat_count=data.seat_count, total_price=total_price,
                            currency=flight.price.currency_code, status="CONFIRMED",
                        )
                        .returning(*bookings.c)
                    )
                ).mappings().one()
        except Exception:
            logger.exception("booking %s: insert failed after ReserveSeats, releasing reservation", booking_id)
            await self._compensate(booking_id, data.flight_id, reason="booking insert failed")
            raise
        log_event(
            logger, logging.INFO, "booking_created", booking_id=str(booking_id), flight_id=str(data.flight_id),
            seat_count=data.seat_count, total_price=str(total_price), currency=row["currency"],
        )
        return row

    async def _compensate(self, booking_id: uuid.UUID, flight_id: uuid.UUID, reason: str) -> None:
        """Снять резервацию бронирования, которое не будет создано."""
        try:
            await self._flights.release_reservation(booking_id)
            log_event(logger, logging.WARNING, "reservation_compensated", booking_id=str(booking_id), reason=reason)
        except grpc.aio.AioRpcError as exc:
            if exc.code() is grpc.StatusCode.NOT_FOUND:
                log_event(logger, logging.INFO, "reservation_compensation_not_needed", booking_id=str(booking_id), reason=reason)
                return
            # Места остаются занятыми резервацией без бронирования; нужна ручная сверка по booking_id.
            log_event(
                logger, logging.ERROR, "reservation_compensation_failed", booking_id=str(booking_id),
                flight_id=str(flight_id), grpc_code=exc.code().name, reason=reason,
            )

    async def get(self, booking_id: uuid.UUID):
        async with self._db.connection() as conn:
            row = (await conn.execute(select(bookings).where(bookings.c.id == booking_id))).mappings().first()
        if row is None:
            raise booking_not_found(booking_id)
        return row

    async def list_for_user(self, user_id: str, limit: int, offset: int):
        query = (
            select(bookings)
            .where(bookings.c.user_id == user_id)
            .order_by(bookings.c.created_at.desc(), bookings.c.id)
            .limit(limit)
            .offset(offset)
        )
        async with self._db.connection() as conn:
            return list((await conn.execute(query)).mappings().all())

    async def cancel(self, booking_id: uuid.UUID):
        return await asyncio.shield(self._cancel(booking_id))

    async def _cancel(self, booking_id: uuid.UUID):
        async with self._db.transaction() as conn:
            # 1. Блокировка строки: параллельные отмены одного бронирования идут по очереди.
            row = (
                await conn.execute(select(bookings).where(bookings.c.id == booking_id).with_for_update())
            ).mappings().first()
            if row is None:
                raise booking_not_found(booking_id)
            if row["status"] != "CONFIRMED":
                raise ApiError(
                    409, "BOOKING_NOT_CONFIRMED", f"Booking {booking_id} is {row['status']}, only CONFIRMED can be cancelled",
                    {"status": row["status"]},
                )

            # 2. Возврат мест. Ошибка -> исключение -> rollback, статус не меняется.
            try:
                await self._flights.release_reservation(booking_id)
            except grpc.aio.AioRpcError as exc:
                reason, _ = error_info(exc)
                if exc.code() is not grpc.StatusCode.NOT_FOUND or reason != "RESERVATION_NOT_FOUND":
                    raise
                # Резервации нет — возвращать нечего; бронирование всё равно отменяем.
                log_event(logger, logging.WARNING, "reservation_missing_on_cancel", booking_id=str(booking_id))

            # 3. Статус CANCELLED.
            row = (
                await conn.execute(
                    update(bookings).where(bookings.c.id == booking_id).values(status="CANCELLED").returning(*bookings.c)
                )
            ).mappings().one()
        log_event(logger, logging.INFO, "booking_cancelled", booking_id=str(booking_id))
        return row
