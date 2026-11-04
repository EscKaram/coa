"""gRPC-реализация FlightService: разбор запроса, кеш, вызов FlightStore, сборка ответа."""

import functools
import logging
import time

import grpc
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError

from app import convert
from app.cache import FlightCache, flight_generation_key, flight_key, search_generation_key, search_key
from app.errors import FlightError, invalid_argument
from app.logging_setup import log_event
from app.store import FlightChange, FlightStore
from flight.v1 import flight_service_pb2 as pb
from flight.v1 import flight_service_pb2_grpc as pb_grpc

logger = logging.getLogger("flight.rpc")

MAX_SEAT_COUNT = 1000


def _metadata_value(context: grpc.aio.ServicerContext, key: str) -> str | None:
    for k, v in context.invocation_metadata() or ():
        if k == key:
            return v
    return None


def _is_unavailable(exc: Exception) -> bool:
    """БД недоступна: клиенту отвечаем UNAVAILABLE, такой вызов безопасно повторить."""
    if isinstance(exc, (OSError, TimeoutError, OperationalError, InterfaceError)):
        return True
    return isinstance(exc, DBAPIError) and exc.connection_invalidated


def rpc(method):
    """Перевод исключений в статусы gRPC и одна строка лога на вызов (метод, код, длительность в мкс)."""

    @functools.wraps(method)
    async def wrapper(self, request, context: grpc.aio.ServicerContext):
        started_ns = time.perf_counter_ns()
        code, reason = grpc.StatusCode.CANCELLED, None
        try:
            response = await method(self, request, context)
            code = grpc.StatusCode.OK
            return response
        except FlightError as exc:
            code, reason = exc.code, exc.reason
            await context.abort(exc.code, exc.message, trailing_metadata=exc.trailing_metadata())
        except Exception as exc:
            if _is_unavailable(exc):
                code = grpc.StatusCode.UNAVAILABLE
                logger.warning("database unavailable in %s: %r", method.__name__, exc)
                await context.abort(code, "flight database is unavailable")
            code = grpc.StatusCode.INTERNAL
            logger.exception("unhandled error in %s", method.__name__)
            await context.abort(code, "internal error")
        finally:
            log_event(
                logger, logging.INFO, "rpc", method=method.__name__, code=code.name, reason=reason,
                duration_us=(time.perf_counter_ns() - started_ns) // 1000,
                request_id=_metadata_value(context, "x-request-id"),
            )

    return wrapper


class FlightServicer(pb_grpc.FlightServiceServicer):
    def __init__(self, store: FlightStore, cache: FlightCache) -> None:
        self._store = store
        self._cache = cache

    async def _invalidate(self, change: FlightChange | None) -> None:
        if change is not None:
            await self._cache.invalidate_flight(
                change.flight_id, change.origin, change.destination, change.departure_dates
            )

    @rpc
    async def SearchFlights(self, request: pb.SearchFlightsRequest, context) -> pb.SearchFlightsResponse:
        origin = convert.parse_airport(request.origin, "origin")
        destination = convert.parse_airport(request.destination, "destination")
        if origin == destination:
            raise invalid_argument("origin and destination must differ")
        departure_date = (
            convert.parse_date(request.departure_date, "departure_date") if request.HasField("departure_date") else None
        )

        async def load() -> bytes:
            rows = await self._store.search(origin, destination, departure_date)
            return pb.SearchFlightsResponse(flights=[convert.flight_to_pb(r) for r in rows]).SerializeToString()

        data = await self._cache.get_or_load(
            search_key(origin, destination, departure_date), search_generation_key(origin, destination), load
        )
        return pb.SearchFlightsResponse.FromString(data)

    @rpc
    async def GetFlight(self, request: pb.GetFlightRequest, context) -> pb.GetFlightResponse:
        flight_id = convert.parse_uuid(request.flight_id, "flight_id")

        async def load() -> bytes:
            row = await self._store.get_flight(flight_id)
            if row is None:
                raise FlightError(
                    grpc.StatusCode.NOT_FOUND, "FLIGHT_NOT_FOUND", f"flight {flight_id} not found", flight_id=flight_id
                )
            return convert.flight_to_pb(row).SerializeToString()

        data = await self._cache.get_or_load(flight_key(str(flight_id)), flight_generation_key(str(flight_id)), load)
        return pb.GetFlightResponse(flight=pb.Flight.FromString(data))

    @rpc
    async def ReserveSeats(self, request: pb.ReserveSeatsRequest, context) -> pb.ReserveSeatsResponse:
        flight_id = convert.parse_uuid(request.flight_id, "flight_id")
        booking_id = convert.parse_uuid(request.booking_id, "booking_id")
        if not 1 <= request.seat_count <= MAX_SEAT_COUNT:
            raise invalid_argument(f"seat_count must be in [1, {MAX_SEAT_COUNT}]", field="seat_count")
        expected_price = (
            convert.parse_money(request.expected_price, "expected_price") if request.HasField("expected_price") else None
        )

        result = await self._store.reserve(flight_id, booking_id, request.seat_count, expected_price)
        await self._invalidate(result.change)
        log_event(
            logger, logging.INFO, "reservation_replayed" if result.replayed else "seats_reserved",
            flight_id=str(flight_id), booking_id=str(booking_id), seat_count=request.seat_count,
            available_seats=result.available_seats,
        )
        return pb.ReserveSeatsResponse(
            reservation=convert.reservation_to_pb(result.reservation), replayed=result.replayed
        )

    @rpc
    async def ReleaseReservation(self, request: pb.ReleaseReservationRequest, context) -> pb.ReleaseReservationResponse:
        booking_id = convert.parse_uuid(request.booking_id, "booking_id")

        result = await self._store.release(booking_id)
        await self._invalidate(result.change)
        log_event(
            logger, logging.INFO, "release_replayed" if result.replayed else "seats_released",
            flight_id=str(result.reservation["flight_id"]), booking_id=str(booking_id),
            seat_count=result.reservation["seat_count"], available_seats=result.available_seats,
        )
        return pb.ReleaseReservationResponse(
            reservation=convert.reservation_to_pb(result.reservation), replayed=result.replayed
        )

    @rpc
    async def UpdateFlight(self, request: pb.UpdateFlightRequest, context) -> pb.UpdateFlightResponse:
        flight_id = convert.parse_uuid(request.flight_id, "flight_id")
        status = None
        if request.HasField("status"):
            status = convert.FLIGHT_STATUS_FROM_PB.get(request.status)
            if status is None:
                raise invalid_argument("status must be a concrete FlightStatus", field="status")
        departure_time = (
            convert.parse_timestamp(request.departure_time, "departure_time")
            if request.HasField("departure_time") else None
        )
        arrival_time = (
            convert.parse_timestamp(request.arrival_time, "arrival_time") if request.HasField("arrival_time") else None
        )
        price = convert.parse_money(request.price, "price") if request.HasField("price") else None
        if status is None and departure_time is None and arrival_time is None and price is None:
            raise invalid_argument("at least one of status, departure_time, arrival_time, price is required")

        view, change = await self._store.update(flight_id, status, departure_time, arrival_time, price)
        await self._invalidate(change)
        log_event(logger, logging.INFO, "flight_updated", flight_id=str(flight_id), changed=change is not None)
        return pb.UpdateFlightResponse(flight=convert.flight_to_pb(view))
