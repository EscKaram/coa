"""Клиент Flight Service: gRPC-канал с интерцепторами и типизированные вызовы."""

import uuid
from datetime import date
from decimal import Decimal

import grpc
from google.rpc import error_details_pb2, status_pb2
from google.type import date_pb2, money_pb2

from app.circuit_breaker import CircuitBreaker
from app.config import Settings
from app.interceptors import CircuitBreakerInterceptor, MetadataInterceptor, RetryInterceptor
from flight.v1 import flight_service_pb2 as pb
from flight.v1 import flight_service_pb2_grpc as pb_grpc

STATUS_DETAILS_KEY = "grpc-status-details-bin"
NANOS_PER_UNIT = Decimal(1_000_000_000)


def money_to_decimal(value: money_pb2.Money) -> Decimal:
    return Decimal(value.units) + Decimal(value.nanos) / NANOS_PER_UNIT


def error_info(exc: grpc.aio.AioRpcError) -> tuple[str | None, dict[str, str]]:
    """reason и metadata из google.rpc.ErrorInfo в деталях статуса (см. flight_service.proto)."""
    for key, value in exc.trailing_metadata() or ():
        if key != STATUS_DETAILS_KEY:
            continue
        status = status_pb2.Status.FromString(value)
        for detail in status.details:
            if detail.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                info = error_details_pb2.ErrorInfo()
                detail.Unpack(info)
                return info.reason, dict(info.metadata)
    return None, {}


class FlightClient:
    def __init__(self, settings: Settings) -> None:
        self.breaker = CircuitBreaker(
            "flight-service",
            failure_threshold=settings.cb_failure_threshold,
            window_seconds=settings.cb_window_seconds,
            open_timeout_seconds=settings.cb_open_timeout_seconds,
        )
        self._channel = grpc.aio.insecure_channel(
            settings.flight_service_addr,
            interceptors=[
                MetadataInterceptor(settings.flight_service_api_key),
                CircuitBreakerInterceptor(self.breaker),
                RetryInterceptor(
                    max_attempts=settings.retry_max_attempts,
                    initial_backoff_s=settings.retry_initial_backoff_ms / 1000,
                    multiplier=settings.retry_backoff_multiplier,
                    attempt_timeout_s=settings.flight_rpc_timeout_ms / 1000,
                ),
            ],
        )
        self._stub = pb_grpc.FlightServiceStub(self._channel)

    async def close(self) -> None:
        await self._channel.close()

    async def search_flights(self, origin: str, destination: str, departure_date: date | None) -> list[pb.Flight]:
        request = pb.SearchFlightsRequest(origin=origin, destination=destination)
        if departure_date is not None:
            request.departure_date.CopyFrom(
                date_pb2.Date(year=departure_date.year, month=departure_date.month, day=departure_date.day)
            )
        return list((await self._stub.SearchFlights(request)).flights)

    async def get_flight(self, flight_id: uuid.UUID) -> pb.Flight:
        return (await self._stub.GetFlight(pb.GetFlightRequest(flight_id=str(flight_id)))).flight

    async def reserve_seats(
        self, flight_id: uuid.UUID, booking_id: uuid.UUID, seat_count: int, expected_price: money_pb2.Money
    ) -> pb.ReserveSeatsResponse:
        return await self._stub.ReserveSeats(
            pb.ReserveSeatsRequest(
                flight_id=str(flight_id), booking_id=str(booking_id), seat_count=seat_count,
                expected_price=expected_price,
            )
        )

    async def release_reservation(self, booking_id: uuid.UUID) -> pb.ReleaseReservationResponse:
        return await self._stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=str(booking_id)))
