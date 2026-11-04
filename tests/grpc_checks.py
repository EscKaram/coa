"""Проверки gRPC-контракта Flight Service напрямую, в обход Booking Service.

Запускается внутри контейнера booking-service: там есть grpcio и сгенерированные стабы.
    docker compose exec -T booking-service python - < tests/grpc_checks.py
Вызывает его tests/e2e.py. Аргумент (через переменную окружения FLIGHT_ID) — UUID рейса
с запасом свободных мест, который не используют другие проверки.
"""

import os
import sys
import uuid

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from google.rpc import error_details_pb2, status_pb2
from google.type import date_pb2, money_pb2

from flight.v1 import flight_service_pb2 as pb
from flight.v1 import flight_service_pb2_grpc as pb_grpc

ADDR = os.environ.get("FLIGHT_SERVICE_ADDR", "flight-service:50051")
API_KEY = os.environ["FLIGHT_SERVICE_API_KEY"]
FLIGHT_ID = os.environ["FLIGHT_ID"]
AUTH = (("x-api-key", API_KEY),)

passed = 0


def ok(name: str) -> None:
    global passed
    passed += 1
    print(f"ok   grpc: {name}")


def fail(name: str, detail: object) -> None:
    print(f"FAIL grpc: {name}: {detail}")
    sys.exit(1)


def reason_of(exc: grpc.RpcError) -> str | None:
    for key, value in exc.trailing_metadata() or ():
        if key == "grpc-status-details-bin":
            for detail in status_pb2.Status.FromString(value).details:
                info = error_details_pb2.ErrorInfo()
                if detail.Unpack(info):
                    return info.reason
    return None


def expect_error(name: str, fn, code: grpc.StatusCode, reason: str | None = None) -> None:
    try:
        fn()
    except grpc.RpcError as exc:
        if exc.code() != code or (reason is not None and reason_of(exc) != reason):
            fail(name, f"got {exc.code().name} {reason_of(exc)}: {exc.details()}")
        ok(f"{name} -> {code.name}{' ' + reason if reason else ''}")
        return
    fail(name, f"expected {code.name}, got OK")


def main() -> None:
    with grpc.insecure_channel(ADDR) as channel:
        stub = pb_grpc.FlightServiceStub(channel)
        get = pb.GetFlightRequest(flight_id=FLIGHT_ID)

        # --- аутентификация (п. 6) ---
        expect_error("GetFlight without api key", lambda: stub.GetFlight(get), grpc.StatusCode.UNAUTHENTICATED)
        expect_error(
            "GetFlight with wrong api key",
            lambda: stub.GetFlight(get, metadata=(("x-api-key", "wrong"),)),
            grpc.StatusCode.UNAUTHENTICATED,
        )
        expect_error(
            "SearchFlights without api key",
            lambda: stub.SearchFlights(pb.SearchFlightsRequest(origin="SVO", destination="LED")),
            grpc.StatusCode.UNAUTHENTICATED,
        )
        expect_error(
            "ReserveSeats without api key",
            lambda: stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=FLIGHT_ID, booking_id=str(uuid.uuid4()), seat_count=1)),
            grpc.StatusCode.UNAUTHENTICATED,
        )
        flight = stub.GetFlight(get, metadata=AUTH).flight
        ok(f"GetFlight with api key -> {flight.flight_number}, available {flight.available_seats}")

        # --- коды ошибок контракта (п. 1) ---
        expect_error(
            "GetFlight unknown id",
            lambda: stub.GetFlight(pb.GetFlightRequest(flight_id=str(uuid.uuid4())), metadata=AUTH),
            grpc.StatusCode.NOT_FOUND, "FLIGHT_NOT_FOUND",
        )
        expect_error(
            "GetFlight malformed id",
            lambda: stub.GetFlight(pb.GetFlightRequest(flight_id="42"), metadata=AUTH),
            grpc.StatusCode.INVALID_ARGUMENT, "INVALID_ARGUMENT",
        )
        expect_error(
            "SearchFlights invalid date",
            lambda: stub.SearchFlights(
                pb.SearchFlightsRequest(origin="SVO", destination="LED", departure_date=date_pb2.Date(year=2026, month=2, day=30)),
                metadata=AUTH,
            ),
            grpc.StatusCode.INVALID_ARGUMENT,
        )
        expect_error(
            "ReserveSeats too many seats",
            lambda: stub.ReserveSeats(
                pb.ReserveSeatsRequest(flight_id=FLIGHT_ID, booking_id=str(uuid.uuid4()), seat_count=flight.total_seats + 1),
                metadata=AUTH,
            ),
            grpc.StatusCode.RESOURCE_EXHAUSTED, "NOT_ENOUGH_SEATS",
        )
        expect_error(
            "ReserveSeats with stale expected_price",
            lambda: stub.ReserveSeats(
                pb.ReserveSeatsRequest(
                    flight_id=FLIGHT_ID, booking_id=str(uuid.uuid4()), seat_count=1,
                    expected_price=money_pb2.Money(currency_code=flight.price.currency_code, units=flight.price.units + 1),
                ),
                metadata=AUTH,
            ),
            grpc.StatusCode.ABORTED, "PRICE_CHANGED",
        )
        expect_error(
            "ReleaseReservation unknown booking",
            lambda: stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=str(uuid.uuid4())), metadata=AUTH),
            grpc.StatusCode.NOT_FOUND, "RESERVATION_NOT_FOUND",
        )

        # --- идемпотентность ReserveSeats (п. 8) ---
        booking_id = str(uuid.uuid4())
        request = pb.ReserveSeatsRequest(flight_id=FLIGHT_ID, booking_id=booking_id, seat_count=2)
        first = stub.ReserveSeats(request, metadata=AUTH)
        second = stub.ReserveSeats(request, metadata=AUTH)
        after = stub.GetFlight(get, metadata=AUTH).flight.available_seats
        if first.replayed or not second.replayed or first.reservation.id != second.reservation.id:
            fail("ReserveSeats replay", f"first={first} second={second}")
        if after != flight.available_seats - 2:
            fail("ReserveSeats replay", f"available {flight.available_seats} -> {after}, expected -2")
        ok(f"ReserveSeats twice with same booking_id -> one reservation {first.reservation.id}, seats -2 once")
        expect_error(
            "ReserveSeats same booking_id, other seat_count",
            lambda: stub.ReserveSeats(
                pb.ReserveSeatsRequest(flight_id=FLIGHT_ID, booking_id=booking_id, seat_count=3), metadata=AUTH
            ),
            grpc.StatusCode.ALREADY_EXISTS, "BOOKING_ID_CONFLICT",
        )

        # --- ReleaseReservation и его идемпотентность ---
        released = stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=AUTH)
        again = stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=AUTH)
        after_release = stub.GetFlight(get, metadata=AUTH).flight.available_seats
        if released.reservation.status != pb.RESERVATION_STATUS_RELEASED or released.replayed or not again.replayed:
            fail("ReleaseReservation", f"{released} / {again}")
        if after_release != flight.available_seats:
            fail("ReleaseReservation", f"available {after_release}, expected {flight.available_seats}")
        ok("ReleaseReservation twice -> RELEASED, seats returned once")
        expect_error(
            "ReserveSeats for released booking_id",
            lambda: stub.ReserveSeats(request, metadata=AUTH),
            grpc.StatusCode.FAILED_PRECONDITION, "RESERVATION_NOT_ACTIVE",
        )

        # --- UpdateFlight: инвалидация кеша (п. 7) ---
        stub.GetFlight(get, metadata=AUTH)  # прогреть flight:{id}
        old_price = flight.price
        new_price = money_pb2.Money(currency_code=old_price.currency_code, units=old_price.units + 100, nanos=old_price.nanos)
        updated = stub.UpdateFlight(pb.UpdateFlightRequest(flight_id=FLIGHT_ID, price=new_price), metadata=AUTH).flight
        fresh = stub.GetFlight(get, metadata=AUTH).flight
        if updated.price != new_price or fresh.price != new_price:
            fail("UpdateFlight price", f"updated={updated.price} fresh={fresh.price}")
        ok("UpdateFlight price -> GetFlight returns new price (cache invalidated)")
        stub.UpdateFlight(pb.UpdateFlightRequest(flight_id=FLIGHT_ID, price=old_price), metadata=AUTH)
        expect_error(
            "UpdateFlight other currency",
            lambda: stub.UpdateFlight(
                pb.UpdateFlightRequest(flight_id=FLIGHT_ID, price=money_pb2.Money(currency_code="USD", units=10)),
                metadata=AUTH,
            ),
            grpc.StatusCode.INVALID_ARGUMENT,
        )
        bad_arrival = Timestamp()
        bad_arrival.FromDatetime(flight.departure_time.ToDatetime())
        expect_error(
            "UpdateFlight arrival == departure",
            lambda: stub.UpdateFlight(pb.UpdateFlightRequest(flight_id=FLIGHT_ID, arrival_time=bad_arrival), metadata=AUTH),
            grpc.StatusCode.INVALID_ARGUMENT,
        )
        expect_error(
            "UpdateFlight SCHEDULED -> COMPLETED",
            lambda: stub.UpdateFlight(
                pb.UpdateFlightRequest(flight_id=FLIGHT_ID, status=pb.FLIGHT_STATUS_COMPLETED), metadata=AUTH
            ),
            grpc.StatusCode.FAILED_PRECONDITION, "INVALID_STATUS_TRANSITION",
        )

    print(f"grpc checks passed: {passed}")


if __name__ == "__main__":
    main()
