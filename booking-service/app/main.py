"""Точка входа Booking Service: REST API бронирований поверх Flight Service (gRPC)."""

import uuid
from contextlib import asynccontextmanager
from datetime import date
from typing import Annotated

from fastapi import FastAPI, Query, Request

from app.bookings import BookingService, booking_not_found
from app.config import load_settings
from app.db import Database
from app.errors import ApiError, register_exception_handlers
from app.flight_client import FlightClient
from app.logging_setup import configure_logging
from app.request_logging import RequestLoggingMiddleware
from app.schemas import USER_ID_PATTERN, BookingCreate, BookingOut, ErrorOut, FlightOut

configure_logging()

IATA = r"^[A-Za-z]{3}$"
ERRORS = {code: {"model": ErrorOut} for code in (400, 404, 409, 502, 503, 504)}


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    db = Database(settings.database_url)
    flights = FlightClient(settings)
    app.state.flights = flights
    app.state.bookings = BookingService(db, flights)
    yield
    await flights.close()
    await db.engine.dispose()


app = FastAPI(title="Booking Service", version="1.0.0", lifespan=lifespan)
register_exception_handlers(app)
app.add_middleware(RequestLoggingMiddleware)


def _uuid_or_404(value: str, not_found: ApiError) -> uuid.UUID:
    """ID в пути: не UUID — такого ресурса заведомо нет, отвечаем 404, а не 400."""
    try:
        return uuid.UUID(value)
    except ValueError:
        raise not_found from None


@app.get("/health")
async def health(request: Request) -> dict:
    return {"status": "ok", "flight_service_circuit_breaker": request.app.state.flights.breaker.state.value}


@app.get("/flights", response_model=list[FlightOut], responses=ERRORS)
async def search_flights(
    request: Request,
    origin: Annotated[str, Query(pattern=IATA, description="IATA-код аэропорта вылета")],
    destination: Annotated[str, Query(pattern=IATA, description="IATA-код аэропорта прилёта")],
    departure_date: Annotated[date | None, Query(alias="date", description="дата вылета (UTC), YYYY-MM-DD")] = None,
) -> list[FlightOut]:
    flights = await request.app.state.flights.search_flights(origin.upper(), destination.upper(), departure_date)
    return [FlightOut.from_pb(f) for f in flights]


@app.get("/flights/{flight_id}", response_model=FlightOut, responses=ERRORS)
async def get_flight(request: Request, flight_id: str) -> FlightOut:
    not_found = ApiError(404, "FLIGHT_NOT_FOUND", f"Flight {flight_id} not found")
    return FlightOut.from_pb(await request.app.state.flights.get_flight(_uuid_or_404(flight_id, not_found)))


@app.post("/bookings", status_code=201, response_model=BookingOut, responses=ERRORS)
async def create_booking(request: Request, body: BookingCreate) -> BookingOut:
    return BookingOut.from_row(await request.app.state.bookings.create(body))


@app.get("/bookings", response_model=list[BookingOut], responses=ERRORS)
async def list_bookings(
    request: Request,
    user_id: Annotated[str, Query(pattern=USER_ID_PATTERN)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=1_000_000)] = 0,
) -> list[BookingOut]:
    rows = await request.app.state.bookings.list_for_user(user_id, limit, offset)
    return [BookingOut.from_row(r) for r in rows]


@app.get("/bookings/{booking_id}", response_model=BookingOut, responses=ERRORS)
async def get_booking(request: Request, booking_id: str) -> BookingOut:
    row = await request.app.state.bookings.get(_uuid_or_404(booking_id, booking_not_found(booking_id)))
    return BookingOut.from_row(row)


@app.post("/bookings/{booking_id}/cancel", response_model=BookingOut, responses=ERRORS)
async def cancel_booking(request: Request, booking_id: str) -> BookingOut:
    row = await request.app.state.bookings.cancel(_uuid_or_404(booking_id, booking_not_found(booking_id)))
    return BookingOut.from_row(row)
