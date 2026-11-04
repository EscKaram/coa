"""Единый формат ошибок REST API {error_code, message, details} и перевод ошибок gRPC в HTTP."""

import logging
import math
from typing import Any

import grpc
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.circuit_breaker import CircuitOpenError
from app.flight_client import error_info

logger = logging.getLogger("booking.errors")


class ApiError(Exception):
    def __init__(
        self,
        status_code: int,
        error_code: str,
        message: str,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.details = details
        self.headers = headers


def error_response(error: ApiError) -> JSONResponse:
    body = {"error_code": error.error_code, "message": error.message, "details": error.details}
    return JSONResponse(status_code=error.status_code, content=body, headers=error.headers)


def _int_values(metadata: dict[str, str]) -> dict[str, Any]:
    return {k: int(v) if v.lstrip("-").isdigit() else v for k, v in metadata.items()}


# Бизнес-ошибки Flight Service: код gRPC -> HTTP-статус и код ошибки по умолчанию.
# Если в ErrorInfo есть reason, error_code берётся из него.
_BUSINESS_ERRORS = {
    grpc.StatusCode.INVALID_ARGUMENT: (400, "VALIDATION_ERROR"),
    grpc.StatusCode.NOT_FOUND: (404, "FLIGHT_NOT_FOUND"),
    grpc.StatusCode.RESOURCE_EXHAUSTED: (409, "NOT_ENOUGH_SEATS"),
    grpc.StatusCode.FAILED_PRECONDITION: (409, "FLIGHT_NOT_BOOKABLE"),
    grpc.StatusCode.ABORTED: (409, "PRICE_CHANGED"),
    grpc.StatusCode.ALREADY_EXISTS: (409, "BOOKING_ID_CONFLICT"),
}


def flight_service_error(exc: grpc.aio.AioRpcError) -> ApiError:
    if isinstance(exc, CircuitOpenError):
        retry_after = max(1, math.ceil(exc.retry_after_seconds))
        return ApiError(
            503, "FLIGHT_SERVICE_UNAVAILABLE",
            "Flight Service is temporarily unavailable: circuit breaker is OPEN",
            {"circuit_breaker": "OPEN", "retry_after_seconds": retry_after},
            {"Retry-After": str(retry_after)},
        )

    code = exc.code()
    if code in _BUSINESS_ERRORS:
        status, default_code = _BUSINESS_ERRORS[code]
        reason, metadata = error_info(exc)
        if code is grpc.StatusCode.INVALID_ARGUMENT:
            reason = "VALIDATION_ERROR"
        return ApiError(status, reason or default_code, exc.details() or code.name, _int_values(metadata) or None)
    if code is grpc.StatusCode.UNAVAILABLE:
        return ApiError(503, "FLIGHT_SERVICE_UNAVAILABLE", "Flight Service is unavailable", {"grpc_code": code.name})
    if code is grpc.StatusCode.DEADLINE_EXCEEDED:
        return ApiError(504, "FLIGHT_SERVICE_TIMEOUT", "Flight Service did not respond in time", {"grpc_code": code.name})
    if code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED):
        logger.error("Flight Service rejected credentials (%s): check FLIGHT_SERVICE_API_KEY", code.name)
    return ApiError(502, "FLIGHT_SERVICE_ERROR", "Flight Service returned an error", {"grpc_code": code.name})


async def _handle_api_error(_: Request, exc: ApiError) -> JSONResponse:
    return error_response(exc)


async def _handle_grpc_error(_: Request, exc: grpc.aio.AioRpcError) -> JSONResponse:
    return error_response(flight_service_error(exc))


async def _handle_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    fields = [
        {
            "location": str(err["loc"][0]) if err.get("loc") else "unknown",
            "field": ".".join(str(p) for p in err.get("loc", ())[1:]),
            "message": err.get("msg"),
            "type": err.get("type"),
        }
        for err in exc.errors()
    ]
    return error_response(ApiError(400, "VALIDATION_ERROR", "Request validation failed", {"fields": fields}))


async def _handle_http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    if exc.status_code == 404:
        return error_response(ApiError(404, "NOT_FOUND", "Resource not found"))
    if exc.status_code == 405:
        return error_response(ApiError(405, "METHOD_NOT_ALLOWED", "Method not allowed"))
    return error_response(ApiError(exc.status_code, "HTTP_ERROR", str(exc.detail)))


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApiError, _handle_api_error)
    app.add_exception_handler(grpc.aio.AioRpcError, _handle_grpc_error)
    app.add_exception_handler(RequestValidationError, _handle_validation_error)
    app.add_exception_handler(StarletteHTTPException, _handle_http_error)
