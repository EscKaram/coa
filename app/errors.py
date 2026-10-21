"""Единый формат ошибок API (схема Error в OpenAPI) и обработчики исключений."""

import logging
import re
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from marketplace_api.models.error import Error
from marketplace_api.models.error_code import ErrorCode

logger = logging.getLogger(__name__)


class ApiError(Exception):
    """Бизнес-ошибка с HTTP-статусом и кодом из каталога ErrorCode."""

    def __init__(
        self, status_code: int, error_code: ErrorCode, message: str, details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.details = details


def validation_error(message: str, fields: list[dict[str, Any]]) -> ApiError:
    return ApiError(400, ErrorCode.VALIDATION_ERROR, message, {"fields": fields})


def access_denied(message: str = "Insufficient permissions") -> ApiError:
    return ApiError(403, ErrorCode.ACCESS_DENIED, message)


def error_response(
    status_code: int, error_code: ErrorCode, message: str, details: dict[str, Any] | None = None
) -> JSONResponse:
    body = Error(error_code=error_code, message=message, details=details)
    return JSONResponse(status_code=status_code, content=body.model_dump(mode="json"))


# Сгенерированные модели описывают decimal как Union[float, int]; pydantic добавляет имя ветки в loc.
_UNION_BRANCH = re.compile(r"^(constrained-)?(int|float|str|bool)$")


def _location_and_field(loc: tuple) -> tuple[str, str]:
    # loc вида ("body", "items", 0, "quantity") -> ("body", "items[0].quantity")
    location, *path = loc or ("unknown",)
    field = ""
    for part in path:
        if isinstance(part, str) and _UNION_BRANCH.match(part):
            continue
        field += f"[{part}]" if isinstance(part, int) else (f".{part}" if field else str(part))
    return str(location), field


async def _handle_api_error(_: Request, exc: ApiError) -> JSONResponse:
    return error_response(exc.status_code, exc.error_code, exc.message, exc.details)


async def _handle_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    fields: dict[tuple[str, str], dict[str, Any]] = {}
    for err in exc.errors():
        location, field = _location_and_field(tuple(err.get("loc", ())))
        if (location, field) in fields:
            # Одна ошибка на поле: для Union pydantic сообщает о каждой ветке отдельно.
            continue
        fields[(location, field)] = {
            "location": location,
            "field": field,
            "message": err.get("msg"),
            "type": err.get("type"),
        }
    return error_response(
        400, ErrorCode.VALIDATION_ERROR, "Request validation failed", {"fields": list(fields.values())}
    )


async def _handle_http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
    if exc.status_code == 404:
        return error_response(404, ErrorCode.NOT_FOUND, "Resource not found")
    if exc.status_code == 405:
        return error_response(405, ErrorCode.METHOD_NOT_ALLOWED, "Method not allowed")
    return error_response(exc.status_code, ErrorCode.INTERNAL_ERROR, str(exc.detail))


async def _handle_unexpected(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("unhandled error", exc_info=exc)
    return error_response(500, ErrorCode.INTERNAL_ERROR, "Internal server error")


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApiError, _handle_api_error)
    app.add_exception_handler(RequestValidationError, _handle_validation_error)
    app.add_exception_handler(StarletteHTTPException, _handle_http_error)
    app.add_exception_handler(Exception, _handle_unexpected)
