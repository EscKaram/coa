"""Лог каждого HTTP-запроса и X-Request-Id: в ответе и в gRPC metadata вызовов Flight Service."""

import logging
import re
import time
import uuid

from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.context import request_id_var
from app.logging_setup import log_event

logger = logging.getLogger("booking.request")

_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class RequestLoggingMiddleware:
    """Чистый ASGI-middleware. X-Request-Id берётся из запроса (если корректный) или генерируется.
    Необработанное исключение превращается здесь в 500 INTERNAL_ERROR, чтобы ответ тоже нёс X-Request-Id."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = dict(scope.get("headers") or ()).get(b"x-request-id", b"").decode("latin-1")
        request_id = incoming if _REQUEST_ID.match(incoming) else str(uuid.uuid4())
        token = request_id_var.set(request_id)
        status_code = 500
        response_started = False
        started_ns = time.perf_counter_ns()

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                MutableHeaders(scope=message).append("X-Request-Id", request_id)
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            logger.exception("unhandled error, request_id=%s", request_id)
            if response_started:
                raise
            body = {"error_code": "INTERNAL_ERROR", "message": "Internal server error", "details": None}
            await JSONResponse(status_code=500, content=body)(scope, receive, send_wrapper)
        finally:
            query = scope.get("query_string", b"").decode("latin-1")
            log_event(
                logger, logging.INFO, "http_request", request_id=request_id, method=scope["method"],
                path=scope["path"] + (f"?{query}" if query else ""), status_code=status_code,
                duration_us=(time.perf_counter_ns() - started_ns) // 1000,
            )
            request_id_var.reset(token)
