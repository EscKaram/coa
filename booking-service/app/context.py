"""Контекст текущего HTTP-запроса, доступный в интерцепторах gRPC."""

from contextvars import ContextVar

# ID текущего HTTP-запроса; выставляет RequestLoggingMiddleware, читает MetadataInterceptor.
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)
