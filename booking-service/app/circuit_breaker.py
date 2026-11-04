"""Circuit Breaker для вызовов Flight Service: конечный автомат CLOSED -> OPEN -> HALF_OPEN.

CLOSED     вызовы проходят. failure_threshold отказов подряд, уложившихся в window_seconds,
           переводят автомат в OPEN. Успешный вызов сбрасывает счётчик.
OPEN       вызовы сразу отклоняются CircuitOpenError, без обращения к сети.
           Через open_timeout_seconds следующий вызов переводит автомат в HALF_OPEN.
HALF_OPEN  проходит ровно один пробный вызов, остальные отклоняются, пока он не завершится.
           Успех -> CLOSED, отказ -> снова OPEN на open_timeout_seconds.

Отказом считается только недоступность сервиса (коды FAILURE_CODES). Бизнес-ошибки
(NOT_FOUND, RESOURCE_EXHAUSTED, ...) означают, что сервис работает, и считаются успехом.

Используется из одного event loop: методы не содержат await, поэтому переходы атомарны без блокировок.
Память O(failure_threshold): хранятся моменты последних отказов, не больше порога.
"""

import enum
import logging
import time
from collections import deque
from collections.abc import Callable

import grpc

from app.logging_setup import log_event

logger = logging.getLogger("booking.circuit_breaker")

FAILURE_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
    }
)


class State(str, enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(grpc.aio.AioRpcError):
    """Вызов отклонён без обращения к сети. Это AioRpcError с кодом UNAVAILABLE, чтобы
    вызывающий код обрабатывал его как обычную ошибку gRPC, но мог отличить по типу."""

    def __init__(self, name: str, retry_after_seconds: float) -> None:
        self.retry_after_seconds = max(retry_after_seconds, 0.0)
        super().__init__(
            grpc.StatusCode.UNAVAILABLE,
            grpc.aio.Metadata(),
            grpc.aio.Metadata(),
            details=f"circuit breaker '{name}' is OPEN, retry after {self.retry_after_seconds:.1f} s",
        )


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int,
        window_seconds: float,
        open_timeout_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self._threshold = failure_threshold
        self._window = window_seconds
        self._open_timeout = open_timeout_seconds
        self._clock = clock
        self._state = State.CLOSED
        self._failures: deque[float] = deque(maxlen=failure_threshold)
        self._opened_at = 0.0
        self._probe_in_flight = False

    @property
    def state(self) -> State:
        return self._state

    def _transition(self, new_state: State, reason: str) -> None:
        old_state, self._state = self._state, new_state
        log_event(
            logger, logging.WARNING, "circuit_breaker_state_change",
            breaker=self.name, from_state=old_state.value, to_state=new_state.value, reason=reason,
        )

    def _open(self, reason: str) -> None:
        self._opened_at = self._clock()
        self._failures.clear()
        self._transition(State.OPEN, reason)

    def acquire(self) -> bool:
        """Разрешение на вызов. Возвращает True, если это пробный вызов HALF_OPEN. В OPEN бросает CircuitOpenError."""
        now = self._clock()
        if self._state is State.OPEN:
            remaining = self._open_timeout - (now - self._opened_at)
            if remaining > 0:
                raise CircuitOpenError(self.name, remaining)
            self._transition(State.HALF_OPEN, f"open timeout {self._open_timeout:g} s elapsed")
        if self._state is State.HALF_OPEN:
            if self._probe_in_flight:
                raise CircuitOpenError(self.name, 1.0)
            self._probe_in_flight = True
            return True
        return False

    def on_success(self, probe: bool) -> None:
        if probe:
            self._probe_in_flight = False
            if self._state is State.HALF_OPEN:
                self._failures.clear()
                self._transition(State.CLOSED, "probe call succeeded")
        elif self._state is State.CLOSED:
            self._failures.clear()

    def on_failure(self, probe: bool, error: str) -> None:
        now = self._clock()
        if probe:
            self._probe_in_flight = False
            if self._state is State.HALF_OPEN:
                self._open(f"probe call failed: {error}")
            return
        if self._state is not State.CLOSED:
            # Вызов начался до перехода в OPEN и завершился позже: на автомат не влияет.
            return
        self._failures.append(now)
        while self._failures and now - self._failures[0] > self._window:
            self._failures.popleft()
        if len(self._failures) >= self._threshold:
            self._open(f"{self._threshold} consecutive failures within {self._window:g} s, last: {error}")

    def on_abandoned(self, probe: bool) -> None:
        """Вызов отменён (например, клиент закрыл соединение): исход неизвестен, состояние не меняем."""
        if probe:
            self._probe_in_flight = False
