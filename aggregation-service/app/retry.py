"""Повтор операции с экспоненциальной задержкой и джиттером (синхронный, выполняется в рабочем потоке)."""

import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")
logger = logging.getLogger("aggregation.retry")


def with_retry(
    operation: Callable[[], T],
    *,
    what: str,
    max_attempts: int,
    base_ms: int,
    max_ms: int,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except retry_on as exc:
            if attempt == max_attempts:
                logger.error(f"{what} failed, giving up", extra={"attempts": attempt, "error": repr(exc)})
                raise
            delay_ms = min(max_ms, base_ms * 2 ** (attempt - 1)) * random.uniform(0.5, 1.0)
            logger.warning(
                f"{what} failed, retrying",
                extra={"attempt": attempt, "retry_in_ms": round(delay_ms), "error": repr(exc)},
            )
            time.sleep(delay_ms / 1000)
    raise AssertionError("unreachable")
