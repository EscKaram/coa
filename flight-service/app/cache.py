"""Cache-Aside поверх Redis Sentinel.

Ключи и TTL:
    flight:{id}                          Flight (protobuf)              CACHE_TTL_SECONDS
    search:{origin}:{destination}:{date} SearchFlightsResponse          CACHE_TTL_SECONDS  ({date} = YYYY-MM-DD или "any")
    gen:flight:{id}, gen:search:{o}:{d}  счётчики поколений              CACHE_GENERATION_TTL_SECONDS

Чтение: GET значения и счётчика поколения одним запросом -> hit: вернуть; miss: прочитать БД
и записать значение с TTL, только если счётчик не изменился (Lua-скрипт, атомарно).

Инвалидация после commit мутации: INCR счётчиков поколений + DEL значений в одной транзакции
MULTI/EXEC. Счётчик закрывает гонку классического Cache-Aside: чтение, начавшееся до мутации,
загрузило из БД старые данные и записало бы их в кеш уже после DEL. С проверкой поколения такая
запись отбрасывается.

Redis здесь не источник истины: при его недоступности запросы обслуживаются из БД, ошибка
пишется в лог. Если не удалась инвалидация, устаревшее значение живёт не дольше TTL.

Чтение и запись идут только в мастер: чтение с реплики после инвалидации могло бы вернуть
удалённое значение из-за задержки репликации.
"""

import logging
from collections.abc import Awaitable, Callable, Iterable
from datetime import date

from redis.asyncio import Redis
from redis.asyncio.sentinel import Sentinel
from redis.backoff import ExponentialBackoff
from redis.exceptions import RedisError
from redis.retry import Retry

from app.config import Settings
from app.logging_setup import log_event

logger = logging.getLogger("flight.cache")

# KEYS[1] — ключ значения, KEYS[2] — счётчик поколения.
# ARGV[1] — поколение, прочитанное до запроса к БД ("" если счётчика не было), ARGV[2] — значение, ARGV[3] — TTL, с.
_SET_IF_GENERATION_UNCHANGED = """
local current = redis.call('GET', KEYS[2])
if not current then current = '' end
if current == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
    return 1
end
return 0
"""


def flight_key(flight_id: str) -> str:
    return f"flight:{flight_id}"


def flight_generation_key(flight_id: str) -> str:
    return f"gen:flight:{flight_id}"


def search_key(origin: str, destination: str, departure_date: date | None) -> str:
    return f"search:{origin}:{destination}:{departure_date.isoformat() if departure_date else 'any'}"


def search_generation_key(origin: str, destination: str) -> str:
    return f"gen:search:{origin}:{destination}"


def create_redis(settings: Settings) -> Redis:
    """Клиент мастера, найденного через Sentinel. После failover клиент сам переключается на нового мастера."""
    timeout = settings.redis_socket_timeout_ms / 1000
    sentinel = Sentinel(
        list(settings.redis_sentinels),
        sentinel_kwargs={"socket_timeout": timeout, "socket_connect_timeout": timeout},
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
    )
    # Короткий retry на обрыв соединения: при failover первый запрос к старому мастеру падает,
    # повторный идёт к мастеру, которого вернул Sentinel.
    return sentinel.master_for(
        settings.redis_master_name,
        retry=Retry(ExponentialBackoff(cap=0.2, base=0.02), retries=2),
    )


class FlightCache:
    def __init__(self, client: Redis, ttl_seconds: int, generation_ttl_seconds: int) -> None:
        self._redis = client
        self._ttl = ttl_seconds
        self._generation_ttl = generation_ttl_seconds
        self._set_if_generation_unchanged = client.register_script(_SET_IF_GENERATION_UNCHANGED)

    async def get_or_load(self, key: str, generation_key: str, loader: Callable[[], Awaitable[bytes]]) -> bytes:
        try:
            cached, generation = await self._redis.mget(key, generation_key)
        except RedisError as exc:
            log_event(logger, logging.WARNING, "cache_unavailable", op="get", key=key, error=repr(exc))
            return await loader()

        if cached is not None:
            log_event(logger, logging.INFO, "cache_hit", key=key)
            return cached

        log_event(logger, logging.INFO, "cache_miss", key=key)
        value = await loader()
        try:
            stored = await self._set_if_generation_unchanged(
                keys=[key, generation_key], args=[generation or b"", value, self._ttl]
            )
            if not stored:
                log_event(logger, logging.INFO, "cache_fill_skipped", key=key, reason="invalidated during load")
        except RedisError as exc:
            log_event(logger, logging.WARNING, "cache_unavailable", op="set", key=key, error=repr(exc))
        return value

    async def invalidate_flight(
        self, flight_id: str, origin: str, destination: str, departure_dates: Iterable[date]
    ) -> None:
        """Сбросить рейс и все результаты поиска, в которые он мог попасть (с датой и без)."""
        keys = [flight_key(flight_id), search_key(origin, destination, None)]
        keys += [search_key(origin, destination, d) for d in set(departure_dates)]
        generation_keys = [flight_generation_key(flight_id), search_generation_key(origin, destination)]
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                for generation_key in generation_keys:
                    pipe.incr(generation_key)
                    pipe.expire(generation_key, self._generation_ttl)
                pipe.delete(*keys)
                await pipe.execute()
        except RedisError as exc:
            log_event(
                logger, logging.ERROR, "cache_invalidation_failed", keys=keys, error=repr(exc),
                note=f"stale entries expire within {self._ttl} s",
            )
            return
        log_event(logger, logging.INFO, "cache_invalidated", keys=keys)
