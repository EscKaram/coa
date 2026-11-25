"""Генератор синтетических событий: имитация пользователей онлайн-кинотеатра.

Все данные вымышлены. Модель детерминирована: состав пользователей, их сессии и event_id выводятся из
(GENERATOR_SEED, дата, пользователь), поэтому повторный запуск в тот же день публикует те же события
с теми же event_id, а ClickHouse схлопывает дубликаты.

Модель:
- каждый день приходит когорта новых пользователей (GENERATOR_NEW_USERS_PER_DAY ± 20%);
- пользователь когорты C активен в день C + k с вероятностью stickiness * 0.55 * k^-0.45 (k >= 1),
  что даёт retention D1 около 45–50%, D7 около 15%;
- активный пользователь проводит 1–3 сессии; сессия: [SEARCHED] → VIEW_STARTED → (VIEW_PAUSED →
  VIEW_RESUMED)* → VIEW_FINISHED [→ LIKED] либо обрыв на VIEW_PAUSED. progress_seconds не убывает,
  между паузой и продолжением равен, время событий согласовано с позицией воспроизведения.

Режимы:
1. Догрузка истории: события за GENERATOR_BACKFILL_DAYS прошедших дней и сегодняшние события до текущего
   момента публикуются пакетно (лог публикации каждого события — DEBUG, по дню — INFO).
2. Реальное время: остальные события публикуются, когда наступает их время (лог каждого — INFO).
"""

import asyncio
import heapq
import logging
import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from .config import Settings
from .events import DeviceType, EventType
from .publisher import EventPublisher, PublishError

logger = logging.getLogger("movie_service.generator")

NAMESPACE = uuid.UUID("6f1c1a52-6b7e-4f2b-9a51-2f4f0c1e9a10")
MAX_RETURN_AGE_DAYS = 30
_WORDS_A = ["Silent", "Last", "Northern", "Red", "Hidden", "Broken", "Golden", "Lost", "Iron", "Midnight"]
_WORDS_B = ["River", "Signal", "Harbor", "Orbit", "Garden", "Empire", "Winter", "Code", "Island", "Protocol"]
_DEVICES = [DeviceType.MOBILE, DeviceType.DESKTOP, DeviceType.TV, DeviceType.TABLET]
_DEVICE_WEIGHTS = [0.45, 0.25, 0.20, 0.10]


@dataclass(frozen=True)
class Movie:
    movie_id: str
    title: str
    duration_s: int
    weight: float


@dataclass(frozen=True)
class User:
    user_id: str
    cohort: date
    stickiness: float
    device: DeviceType


@dataclass(order=True)
class PlannedEvent:
    ts: datetime
    seq: int
    record: dict[str, Any] = field(compare=False)


def build_catalog(seed: int, size: int) -> list[Movie]:
    rng = random.Random(f"{seed}:catalog")
    movies = []
    for i in range(size):
        is_episode = rng.random() < 0.4
        duration = rng.randint(20, 55) * 60 if is_episode else rng.randint(85, 160) * 60
        title = f"{rng.choice(_WORDS_A)} {rng.choice(_WORDS_B)}"
        # Популярность по закону Ципфа: несколько хитов и длинный хвост.
        movies.append(Movie(f"m-{i + 1:04d}", title, duration, 1 / (i + 1) ** 0.9))
    return movies


def _midnight(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


class EventGenerator:
    def __init__(self, settings: Settings, publisher: EventPublisher):
        self._seed = settings.generator_seed
        self._backfill_days = settings.generator_backfill_days
        self._new_users = settings.generator_new_users_per_day
        self._publisher = publisher
        self._catalog = build_catalog(self._seed, settings.generator_movies)
        self._weights = [m.weight for m in self._catalog]
        self._cohorts: dict[date, list[User]] = {}
        self._start: date | None = None
        self._seq = 0
        self.stats = {"published": 0, "failed": 0, "mode": "starting", "next_event_at": None}

    # ---------- модель пользователей ----------

    def _cohort(self, day: date) -> list[User]:
        if day not in self._cohorts:
            rng = random.Random(f"{self._seed}:cohort:{day.isoformat()}")
            size = max(1, round(self._new_users * rng.uniform(0.8, 1.2)))
            self._cohorts[day] = [
                User(
                    user_id=f"u-{day:%Y%m%d}-{i:04d}",
                    cohort=day,
                    stickiness=rng.uniform(0.35, 1.0),
                    device=rng.choices(_DEVICES, _DEVICE_WEIGHTS)[0],
                )
                for i in range(size)
            ]
        return self._cohorts[day]

    def _active_users(self, day: date) -> list[User]:
        assert self._start is not None
        active = list(self._cohort(day))
        for k in range(1, MAX_RETURN_AGE_DAYS + 1):
            cohort_day = day - timedelta(days=k)
            if cohort_day < self._start:
                break
            for user in self._cohort(cohort_day):
                p = user.stickiness * 0.55 * k**-0.45
                if random.Random(f"{self._seed}:active:{user.user_id}:{day.isoformat()}").random() < p:
                    active.append(user)
        return active

    # ---------- сессии ----------

    def _session(self, user: User, day: date, index: int, start: datetime, rng: random.Random):
        movie = rng.choices(self._catalog, self._weights)[0]
        device = user.device if rng.random() < 0.8 else rng.choice(_DEVICES)
        session_id = str(uuid.uuid5(NAMESPACE, f"{self._seed}:session:{user.user_id}:{day.isoformat()}:{index}"))
        events: list[tuple[datetime, dict[str, Any]]] = []
        t = start

        def emit(event_type: EventType, progress: int | None = None, query: str | None = None) -> None:
            events.append(
                (
                    t,
                    {
                        "event_id": str(uuid.uuid5(NAMESPACE, f"{session_id}:{len(events)}")),
                        "user_id": user.user_id,
                        "movie_id": movie.movie_id,
                        "event_type": event_type.value,
                        "timestamp": t,
                        "device_type": device.value,
                        "session_id": session_id,
                        "progress_seconds": progress,
                        "search_query": query,
                    },
                )
            )

        if rng.random() < 0.35:
            title = movie.title.lower()
            emit(EventType.SEARCHED, query=title[: rng.randint(3, len(title))])
            t += timedelta(seconds=rng.randint(5, 90))

        emit(EventType.VIEW_STARTED, 0)
        finished = rng.random() < 0.62
        end_position = movie.duration_s if finished else rng.randint(60, movie.duration_s - 60)
        pauses = rng.choices([0, 1, 2, 3], [0.35, 0.35, 0.2, 0.1])[0]
        pause_points = sorted(rng.sample(range(30, end_position - 10), k=pauses))
        position = 0
        for point in pause_points:
            t += timedelta(seconds=point - position)
            position = point
            emit(EventType.VIEW_PAUSED, position)
            t += timedelta(seconds=rng.randint(20, 900))
            emit(EventType.VIEW_RESUMED, position)
        t += timedelta(seconds=end_position - position)
        if finished:
            emit(EventType.VIEW_FINISHED, movie.duration_s)
            if rng.random() < 0.3:
                t += timedelta(seconds=rng.randint(5, 60))
                emit(EventType.LIKED)
        else:
            # Пользователь ушёл, не досмотрев: сессия обрывается на паузе.
            emit(EventType.VIEW_PAUSED, end_position)
        return events, t

    def day_plan(self, day: date) -> list[PlannedEvent]:
        """Все события сессий, начатых в день day (часть может закончиться после полуночи)."""
        plan: list[PlannedEvent] = []
        day_start = _midnight(day)
        for user in self._active_users(day):
            rng = random.Random(f"{self._seed}:sessions:{user.user_id}:{day.isoformat()}")
            sessions = rng.choices([1, 2, 3], [0.6, 0.3, 0.1])[0]
            # Вечерний пик около 19:00 UTC плюс равномерный фон.
            if rng.random() < 0.6:
                offset = min(max(rng.gauss(19 * 3600, 3 * 3600), 0), 86_399)
            else:
                offset = rng.uniform(0, 86_399)
            start = day_start + timedelta(seconds=math.floor(offset))
            for index in range(sessions):
                if start.date() != day:
                    break
                events, end = self._session(user, day, index, start, rng)
                for ts, record in events:
                    self._seq += 1
                    plan.append(PlannedEvent(ts, self._seq, record))
                start = end + timedelta(minutes=rng.randint(10, 240))
        plan.sort()
        return plan

    # ---------- запуск ----------

    async def run(self) -> None:
        try:
            await self._run()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("generator crashed")
            self.stats["mode"] = "crashed"

    async def _run(self) -> None:
        now = datetime.now(timezone.utc)
        today = now.date()
        self._start = today - timedelta(days=self._backfill_days)
        pending: list[PlannedEvent] = []

        self.stats["mode"] = "backfill"
        logger.info(
            "generator started: backfill",
            extra={"from": self._start.isoformat(), "to": now.isoformat(), "seed": self._seed},
        )
        day = self._start
        while day <= today:
            due = []
            for event in self.day_plan(day):
                (due if event.ts <= now else pending).append(event)
            ok, failed = await self._publisher.publish_many([e.record for e in due])
            self.stats["published"] += ok
            self.stats["failed"] += failed
            logger.info("backfill day published", extra={"day": day.isoformat(), "events": ok, "failed": failed})
            day += timedelta(days=1)
        heapq.heapify(pending)

        self.stats["mode"] = "realtime"
        next_day = today + timedelta(days=1)
        logger.info("generator switched to real time", extra={"scheduled_events": len(pending)})
        while True:
            now = datetime.now(timezone.utc)
            if now >= _midnight(next_day):
                for event in self.day_plan(next_day):
                    heapq.heappush(pending, event)
                next_day += timedelta(days=1)
                continue
            while pending and pending[0].ts <= now:
                event = heapq.heappop(pending)
                try:
                    await self._publisher.publish(event.record)
                    self.stats["published"] += 1
                except PublishError:
                    self.stats["failed"] += 1
            wake_at = _midnight(next_day)
            if pending:
                wake_at = min(wake_at, pending[0].ts)
                self.stats["next_event_at"] = pending[0].ts.isoformat()
            delay = (wake_at - datetime.now(timezone.utc)).total_seconds()
            await asyncio.sleep(min(max(delay, 0.05), 5.0))
