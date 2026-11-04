# ДЗ 3. Flight Booking: gRPC + Redis

Два микросервиса бронирования авиабилетов. **Booking Service** даёт клиентам REST API и вызывает **Flight Service** по gRPC. Flight Service хранит рейсы и места и кеширует их в Redis (Sentinel). У каждого сервиса своя PostgreSQL.

Условие задания: [`docs/hw3-assignment.md`](docs/hw3-assignment.md). ДЗ 1 и ДЗ 2 лежат в ветках [`hw1`](../../tree/hw1) и [`hw2`](../../tree/hw2).

**Стек:** Python 3.12, gRPC (`grpcio`, `grpc.aio`), protobuf + `protoc` (`grpcio-tools`), FastAPI, SQLAlchemy 2 Core (async) + asyncpg, PostgreSQL 16, Flyway, Redis 7.4 Sentinel, `redis-py`.

```
Client ──REST──▶ Booking Service ──gRPC + x-api-key──▶ Flight Service
                  │  retry · circuit breaker              │        │
                  ▼                                       ▼        ▼
             PostgreSQL (bookings)              PostgreSQL     Redis Sentinel
                                                (flights)      master + replica + 3 sentinel
```

---

## 1. Быстрый старт

Нужен только Docker (Compose v2).

```bash
docker compose up --build
```

Порядок запуска: обе PostgreSQL → Flyway применяет миграции (контейнеры `*-migrations` завершаются с кодом 0) → Redis master и replica → 3 sentinel → `flight-service` (healthcheck по `grpc.health.v1`) → `booking-service`.

| Что | Адрес |
|---|---|
| Booking Service REST, Swagger UI | http://localhost:8080, http://localhost:8080/docs |
| Flight Service gRPC (reflection, нужен `x-api-key`) | `localhost:50051` |
| PostgreSQL bookings / flights | `localhost:5434` / `localhost:5435`, логин = пароль = имя БД |

Проверки (на хосте нужен только Python 3):

```bash
python3 tests/e2e.py                # 43 проверки: REST, кеш, гонка за последние места, gRPC-контракт (ещё 19)
python3 tests/e2e.py --resilience   # + 12 проверок отказов: Flight Service, БД бронирований, мастер Redis (~1 мин)
make unit                           # 10 unit-тестов Circuit Breaker и интерцепторов
```

В БД Flight Service миграция `V2` загружает демонстрационные рейсы. Расписание и цены **вымышлены**, время указано в UTC, цены в RUB за место:

| ID | Рейс | Маршрут | Вылет (UTC) | Мест | Цена | Статус |
|---|---|---|---|---|---|---|
| `a0000000-0000-4000-8000-000000000001` | SU1234 | SVO → LED | 2026-12-01 06:00 | 180 | 6500.00 | SCHEDULED |
| `…0002` | SU1236 | SVO → LED | 2026-12-01 10:00 | 150 | 7200.00 | SCHEDULED |
| `…0003` | SU1238 | SVO → LED | 2026-12-01 15:00 | 150 | 6900.00 | CANCELLED |
| `…0004` | SU1234 | SVO → LED | 2026-12-02 06:00 | 180 | 6500.00 | SCHEDULED |
| `…0005` | DP101 | VKO → LED | 2026-12-01 08:00 | 189 | 3900.00 | SCHEDULED |
| `…0006` | U61500 | SVO → AER | 2026-12-01 09:00 | **2** | 9800.00 | SCHEDULED |
| `…0007` | S72100 | DME → KZN | 2026-12-03 07:00 | 120 | 5400.00 | SCHEDULED |

Остановка: `docker compose down`, с удалением данных: `docker compose down -v`.

---

## 2. Структура

```
.
├── proto/flight/v1/flight_service.proto   # gRPC-контракт Flight Service
├── scripts/gen_proto.sh                   # protoc: *_pb2.py, *_pb2_grpc.py, *.pyi (в обоих Dockerfile и в make proto)
├── flight-service/
│   ├── migrations/                        # Flyway: V1 схема, V2 демо-данные
│   └── app/
│       ├── main.py                        # gRPC-сервер: FlightService + Health + Reflection
│       ├── servicer.py                    # методы контракта, перевод ошибок в gRPC-статусы, лог вызовов
│       ├── store.py                       # транзакции и SELECT … FOR UPDATE
│       ├── cache.py                       # Cache-Aside, Redis Sentinel
│       ├── auth.py                        # серверный интерцептор API-ключа
│       ├── convert.py, errors.py, db.py, config.py, logging_setup.py, healthcheck.py
├── booking-service/
│   ├── migrations/                        # Flyway: V1 схема
│   ├── app/
│   │   ├── main.py                        # REST-эндпоинты
│   │   ├── bookings.py                    # создание/отмена бронирования, компенсация
│   │   ├── flight_client.py               # gRPC-канал с интерцепторами
│   │   ├── interceptors.py                # metadata (API-ключ), circuit breaker, retry
│   │   ├── circuit_breaker.py             # конечный автомат CLOSED / OPEN / HALF_OPEN
│   │   ├── errors.py                      # формат ошибок, gRPC → HTTP
│   │   ├── request_logging.py, schemas.py, db.py, config.py, context.py, logging_setup.py
│   └── tests/test_resilience.py           # unit-тесты
├── redis/sentinel.conf
├── docs/er-diagram.md                     # ER-диаграмма (Mermaid) и обоснование 3NF
├── tests/e2e.py, tests/grpc_checks.py
├── docker-compose.yml
└── Makefile
```

Сгенерированный из `.proto` код в репозитории не хранится (`generated/` в `.gitignore`). Каждый Dockerfile генерирует его на отдельном этапе `codegen`. Для IDE есть `make proto`.

---

## 3. Соответствие требованиям

| № | Требование | Реализация |
|---|---|---|
| 1 | gRPC-контракт: `SearchFlights`, `GetFlight`, `ReserveSeats`, `ReleaseReservation`; `Timestamp`, `enum`, коды ошибок; `protoc` | [`flight_service.proto`](proto/flight/v1/flight_service.proto): у каждого метода в комментарии перечислены коды ошибок. Есть `FlightStatus`, `ReservationStatus`, `ErrorReason`, `google.protobuf.Timestamp`, `google.type.Date`/`Money`. Сверх обязательных методов: `UpdateFlight` (мутация из п. 7). Кодогенерация: `scripts/gen_proto.sh` |
| 2 | ER-диаграмма в 3NF, ограничения целостности | [`docs/er-diagram.md`](docs/er-diagram.md), CHECK / UNIQUE / FK / ENUM в миграциях |
| 3 | 2 PostgreSQL, Flyway, REST и gRPC-сервер | `booking-db` + `booking-migrations`, `flight-db` + `flight-migrations`. Эндпоинты в разделе 5 |
| 4 | `GetFlight` → `ReserveSeats` → `total_price = seat_count * price` → запись; при ошибке бронирования нет | `BookingService.create` в `booking-service/app/bookings.py` |
| 5 | Транзакции, `SELECT FOR UPDATE`, нет частичных изменений | `flight-service/app/store.py`, раздел 6. E2E: 12 параллельных бронирований на 3 места дают ровно 3 × 201 |
| 6 | Аутентификация gRPC: API Key в metadata, `UNAUTHENTICATED`, ключ из env | `flight-service/app/auth.py`, `MetadataInterceptor`, раздел 7 |
| 7 | Redis, Cache-Aside, TTL, инвалидация при мутациях, логи hit/miss | `flight-service/app/cache.py`, раздел 8 |
| 8 | Retry: 3 попытки, 100 → 200 мс, только `UNAVAILABLE`/`DEADLINE_EXCEEDED`, идемпотентный `ReserveSeats` | `RetryInterceptor`, раздел 9. Идемпотентность по `booking_id`: `FlightStore.reserve` + `UNIQUE (booking_id)` |
| 9 | Redis Sentinel, клиент с поддержкой Sentinel | `redis-master`, `redis-replica`, `redis-sentinel-1..3`; `redis.asyncio.sentinel.Sentinel.master_for`. Раздел 10 |
| 10 | Circuit Breaker: интерцептор, параметры из env, переходы в логах, 503 в OPEN | `CircuitBreakerInterceptor` + `circuit_breaker.py`, раздел 9 |

---

## 4. gRPC-контракт Flight Service

| Метод | Что делает | Ошибки (код / `ErrorInfo.reason`) |
|---|---|---|
| `SearchFlights(origin, destination, departure_date?)` | Рейсы `SCHEDULED` по маршруту и дате вылета (UTC), по времени вылета | `INVALID_ARGUMENT` |
| `GetFlight(flight_id)` | Рейс в любом статусе | `INVALID_ARGUMENT`, `NOT_FOUND` / `FLIGHT_NOT_FOUND` |
| `ReserveSeats(flight_id, booking_id, seat_count, expected_price?)` | Атомарно `available_seats -= n` + `SeatReservation(ACTIVE)`. Идемпотентен по `booking_id` | `NOT_FOUND`, `RESOURCE_EXHAUSTED` / `NOT_ENOUGH_SEATS`, `FAILED_PRECONDITION` / `FLIGHT_NOT_BOOKABLE`, `RESERVATION_NOT_ACTIVE`, `ABORTED` / `PRICE_CHANGED`, `ALREADY_EXISTS` / `BOOKING_ID_CONFLICT` |
| `ReleaseReservation(booking_id)` | Атомарно `available_seats += n` + статус `RELEASED`. Идемпотентен | `NOT_FOUND` / `RESERVATION_NOT_FOUND`, `FAILED_PRECONDITION` / `FLIGHT_ALREADY_DEPARTED`, `RESERVATION_NOT_ACTIVE` |
| `UpdateFlight(flight_id, status?, departure_time?, arrival_time?, price?)` | Смена статуса (`SCHEDULED→DEPARTED→COMPLETED`, `SCHEDULED→CANCELLED`), перенос, цена | `INVALID_ARGUMENT`, `NOT_FOUND`, `FAILED_PRECONDITION` / `INVALID_STATUS_TRANSITION`, `ALREADY_EXISTS` / `FLIGHT_ALREADY_EXISTS` |

Решения по контракту:

- **Ошибки.** Используются стандартные коды gRPC. Машиночитаемая причина передаётся в `google.rpc.ErrorInfo` (`domain = "flight.v1"`) в trailer `grpc-status-details-bin`. Для `NOT_ENOUGH_SEATS` в `metadata` есть `requested` и `available`. Booking Service переводит `reason` в `error_code` REST-ответа.
- **Деньги** передаются как `google.type.Money` (`currency_code` + `units` + `nanos`), не `double`. Валюта рейса фиксирована, `UpdateFlight` в другой валюте отклоняется. Конвертации валют нигде нет.
- **`expected_price` в `ReserveSeats`.** Booking Service передаёт цену из `GetFlight`. Если между двумя вызовами цену изменили через `UpdateFlight`, резервация не создаётся (`ABORTED / PRICE_CHANGED`, REST 409). Так в `total_price` не попадает цена, которой уже нет.
- **Даты.** Моменты времени передаются как `Timestamp` (UTC). Дата поиска — `google.type.Date`: это календарная дата, а не момент времени.

Вызов через `grpcurl` (reflection включён, ключ обязателен):

```bash
make grpc-list
docker run --rm --network flight-booking_default fullstorydev/grpcurl:v1.9.1 -plaintext \
  -H 'x-api-key: dev-booking-to-flight-key-change-me' \
  -d '{"flight_id":"a0000000-0000-4000-8000-000000000005","price":{"currency_code":"RUB","units":"4100"}}' \
  flight-service:50051 flight.v1.FlightService/UpdateFlight
```

---

## 5. REST API Booking Service

| Метод | Путь | Ответ |
|---|---|---|
| GET | `/flights?origin=SVO&destination=LED&date=2026-12-01` | `200` список рейсов `SCHEDULED`. `origin`, `destination` обязательны, `date` опционален |
| GET | `/flights/{id}` | `200` рейс; `404 FLIGHT_NOT_FOUND` |
| POST | `/bookings` | `201` бронирование `CONFIRMED` |
| GET | `/bookings/{id}` | `200`; `404 BOOKING_NOT_FOUND` |
| POST | `/bookings/{id}/cancel` | `200` бронирование `CANCELLED`; `409 BOOKING_NOT_CONFIRMED` |
| GET | `/bookings?user_id=X[&limit=50&offset=0]` | `200` бронирования пользователя, новые первыми |
| GET | `/health` | `200`, в ответе состояние circuit breaker |

Ошибки имеют формат `{"error_code", "message", "details"}`. Каждый ответ несёт `X-Request-Id`, и тот же ID уходит во Flight Service в metadata `x-request-id`, поэтому запрос прослеживается по логам обоих сервисов.

| `error_code` | HTTP | Когда |
|---|---|---|
| `VALIDATION_ERROR` | 400 | тело или параметры не прошли проверку (`details.fields`) |
| `FLIGHT_NOT_FOUND`, `BOOKING_NOT_FOUND` | 404 | нет по ID (в том числе ID не UUID) |
| `NOT_ENOUGH_SEATS` | 409 | `RESOURCE_EXHAUSTED`; `details.requested`, `details.available` |
| `FLIGHT_NOT_BOOKABLE` | 409 | рейс не `SCHEDULED` или уже вылетел |
| `PRICE_CHANGED` | 409 | цена изменилась между `GetFlight` и `ReserveSeats` |
| `BOOKING_NOT_CONFIRMED` | 409 | отмена не `CONFIRMED` бронирования |
| `FLIGHT_ALREADY_DEPARTED` | 409 | отмена бронирования на вылетевший рейс |
| `FLIGHT_SERVICE_UNAVAILABLE` | 503 | Flight Service недоступен после повторов, или circuit breaker в OPEN (`details.circuit_breaker = "OPEN"`, заголовок `Retry-After`) |
| `FLIGHT_SERVICE_TIMEOUT` | 504 | `DEADLINE_EXCEEDED` после повторов |
| `FLIGHT_SERVICE_ERROR` | 502 | прочие ошибки Flight Service, в том числе `UNAUTHENTICATED` (неверный ключ в конфигурации) |
| `INTERNAL_ERROR` | 500 | необработанная ошибка, например недоступна БД бронирований |

Пример:

```bash
B=http://localhost:8080
curl -s "$B/flights?origin=SVO&destination=LED&date=2026-12-01"
BID=$(curl -s -X POST $B/bookings -H 'Content-Type: application/json' -d '{
  "user_id": "user-1", "flight_id": "a0000000-0000-4000-8000-000000000001",
  "passenger_name": "Иван Петров", "passenger_email": "ivan@example.com", "seat_count": 3}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')
curl -s $B/bookings/$BID                     # total_price "19500.00", currency "RUB"
curl -s $B/flights/a0000000-0000-4000-8000-000000000001   # available_seats 177
curl -s "$B/bookings?user_id=user-1"
curl -s -X POST $B/bookings/$BID/cancel      # CANCELLED, available_seats снова 180
```

---

## 6. Транзакции и согласованность

**Flight Service** (`store.py`). Каждая мутация выполняется в одной транзакции.

- `ReserveSeats`: `SELECT … FOR UPDATE` строки рейса → проверка повтора по `booking_id` → проверка статуса, цены и мест → `UPDATE flights SET available_seats = available_seats - n` + `INSERT seat_reservations` → `COMMIT`. Второй параллельный запрос на последнее место ждёт блокировку и видит уже уменьшенный остаток. Вторая линия защиты — `CHECK (available_seats >= 0)`.
- `ReleaseReservation`: блокировка рейса, затем резервации (тот же порядок, что в `ReserveSeats`, поэтому взаимоблокировок нет) → `available_seats += n` + `status = RELEASED` → `COMMIT`.
- Идемпотентность `ReserveSeats` проверяется под блокировкой рейса. Повтор с тем же `booking_id` дожидается первого вызова и возвращает его резервацию (`replayed = true`), места не списываются повторно. Повтор того же `booking_id` с другим рейсом разделяет `UNIQUE (booking_id)`.

**Booking Service** (`bookings.py`). Общей транзакции у двух БД нет, поэтому порядок шагов выбран так, чтобы частичных изменений не оставалось:

1. `booking_id` генерируется до `ReserveSeats` и служит ключом идемпотентности.
2. `ReserveSeats` упал с бизнес-ошибкой → в БД ничего не пишется. Исход неизвестен (`UNAVAILABLE`, `DEADLINE_EXCEEDED` после повторов) → резервация могла успеть создаться, поэтому вызывается компенсирующий `ReleaseReservation` (`NOT_FOUND` означает, что снимать нечего). В OPEN вызов не отправлялся, компенсация не нужна.
3. После успешного `ReserveSeats` не удалась запись в БД → компенсирующий `ReleaseReservation`. Это проверяет `e2e --resilience`: остановлена `booking-db`, ответ 500, места на рейсе не изменились.
4. Отмена: `SELECT … FOR UPDATE` бронирования → `ReleaseReservation` → `UPDATE status = CANCELLED` → `COMMIT`. Если Flight Service ответил ошибкой, транзакция откатывается и бронирование остаётся `CONFIRMED`. Если не удался `COMMIT` после `ReleaseReservation`, повторная отмена безопасна: `ReleaseReservation` идемпотентен.
5. Участки «резервирование → запись» и «снятие → смена статуса» выполняются в `asyncio.shield`: обрыв HTTP-соединения клиентом не прерывает их на середине.

Остаточный риск: если не удалась и сама компенсация (Flight Service недоступен), резервация остаётся без бронирования. В лог пишется событие `reservation_compensation_failed` с `booking_id`, сверка ручная.

---

## 7. Аутентификация межсервисных вызовов

- Способ: **API Key** в gRPC metadata `x-api-key`.
- Flight Service (`auth.py`, `grpc.aio.ServerInterceptor`) проверяет ключ во всех методах `FlightService` и в reflection. Ключ сравнивается за постоянное время (`hmac.compare_digest`). Нет ключа или он неверный → `UNAUTHENTICATED`, в лог пишется `auth_rejected` без значения ключа. Без ключа доступен только `grpc.health.v1.Health` (healthcheck контейнера).
- Ключи берутся из переменных окружения: `FLIGHT_SERVICE_API_KEYS` у Flight Service (список через запятую, чтобы ротировать ключ без простоя) и `FLIGHT_SERVICE_API_KEY` у Booking Service. В `docker-compose.yml` оба берутся из одной переменной `${FLIGHT_SERVICE_API_KEY:-dev-…}`, её можно задать в окружении или в `.env`. Без ключа Flight Service не стартует.
- Канал без TLS: обе стороны находятся во внутренней сети Compose. Вне её ключ нужно передавать только по TLS.

---

## 8. Кеширование (Redis, Cache-Aside)

| Данные | Ключ | Значение | TTL |
|---|---|---|---|
| Рейс | `flight:{id}` | `Flight` (protobuf) | `CACHE_TTL_SECONDS` = 300 с |
| Поиск | `search:{origin}:{destination}:{YYYY-MM-DD \| any}` | `SearchFlightsResponse` | 300 с |
| Счётчики поколений | `gen:flight:{id}`, `gen:search:{origin}:{destination}` | целое | 3600 с |

TTL есть у всех ключей. Кроме того, у Redis задан `maxmemory 128mb` и политика `volatile-lru`.

- **Чтение.** `MGET` значения и счётчика поколения → hit: вернуть значение → miss: запрос в PostgreSQL → `SET … EX ttl` Lua-скриптом, только если счётчик не изменился.
- **Инвалидация** после `COMMIT` в `ReserveSeats`, `ReleaseReservation`, `UpdateFlight` выполняется одной транзакцией `MULTI/EXEC`: `INCR` счётчиков + `DEL flight:{id}`, `search:{o}:{d}:{дата}`, `search:{o}:{d}:any`. При переносе рейса сбрасываются обе даты, старая и новая.
- **Зачем счётчик поколений.** Он закрывает гонку классического Cache-Aside. Чтение начинается до мутации и читает из БД старую строку. Мутация делает `DEL`. Затем чтение кладёт старую строку в кеш, и она живёт весь TTL. Со счётчиком такая запись отбрасывается (`cache_fill_skipped`).
- **Логи:** `cache_hit`, `cache_miss`, `cache_invalidated`, `cache_fill_skipped`, `cache_unavailable`, `cache_invalidation_failed` с ключом.
- Redis не источник истины. Если он недоступен, запросы обслуживаются из PostgreSQL. Если не удалась инвалидация, устаревшее значение живёт не дольше TTL. Чтение идёт только из мастера: чтение с реплики после `DEL` могло бы вернуть удалённое значение из-за задержки репликации.

```bash
docker compose logs -f flight-service | grep cache_
```

---

## 9. Retry и Circuit Breaker (Booking Service)

Оба механизма реализованы как клиентские интерцепторы `grpc.aio` в `interceptors.py`, бизнес-логика о них не знает. Порядок в канале: `Metadata → CircuitBreaker → Retry → сеть`. Retry стоит внутри: в OPEN вызов отклоняется сразу, без повторов, а исчерпанные повторы считаются одним отказом.

**Retry.** Не больше `RETRY_MAX_ATTEMPTS` = 3 попыток, пауза `RETRY_INITIAL_BACKOFF_MS · RETRY_BACKOFF_MULTIPLIER^(n−1)`: попытка, 100 мс, попытка, 200 мс, попытка. Повторяются только `UNAVAILABLE` и `DEADLINE_EXCEEDED`. `INVALID_ARGUMENT`, `NOT_FOUND`, `RESOURCE_EXHAUSTED` и другие бизнес-ошибки сразу возвращаются вызывающему коду. Дедлайн каждой попытки — `FLIGHT_RPC_TIMEOUT_MS` = 2000 мс. Худшее время одного логического вызова при этих настройках: 3 × 2000 + 100 + 200 = 6300 мс. Повтор мутаций безопасен: `ReserveSeats` идемпотентен по `booking_id`, `ReleaseReservation` — по статусу резервации. Логи: `grpc_retry` (попытка, код, `backoff_ms`, `attempt_duration_us`), `grpc_retry_exhausted`.

**Circuit Breaker** (`circuit_breaker.py`):

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `CB_FAILURE_THRESHOLD` | 5 | отказов подряд для перехода CLOSED → OPEN |
| `CB_WINDOW_SECONDS` | 30 | эти отказы должны уложиться в окно |
| `CB_OPEN_TIMEOUT_SECONDS` | 15 | время в OPEN до перехода в HALF_OPEN |

- Отказом считаются `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, `UNKNOWN`. Бизнес-ошибки означают, что сервис работает, и сбрасывают счётчик, как и успех.
- OPEN: вызов не отправляется. Клиент сразу (порядка единиц миллисекунд) получает `503 FLIGHT_SERVICE_UNAVAILABLE` с `details.circuit_breaker = "OPEN"` и `Retry-After`.
- HALF_OPEN: проходит ровно один пробный вызов, остальные получают 503, пока он не завершится. Успех → CLOSED, отказ → OPEN.
- Переходы пишутся в лог: `circuit_breaker_state_change` с `from_state`, `to_state`, `reason`. Текущее состояние отдаёт `GET /health`.

```bash
docker compose stop flight-service
for i in $(seq 8); do curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" localhost:8080/flights/a0000000-0000-4000-8000-000000000001; done
docker compose logs booking-service | grep -E 'grpc_retry|circuit_breaker_state_change'
docker compose start flight-service   # через 15 с: OPEN → HALF_OPEN → CLOSED
```

---

## 10. Redis Sentinel

`redis-master` + `redis-replica` (`replicaof`) + `redis-sentinel-1..3` с кворумом 2, `down-after-milliseconds 5000`. Узлы анонсируют себя именами контейнеров (`replica-announce-ip`, `resolve-hostnames`, `announce-hostnames`), поэтому Sentinel отдаёт клиенту имя, а не IP, который меняется при перезапуске.

Flight Service подключается через `redis.asyncio.sentinel.Sentinel(...).master_for("mymaster")`. Адрес мастера клиент узнаёт у sentinel, а после failover переключается на нового мастера сам. Короткий `Retry` на соединение покрывает первый запрос после переключения.

```bash
docker compose exec redis-sentinel-1 redis-cli -p 26379 sentinel get-master-addr-by-name mymaster
docker compose stop redis-master      # ~5–6 с: sentinel повышает реплику, запросы всё это время идут в PostgreSQL
docker compose exec redis-sentinel-1 redis-cli -p 26379 sentinel get-master-addr-by-name mymaster   # redis-replica
docker compose start redis-master     # возвращается репликой нового мастера
```

---

## 11. Принятые трактовки и ограничения

- **Дата рейса** — дата вылета по UTC. По ней работают и уникальность «номер + дата», и фильтр `date` в поиске. Местное время аэропортов не моделируется.
- **Номер рейса** хранится как `airline_code` + `flight_no` (см. обоснование 3NF в [`docs/er-diagram.md`](docs/er-diagram.md)). В API отдаётся строкой `SU1234`.
- **`total_price`** считается в `Decimal` и хранится как `NUMERIC(15,2)` вместе с валютой рейса. В JSON деньги передаются строкой (`"19500.00"`), чтобы не терять точность.
- **`seat_count`** ограничен диапазоном 1–1000 для защиты от заведомо некорректных запросов. Реальный предел задаёт `available_seats`.
- **`EXPIRED`** есть в схеме и контракте, но в текущем флоу резервация не истекает: бронирование подтверждается синхронно сразу после `ReserveSeats`. `ReleaseReservation` для `EXPIRED` отвечает `FAILED_PRECONDITION`.
- **Поиск без даты** возвращает не больше `SEARCH_LIMIT` = 100 ближайших рейсов. Результат с датой не ограничен.
- **Отмена рейса** (`UpdateFlight` → `CANCELLED`) не отменяет бронирования автоматически: Flight Service не вызывает Booking Service. Клиент отменяет бронирование сам, места возвращаются.
- **`user_id`** — внешний идентификатор пользователя. Аутентификации клиентов в задании нет.
- **Circuit breaker** хранит состояние в памяти процесса (один процесс uvicorn). При нескольких экземплярах у каждого своё состояние.
- **Логи** в JSON. Длительности указаны в микросекундах (`duration_us`, `attempt_duration_us`), настройки пауз — в миллисекундах (`*_MS`), время — в UTC.

---

## 12. Внешние зависимости

| Зависимость | Версия | Где | Назначение |
|---|---|---|---|
| `grpcio` | 1.68.1 | оба | gRPC (`grpc.aio`) |
| `grpcio-tools` | 1.68.1 | сборка | `protoc` + плагин Python |
| `grpcio-health-checking` | 1.68.1 | flight | `grpc.health.v1` для healthcheck |
| `grpcio-reflection` | 1.68.1 | flight | reflection для `grpcurl` |
| `protobuf` | 5.29.3 | оба | runtime protobuf |
| `googleapis-common-protos` | 1.66.0 | оба | `google.type.Money/Date`, `google.rpc.Status/ErrorInfo` |
| `redis` | 5.2.1 | flight | клиент Redis с Sentinel (`redis.asyncio.sentinel`) |
| `SQLAlchemy` | 2.0.36 | оба | SQL Core, async |
| `asyncpg` | 0.30.0 | оба | драйвер PostgreSQL |
| `fastapi` | 0.115.6 | booking | REST |
| `uvicorn` | 0.34.0 | booking | ASGI-сервер |
| `pydantic` | 2.10.4 | booking | валидация запросов |
| `postgres` | 16.4-alpine | образ | БД |
| `flyway/flyway` | 10.20.1 | образ | миграции |
| `redis` | 7.4.1-alpine | образ | Redis и Sentinel |
| `fullstorydev/grpcurl` | v1.9.1 | образ | только для ручных вызовов (`make grpc-list`) |
