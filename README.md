# ДЗ 2. Marketplace: OpenAPI + CRUD

API маркетплейса, спроектированное от контракта: товары (CRUD с мягким удалением), заказы с бизнес-логикой, промокоды, JWT-аутентификация и роли.

Условие задания: [`docs/hw2-assignment.md`](docs/hw2-assignment.md). ДЗ 1 лежит в ветке [`hw1`](../../tree/hw1).

**Стек:** Python 3.12, FastAPI, SQLAlchemy 2 (async) + asyncpg, PostgreSQL 16, Flyway, openapi-generator (python-fastapi).

---

## 1. Быстрый старт

Нужен только Docker (Engine 20.10+ с Compose v2).

```bash
docker compose up --build          # postgres → flyway migrate → app
```

Что происходит при запуске:

1. `postgres` поднимается и проходит healthcheck.
2. `flyway` применяет миграции из `db/migrations` и завершается с кодом 0.
3. `app` собирается. На первом этапе Dockerfile генерирует код из `openapi/openapi.yaml`. При старте создаётся администратор из `ADMIN_EMAIL` / `ADMIN_PASSWORD`.

| Что | Адрес |
|---|---|
| API | http://localhost:8080 |
| Swagger UI | http://localhost:8080/docs |
| PostgreSQL | `localhost:5433`, БД / пользователь / пароль: `marketplace` |
| Администратор | `admin@marketplace.local` / `admin12345` |

E2E-проверка всех сценариев (78 проверок, нужен только Python 3 из стандартной поставки):

```bash
python3 tests/e2e.py
```

Остановка: `docker compose down`, с удалением данных: `docker compose down -v`.

### Конфигурация (`docker-compose.yml`)

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `ORDER_RATE_LIMIT_MINUTES` | `1` | N из задания: интервал между `CREATE_ORDER` (и между `UPDATE_ORDER`) одного пользователя. `0` отключает лимит |
| `ACCESS_TOKEN_TTL_MINUTES` | `15` | Время жизни access token |
| `REFRESH_TOKEN_TTL_DAYS` | `7` | Время жизни refresh token |
| `JWT_SECRET` | — | Ключ подписи HS256 |
| `ADMIN_EMAIL`, `ADMIN_PASSWORD` | — | Учётная запись ADMIN, создаётся при старте |

---

## 2. Структура

```
.
├── openapi/
│   ├── openapi.yaml            # контракт: эндпоинты, схемы, ограничения, ошибки, security
│   └── templates/              # 4 переопределённых шаблона генератора (см. раздел 3)
├── scripts/generate.sh         # генерация кода, одна команда
├── generated/                  # сгенерированный код (в .gitignore)
├── db/migrations/V1__init_schema.sql   # Flyway
├── app/
│   ├── main.py                 # сборка приложения из сгенерированных роутеров
│   ├── impl/                   # реализации сгенерированных Base*Api
│   ├── services/               # бизнес-логика: products, orders, promo_codes, auth
│   ├── tables.py               # ORM-отображение таблиц
│   ├── security.py             # пароли, JWT, проверка ролей
│   ├── errors.py               # формат Error и обработчики исключений
│   └── request_logging.py      # JSON-лог каждого запроса, X-Request-Id
├── tests/e2e.py                # E2E-сценарии против запущенного сервиса
├── Dockerfile                  # этап 1 — кодогенерация, этап 2 — приложение
├── docker-compose.yml
└── Makefile
```

---

## 3. Кодогенерация

```bash
make generate     # = docker run openapitools/openapi-generator-cli:v7.10.0 ... scripts/generate.sh
```

- Генератор: `openapi-generator` 7.10.0, `python-fastapi`. Результат пишется в `generated/marketplace_api/`, каталог в `.gitignore`.
- При `docker compose up --build` код генерируется заново на этапе `codegen` в Dockerfile, ручной шаг не нужен.
- Из сгенерированного кода используются:
  - **роутеры** (`marketplace_api.apis.*_api`): FastAPI-эндпоинты, параметры и тела запросов;
  - **базовые классы** (`*_api_base.Base*Api`): их реализуют классы из `app/impl/`;
  - **модели** (`marketplace_api.models.*`): pydantic-модели запросов и ответов с ограничениями из спецификации. Ручных DTO нет;
  - `security_api.get_token_bearerAuth`: зависимость аутентификации. Её заменяет `app.security.authenticate` через `dependency_overrides`.
- Переопределены 4 шаблона (`openapi/templates/`), чтобы исправить недостатки штатного генератора:

| Шаблон | Что исправлено |
|---|---|
| `endpoint_argument_definition.mustache` | обязательное тело запроса было `Body(None)`, стало `Body(...)`; `pattern` для path-параметров (UUID); целые query-параметры принимаются как `int`, а не `strict int` (иначе `?page=0` отклоняется) |
| `api.mustache` | `status_code` берётся из первого ответа в спецификации (`201` для POST-создания); в реализацию передаётся проверенный токен |
| `base_api.mustache` | в сигнатуры методов добавлен параметр `token: TokenModel` |
| `extra_models.mustache` | в `TokenModel` добавлен `role` |

Как проходит запрос: сгенерированный роутер → валидация pydantic-моделью → `authenticate` (JWT) → `app/impl/*` → `app/services/*` → PostgreSQL.

---

## 4. Соответствие требованиям

| № | Требование | Где реализовано |
|---|---|---|
| 1 | CRUD `/products`, пагинация `page`/`size`, фильтры `status`/`category`, ответ с `totalElements` | `openapi.yaml`, `services/products.py` |
| 2 | Схемы `ProductCreate` / `ProductUpdate` / `ProductResponse`, `format: decimal / date-time / uuid`, `enum`, `nullable`, `required` | `openapi.yaml → components.schemas` |
| 3 | Кодогенерация одной командой, код в `.gitignore`, генерация при сборке | раздел 3 |
| 4 | PostgreSQL, Flyway, индекс `idx_products_status`, `created_at` (default) / `updated_at` (триггер `set_updated_at`), мягкое удаление | `db/migrations/V1__init_schema.sql`, `archive_product` |
| 5 | Единый формат `Error {error_code, message, details}`, каталог кодов в `ErrorCode` | `openapi.yaml`, `app/errors.py` |
| 6 | Ограничения в OpenAPI (`minLength`, `maximum`, `minItems`, `pattern`…), `VALIDATION_ERROR` с полями в `details.fields` | сгенерированные модели + обработчик `RequestValidationError` |
| 7 | Создание / обновление / отмена заказа, машина состояний, промокоды, всё в одной транзакции | `services/orders.py`, `services/promo_codes.py` |
| 8 | JSON-лог каждого запроса: `request_id`, `method`, `endpoint`, `status_code`, `duration_ms`, `user_id`, `timestamp`, тело мутирующих запросов с маской паролей; `X-Request-Id` в каждом ответе, включая 500 `INTERNAL_ERROR` | `app/request_logging.py` |
| 9 | `/auth/register`, `/auth/login`, `/auth/refresh`; access 15 мин, refresh 7 дней; `TOKEN_EXPIRED` / `TOKEN_INVALID` / `REFRESH_TOKEN_INVALID` | `app/security.py`, `services/auth.py` |
| 10 | Роли `USER` / `SELLER` / `ADMIN` в claim `role`, `seller_id` у товара, `ACCESS_DENIED`, матрица доступа в описании спецификации | `require_role`, `_check_can_manage`, `_check_owner` |

### Каталог ошибок

| Код | HTTP | Когда |
|---|---|---|
| `VALIDATION_ERROR` | 400 | нарушение ограничений OpenAPI (включая границы int32), битый JSON, нет тела, некорректный UUID, ограничения между полями промокода |
| `TOKEN_INVALID` / `TOKEN_EXPIRED` | 401 | нет access token, неверная подпись или тип токена / срок истёк |
| `REFRESH_TOKEN_INVALID` | 401 | refresh token невалиден или истёк |
| `INVALID_CREDENTIALS` | 401 | неверный email или пароль |
| `ACCESS_DENIED` | 403 | роли не разрешена операция; SELLER меняет чужой товар |
| `ORDER_OWNERSHIP_VIOLATION` | 403 | заказ другого пользователя |
| `PRODUCT_NOT_FOUND` / `ORDER_NOT_FOUND` | 404 | нет по ID |
| `PRODUCT_INACTIVE` | 409 | товар в заказе не `ACTIVE` |
| `ORDER_HAS_ACTIVE` | 409 | у пользователя есть заказ в `CREATED` / `PAYMENT_PENDING` |
| `INVALID_STATE_TRANSITION` | 409 | недопустимый переход; обновление не в `CREATED` |
| `INSUFFICIENT_STOCK` | 409 | `details.items`: `product_id`, `requested`, `available` |
| `USER_ALREADY_EXISTS` / `PROMO_CODE_ALREADY_EXISTS` | 409 | дубликат email / кода |
| `PROMO_CODE_INVALID` | 422 | `details.reason`: `NOT_FOUND`, `INACTIVE`, `NOT_STARTED`, `EXPIRED`, `EXHAUSTED` |
| `PROMO_CODE_MIN_AMOUNT` | 422 | сумма ниже `min_order_amount` |
| `ORDER_LIMIT_EXCEEDED` | 429 | `details.retry_after_seconds` |

---

## 5. Бизнес-логика заказов

### Модель состояний

```
CREATED → PAYMENT_PENDING → PAID → SHIPPED → COMPLETED
   ↓             ↓
CANCELED     CANCELED
```

Переходы выполняются через `POST /orders/{id}/status` (`{"status": "PAYMENT_PENDING"}`), отмена — через `POST /orders/{id}/cancel`. Запрос на переход в `CANCELED` через `/status` выполняет ту же процедуру отмены. Таблица переходов: `TRANSITIONS` в `services/orders.py`.

### Транзакции и блокировки

Каждый запрос выполняется в одной транзакции (`app.db.transaction`): при любой ошибке откатывается всё, включая резерв остатков и `current_uses`. От гонок защищают блокировки `SELECT … FOR UPDATE`:

- строка пользователя: два параллельных `POST /orders` одного пользователя выполняются по очереди, поэтому не обходят лимит частоты и проверку активного заказа;
- строки товаров блокируются в порядке `id`, поэтому параллельные заказы не взаимоблокируются и не уводят остаток в минус (дополнительно есть `CHECK (stock >= 0)`);
- строка промокода: `current_uses` не превысит `max_uses` (дополнительно есть `CHECK`).

### Принятые трактовки

- **PERCENTAGE больше 70%.** Условие «не более 70% от `total_amount`, в ином случае цена 100%» прочитано так: если `discount_value > 70`, скидка равна 0 и заказ оплачивается полностью. Промокод при этом считается применённым (`current_uses` увеличивается), как описано в алгоритме.
- **Пересчёт при `PUT /orders/{id}`.** Промокод снимается, если сумма стала ниже `min_order_amount` или промокод стал неактивным / истёк. Проверка `current_uses < max_uses` при пересчёте не выполняется: использование этим заказом уже учтено.
- **Лимит частоты при обновлении** считается по пользователю, который выполняет запрос (для ADMIN это сам ADMIN).
- **Повторяющийся `product_id`** в `items` допустим. Остаток проверяется по суммарному количеству.
- **Регистрация** доступна с ролями `USER` и `SELLER`. `ADMIN` создаётся только из переменных окружения.
- **Деньги** в API передаются как `number` (`format: decimal`), в БД хранятся как `NUMERIC(12,2)`, считаются в `Decimal` и округляются до копеек (ROUND_HALF_UP). Сумма заказа больше 9 999 999 999.99 (предел колонки) отклоняется с `VALIDATION_ERROR`.
- **Даты без часового пояса** в `valid_from` / `valid_until` считаются UTC.

---

## 6. Демонстрация

```bash
BASE=http://localhost:8080
json() { python3 -c "import sys,json; print(json.load(sys.stdin)$1)"; }

# продавец и покупатель
curl -s -X POST $BASE/auth/register -H 'Content-Type: application/json' -d '{"email":"seller@demo.io","password":"password123","role":"SELLER"}'
curl -s -X POST $BASE/auth/register -H 'Content-Type: application/json' -d '{"email":"buyer@demo.io","password":"password123"}'
SELLER=$(curl -s -X POST $BASE/auth/login -H 'Content-Type: application/json' -d '{"email":"seller@demo.io","password":"password123"}' | json "['access_token']")
BUYER=$(curl -s -X POST $BASE/auth/login -H 'Content-Type: application/json' -d '{"email":"buyer@demo.io","password":"password123"}' | json "['access_token']")

# товар и промокод
PID=$(curl -s -X POST $BASE/products -H "Authorization: Bearer $SELLER" -H 'Content-Type: application/json' \
  -d '{"name":"Phone","price":100.00,"stock":10,"category":"electronics","status":"ACTIVE"}' | json "['id']")
curl -s -X POST $BASE/promo-codes -H "Authorization: Bearer $SELLER" -H 'Content-Type: application/json' \
  -d '{"code":"SALE_10","discount_type":"PERCENTAGE","discount_value":10,"max_uses":100,"valid_from":"2026-01-01T00:00:00Z","valid_until":"2027-01-01T00:00:00Z"}'

# заказ
OID=$(curl -s -X POST $BASE/orders -H "Authorization: Bearer $BUYER" -H 'Content-Type: application/json' \
  -d "{\"items\":[{\"product_id\":\"$PID\",\"quantity\":2}],\"promo_code\":\"SALE_10\"}" | tee /dev/stderr | json "['id']")

# список, обновление, отмена
curl -s "$BASE/products?page=0&size=20&status=ACTIVE&category=electronics" -H "Authorization: Bearer $BUYER"
curl -s -X PUT $BASE/orders/$OID -H "Authorization: Bearer $BUYER" -H 'Content-Type: application/json' -d "{\"items\":[{\"product_id\":\"$PID\",\"quantity\":1}]}"
curl -s -X POST $BASE/orders/$OID/cancel -H "Authorization: Bearer $BUYER"
```

Содержимое таблиц:

```bash
make psql     # = docker compose exec postgres psql -U marketplace -d marketplace
```

```sql
SELECT id, name, price, stock, status, seller_id, created_at, updated_at FROM products ORDER BY created_at DESC LIMIT 10;
SELECT id, user_id, status, promo_code_id, total_amount, discount_amount, created_at, updated_at FROM orders ORDER BY created_at DESC;
SELECT order_id, product_id, quantity, price_at_order FROM order_items;
SELECT code, discount_type, discount_value, min_order_amount, max_uses, current_uses, active FROM promo_codes;
SELECT user_id, operation_type, created_at FROM user_operations ORDER BY created_at DESC;
SELECT id, email, role FROM users;
SELECT version, description, success FROM flyway_schema_history;
\d products        -- индекс idx_products_status и триггер products_set_updated_at
```

Логи запросов: `docker compose logs -f app`.

---

## 7. Внешние зависимости

| Зависимость | Версия | Назначение |
|---|---|---|
| `fastapi` | 0.115.6 | HTTP-фреймворк, его используют сгенерированные роутеры |
| `uvicorn` | 0.34.0 | ASGI-сервер |
| `pydantic` | 2.10.4 | сгенерированные модели и валидация |
| `SQLAlchemy` | 2.0.36 | ORM, асинхронные сессии |
| `asyncpg` | 0.30.0 | драйвер PostgreSQL |
| `PyJWT` | 2.10.1 | подпись и проверка JWT (HS256) |
| `openapitools/openapi-generator-cli` | v7.10.0 | образ генератора (только сборка) |
| `flyway/flyway` | 10.20.1 | образ для миграций |
| `postgres` | 16.4-alpine | БД |

Пароли хешируются PBKDF2-SHA256 (200 000 итераций) из стандартной библиотеки, отдельная зависимость не нужна.
