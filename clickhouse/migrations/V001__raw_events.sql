-- Постоянное хранилище событий и производные таблицы, которые наполняются материализованными представлениями.

-- Сырые события. ReplacingMergeTree схлопывает повторные доставки одного события (at-least-once из Kafka
-- и повторы продюсера): дубликаты совпадают по всему ключу сортировки, включая event_id.
-- Партиция — месяц по дате события (UTC). Ключ сортировки начинается с event_date и event_type:
-- агрегаты сервиса агрегации фильтруют по дню и типу события и группируют по user_id / movie_id.
CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id         UUID,
    user_id          String,
    movie_id         LowCardinality(String),
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                           'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Nullable(Int32),
    search_query     Nullable(String),
    event_date       Date MATERIALIZED toDate(timestamp),
    kafka_partition  UInt16,
    kafka_offset     UInt64,
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id, event_id);

-- Дни активности пользователя: (user_id, день) без повторов. Отсюда считается дата первой активности
-- (когорта) и возвращаемость, не сканируя сырые события целиком.
CREATE TABLE IF NOT EXISTS cinema.user_daily_activity
(
    user_id       String,
    activity_date Date
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(activity_date)
ORDER BY (user_id, activity_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.user_daily_activity_mv
TO cinema.user_daily_activity
AS
SELECT DISTINCT user_id, toDate(timestamp) AS activity_date
FROM cinema.movie_events;

-- Сообщения, которые не удалось разобрать (не Avro Confluent, неизвестная схема и т. п.).
-- Битое сообщение не останавливает потребителя: оно попадает сюда вместе с текстом ошибки.
CREATE TABLE IF NOT EXISTS cinema.movie_events_dlq
(
    received_at     DateTime64(3, 'UTC') DEFAULT now64(3),
    kafka_topic     LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset    UInt64,
    kafka_key       String,
    raw_message     String,
    error           String
)
ENGINE = MergeTree
ORDER BY (received_at, kafka_partition, kafka_offset)
TTL toDateTime(received_at) + INTERVAL 30 DAY;
