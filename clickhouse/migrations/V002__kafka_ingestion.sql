-- Приём событий из Kafka: таблица на движке Kafka и материализованные представления, которые перекладывают
-- прочитанное в cinema.movie_events (корректные сообщения) и cinema.movie_events_dlq (ошибки разбора).

-- Формат AvroConfluent: в сообщении магический байт и id схемы, сама схема берётся из Schema Registry.
-- Поля сопоставляются по именам; типы соответствуют schemas/movie_event.v2.avsc:
--   uuid -> UUID, enum -> Enum8, timestamp-millis -> DateTime64(3), ["null", T] -> Nullable(T).
-- input_format_avro_allow_missing_fields = 1: у сообщений схемы v1 нет search_query, оно станет NULL.
-- kafka_handle_error_mode = 'stream': ошибка разбора не останавливает чтение, а заполняет _error и _raw_message.
-- Один потребитель читает все 3 партиции; при росте потока kafka_num_consumers увеличивается до числа партиций.
CREATE TABLE IF NOT EXISTS cinema.movie_events_queue
(
    event_id         UUID,
    user_id          String,
    movie_id         String,
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                           'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Nullable(Int32),
    search_query     Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{KAFKA_BROKER_LIST}}',
    kafka_topic_list = '{{KAFKA_TOPIC}}',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 1000,
    kafka_handle_error_mode = 'stream',
    format_avro_schema_registry_url = '{{SCHEMA_REGISTRY_URL}}',
    input_format_avro_allow_missing_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_consumer
TO cinema.movie_events
AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    timestamp,
    device_type,
    session_id,
    progress_seconds,
    search_query,
    _partition AS kafka_partition,
    _offset    AS kafka_offset,
    now64(3)   AS ingested_at
FROM cinema.movie_events_queue
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_dlq_consumer
TO cinema.movie_events_dlq
AS
SELECT
    now64(3)     AS received_at,
    _topic       AS kafka_topic,
    _partition   AS kafka_partition,
    _offset      AS kafka_offset,
    _key         AS kafka_key,
    _raw_message AS raw_message,
    _error       AS error
FROM cinema.movie_events_queue
WHERE length(_error) > 0;
