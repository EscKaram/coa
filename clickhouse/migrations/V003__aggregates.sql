-- Материализованные результаты сервиса агрегации. Каждый пересчёт вставляет строки с новым computed_at;
-- ReplacingMergeTree(computed_at) оставляет по ключу самую свежую версию (чтение — с FINAL).
-- Поэтому пересчёт за ту же дату заменяет значения, а не добавляет дубликаты.

-- Дневные метрики в длинном формате: одна строка — (день, метрика). Список метрик — в README, §5.
CREATE TABLE IF NOT EXISTS cinema.agg_daily_metrics
(
    metric_date Date,
    metric_name LowCardinality(String),
    value       Nullable(Float64),
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, metric_name);

-- Рейтинг фильмов за день по числу VIEW_STARTED; хранится для всех фильмов дня, топ — WHERE rank <= N.
CREATE TABLE IF NOT EXISTS cinema.agg_top_movies
(
    metric_date    Date,
    movie_id       LowCardinality(String),
    rank           UInt32,
    views          UInt64,
    unique_viewers UInt64,
    finishes       UInt64,
    likes          UInt64,
    computed_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, movie_id);

-- Когортный retention: когорта = день первой активности пользователя, day_number = 0..7.
-- Ячейка (когорта, N) есть только если день cohort_date + N уже наступил.
CREATE TABLE IF NOT EXISTS cinema.agg_retention_cohort
(
    cohort_date    Date,
    day_number     UInt8,
    cohort_size    UInt64,
    retained_users UInt64,
    retention_rate Float64,
    computed_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, day_number);
