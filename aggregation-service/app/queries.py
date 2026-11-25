"""SQL сервиса агрегации. Вычисления выполняет ClickHouse (INSERT ... SELECT), сервис только запускает их,
читает результат и переносит его в PostgreSQL.

Параметры подставляет сервер ClickHouse ({name:Type}), не форматирование строк.
Сырые события читаются с FINAL: повторные доставки одного event_id не искажают счётчики.
"""

# Дневные метрики: одна строка на (день, метрика). ARRAY JOIN разворачивает широкую строку агрегатов в длинную.
DAILY_METRICS = """
INSERT INTO cinema.agg_daily_metrics (metric_date, metric_name, value, computed_at)
SELECT metric_date, m.1 AS metric_name, m.2 AS value, {computed_at:DateTime64(3, 'UTC')}
FROM
(
    SELECT
        event_date                                          AS metric_date,
        uniqExact(user_id)                                  AS dau,
        count()                                             AS events_total,
        uniqExact(session_id)                               AS sessions,
        countIf(event_type = 'VIEW_STARTED')                AS views_started,
        countIf(event_type = 'VIEW_FINISHED')               AS views_finished,
        avgIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch_time,
        countIf(event_type = 'LIKED')                       AS likes,
        countIf(event_type = 'SEARCHED')                    AS searches
    FROM cinema.movie_events FINAL
    WHERE event_date BETWEEN {date_from:Date} AND {date_to:Date}
    GROUP BY event_date
)
ARRAY JOIN [
    ('dau',                    CAST(dau AS Nullable(Float64))),
    ('events_total',           CAST(events_total AS Nullable(Float64))),
    ('sessions',               CAST(sessions AS Nullable(Float64))),
    ('views_started',          CAST(views_started AS Nullable(Float64))),
    ('views_finished',         CAST(views_finished AS Nullable(Float64))),
    ('likes',                  CAST(likes AS Nullable(Float64))),
    ('searches',               CAST(searches AS Nullable(Float64))),
    ('view_conversion',        if(views_started = 0, NULL, views_finished / views_started)),
    ('avg_watch_time_seconds', if(views_finished = 0, NULL, avg_watch_time))
] AS m
"""

# Рейтинг фильмов за день: оконная функция row_number() по числу запусков просмотра.
TOP_MOVIES = """
INSERT INTO cinema.agg_top_movies
    (metric_date, movie_id, rank, views, unique_viewers, finishes, likes, computed_at)
SELECT
    metric_date,
    movie_id,
    toUInt32(row_number() OVER (PARTITION BY metric_date
                                ORDER BY views DESC, unique_viewers DESC, movie_id ASC)) AS rank,
    views,
    unique_viewers,
    finishes,
    likes,
    {computed_at:DateTime64(3, 'UTC')}
FROM
(
    SELECT
        event_date                                          AS metric_date,
        movie_id,
        countIf(event_type = 'VIEW_STARTED')                AS views,
        uniqExactIf(user_id, event_type = 'VIEW_STARTED')   AS unique_viewers,
        countIf(event_type = 'VIEW_FINISHED')               AS finishes,
        countIf(event_type = 'LIKED')                       AS likes
    FROM cinema.movie_events FINAL
    WHERE event_date BETWEEN {date_from:Date} AND {date_to:Date}
      AND event_type IN ('VIEW_STARTED', 'VIEW_FINISHED', 'LIKED')
    GROUP BY metric_date, movie_id
    HAVING views > 0
)
"""

# Когортный retention. Дата первой активности — оконный min() по пользователю; для каждого пользователя
# когорты собирается множество дней жизни, в которые он был активен; ARRAY JOIN по 0..7 даёт ячейки.
# Ячейка (когорта, N) пишется, только если день cohort_date + N уже наступил ({today}).
RETENTION_COHORTS = """
INSERT INTO cinema.agg_retention_cohort
    (cohort_date, day_number, cohort_size, retained_users, retention_rate, computed_at)
SELECT
    cohort_date,
    day_number,
    count()                                AS cohort_size,
    countIf(has(active_days, day_number))  AS retained_users,
    retained_users / cohort_size           AS retention_rate,
    {computed_at:DateTime64(3, 'UTC')}
FROM
(
    SELECT
        first_date AS cohort_date,
        user_id,
        groupUniqArray(toUInt8(dateDiff('day', first_date, activity_date))) AS active_days
    FROM
    (
        SELECT
            user_id,
            activity_date,
            min(activity_date) OVER (PARTITION BY user_id) AS first_date
        FROM cinema.user_daily_activity FINAL
    )
    WHERE first_date BETWEEN {cohort_from:Date} AND {cohort_to:Date}
      AND activity_date <= first_date + 7
    GROUP BY cohort_date, user_id
)
ARRAY JOIN range(8) AS day_number
WHERE cohort_date + day_number <= {today:Date}
GROUP BY cohort_date, day_number
"""

# Retention D1 / D7 как метрики дня D: доля когорты D-1 (D-7), активной в день D.
# Значение за день D окончательно, как только закончился день D.
RETENTION_METRICS = """
INSERT INTO cinema.agg_daily_metrics (metric_date, metric_name, value, computed_at)
SELECT
    cohort_date + day_number                              AS metric_date,
    if(day_number = 1, 'retention_d1', 'retention_d7')    AS metric_name,
    retention_rate                                        AS value,
    {computed_at:DateTime64(3, 'UTC')}
FROM cinema.agg_retention_cohort FINAL
WHERE day_number IN (1, 7)
  AND cohort_date + day_number BETWEEN {date_from:Date} AND {date_to:Date}
"""

RAW_EVENTS_IN_RANGE = """
SELECT count() FROM cinema.movie_events FINAL
WHERE event_date BETWEEN {date_from:Date} AND {date_to:Date}
"""

READ_DAILY_METRICS = """
SELECT metric_date, metric_name, value
FROM cinema.agg_daily_metrics FINAL
WHERE metric_date BETWEEN {date_from:Date} AND {date_to:Date}
  AND computed_at = {computed_at:DateTime64(3, 'UTC')}
ORDER BY metric_date, metric_name
"""

READ_TOP_MOVIES = """
SELECT metric_date, rank, movie_id, views, unique_viewers, finishes, likes
FROM cinema.agg_top_movies FINAL
WHERE metric_date BETWEEN {date_from:Date} AND {date_to:Date}
  AND computed_at = {computed_at:DateTime64(3, 'UTC')}
  AND rank <= {limit:UInt32}
ORDER BY metric_date, rank
"""

READ_RETENTION_COHORTS = """
SELECT cohort_date, day_number, cohort_size, retained_users
FROM cinema.agg_retention_cohort FINAL
WHERE cohort_date BETWEEN {cohort_from:Date} AND {cohort_to:Date}
  AND computed_at = {computed_at:DateTime64(3, 'UTC')}
ORDER BY cohort_date, day_number
"""
