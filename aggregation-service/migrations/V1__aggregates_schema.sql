-- Хранилище готовых метрик. Сюда пишет сервис агрегации после каждого цикла; отсюда читают другие
-- сервисы и экспорт в S3. Все отметки времени — timestamptz (UTC).

-- Справочник метрик: имя, единица измерения, определение.
CREATE TABLE metrics (
    name        text PRIMARY KEY,
    unit        text NOT NULL,
    description text NOT NULL
);

INSERT INTO metrics (name, unit, description) VALUES
    ('dau',                    'users',   'Уникальные пользователи с любым событием за день (UTC)'),
    ('events_total',           'events',  'Все события за день'),
    ('sessions',               'sessions','Уникальные session_id за день'),
    ('views_started',          'events',  'Число событий VIEW_STARTED за день'),
    ('views_finished',         'events',  'Число событий VIEW_FINISHED за день'),
    ('likes',                  'events',  'Число событий LIKED за день'),
    ('searches',               'events',  'Число событий SEARCHED за день'),
    ('view_conversion',        'ratio',   'VIEW_FINISHED / VIEW_STARTED за день, доля 0..1; NULL, если просмотров не было'),
    ('avg_watch_time_seconds', 'seconds', 'Средний progress_seconds у VIEW_FINISHED за день; NULL, если досмотров не было'),
    ('retention_d1',           'ratio',   'Доля когорты дня D-1, активной в день D (метрика дня D)'),
    ('retention_d7',           'ratio',   'Доля когорты дня D-7, активной в день D (метрика дня D)');

-- Дневные метрики. Повторный пересчёт: INSERT ... ON CONFLICT (metric_date, metric_name) DO UPDATE.
CREATE TABLE daily_metrics (
    metric_date date             NOT NULL,
    metric_name text             NOT NULL REFERENCES metrics (name),
    value       double precision,
    computed_at timestamptz      NOT NULL,
    PRIMARY KEY (metric_date, metric_name),
    CONSTRAINT daily_metrics_value_non_negative CHECK (value IS NULL OR value >= 0)
);

-- Топ фильмов дня. Пересчёт заменяет топ за день целиком в одной транзакции (DELETE + INSERT).
CREATE TABLE daily_top_movies (
    metric_date    date        NOT NULL,
    rank           smallint    NOT NULL CHECK (rank >= 1),
    movie_id       text        NOT NULL,
    views          bigint      NOT NULL CHECK (views > 0),
    unique_viewers bigint      NOT NULL CHECK (unique_viewers > 0 AND unique_viewers <= views),
    finishes       bigint      NOT NULL CHECK (finishes >= 0),
    likes          bigint      NOT NULL CHECK (likes >= 0),
    computed_at    timestamptz NOT NULL,
    PRIMARY KEY (metric_date, rank),
    UNIQUE (metric_date, movie_id)
);

-- Когортный retention: cohort_date — день первой активности, day_number — день жизни 0..7.
-- retention_rate вычисляется из двух хранимых столбцов и не может с ними разойтись.
CREATE TABLE retention_cohorts (
    cohort_date    date        NOT NULL,
    day_number     smallint    NOT NULL CHECK (day_number BETWEEN 0 AND 7),
    cohort_size    bigint      NOT NULL CHECK (cohort_size > 0),
    retained_users bigint      NOT NULL CHECK (retained_users BETWEEN 0 AND cohort_size),
    retention_rate double precision GENERATED ALWAYS AS (retained_users::double precision / cohort_size) STORED,
    computed_at    timestamptz NOT NULL,
    PRIMARY KEY (cohort_date, day_number)
);
