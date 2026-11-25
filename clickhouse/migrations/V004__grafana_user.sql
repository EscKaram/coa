-- Пользователь Grafana читает только таблицы агрегатов: дашборд не может обратиться к сырым событиям.
CREATE USER IF NOT EXISTS grafana IDENTIFIED WITH sha256_password BY '{{GRAFANA_PASSWORD}}';
GRANT SELECT ON cinema.agg_daily_metrics TO grafana;
GRANT SELECT ON cinema.agg_top_movies TO grafana;
GRANT SELECT ON cinema.agg_retention_cohort TO grafana;
