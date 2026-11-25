# Grafana с предустановленным плагином ClickHouse: плагин скачивается при сборке образа, а не при каждом
# старте контейнера, поэтому запуск не зависит от доступности grafana.com и от тома с данными Grafana.
FROM grafana/grafana:11.6.0

ENV GF_PATHS_PLUGINS=/var/lib/grafana-plugins

USER root
RUN mkdir -p /var/lib/grafana-plugins \
    && grafana cli --pluginsDir /var/lib/grafana-plugins plugins install grafana-clickhouse-datasource 4.23.5 \
    && chown -R grafana:root /var/lib/grafana-plugins
USER grafana
