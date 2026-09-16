#!/usr/bin/env sh
set -e

echo "Ожидание готовности Postgres и RabbitMQ..."
python /srv/docker/wait_for_deps.py

echo "Запуск consumer..."
exec faststream run app.broker.consumer:app
