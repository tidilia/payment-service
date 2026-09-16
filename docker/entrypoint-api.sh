#!/usr/bin/env sh
set -e

echo "Ожидание готовности Postgres и RabbitMQ..."
python /srv/docker/wait_for_deps.py

echo "Применение миграций Alembic..."
alembic upgrade head

echo "Запуск API..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
