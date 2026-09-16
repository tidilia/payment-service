"""Ждёт готовности Postgres и RabbitMQ перед стартом api/consumer.

docker-compose healthcheck-ов обычно достаточно, но т.к. api и consumer
запускаются как обычные (не depends_on: condition: service_healthy для
всех оркестраторов гарантированно поддерживается) — подстраховываемся
явным ожиданием на уровне приложения.
"""

import asyncio
import sys
import time

import aio_pika
import asyncpg

from app.config import settings

TIMEOUT_SEC = 60
INTERVAL_SEC = 1.5


async def wait_for_postgres() -> None:
    dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    deadline = time.monotonic() + TIMEOUT_SEC
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = await asyncpg.connect(dsn)
            await conn.close()
            print("Postgres готов")
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            await asyncio.sleep(INTERVAL_SEC)
    print(f"Postgres не готов после {TIMEOUT_SEC}с: {last_error}", file=sys.stderr)
    sys.exit(1)


async def wait_for_rabbitmq() -> None:
    deadline = time.monotonic() + TIMEOUT_SEC
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            connection = await aio_pika.connect(settings.rabbitmq_url)
            await connection.close()
            print("RabbitMQ готов")
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            await asyncio.sleep(INTERVAL_SEC)
    print(f"RabbitMQ не готов после {TIMEOUT_SEC}с: {last_error}", file=sys.stderr)
    sys.exit(1)


async def main() -> None:
    await wait_for_postgres()
    await wait_for_rabbitmq()


if __name__ == "__main__":
    asyncio.run(main())
