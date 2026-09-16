import asyncio
import json
import logging

import aio_pika

from app.broker.topology import declare_topology
from app.config import settings
from app.database import session_scope
from app.repositories.outbox_repository import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxRelay:
    """Фоновый процесс (background task внутри api-сервиса), который
    вычитывает неопубликованные записи из outbox и публикует их в
    RabbitMQ. Это вторая половина Outbox pattern: сам outbox гарантирует,
    что событие не потеряется на записи, а relay — что оно рано или
    поздно будет опубликовано (at-least-once; polling с интервалом
    outbox_poll_interval_sec).
    """

    def __init__(self) -> None:
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        self._channel = await self._connection.channel()
        exchange_main, _, _ = await declare_topology(self._channel)
        self._exchange = exchange_main
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name="outbox-relay")
        logger.info("Outbox relay запущен")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            await self._task
        if self._connection is not None:
            await self._connection.close()
        logger.info("Outbox relay остановлен")

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                published = await self._publish_batch()
            except Exception:
                logger.exception("Ошибка в цикле outbox relay")
                published = 0

            if published == 0:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=settings.outbox_poll_interval_sec,
                    )
                except asyncio.TimeoutError:
                    pass

    async def _publish_batch(self) -> int:
        async with session_scope() as session:
            repo = OutboxRepository(session)
            events = await repo.fetch_unpublished(settings.outbox_batch_size)
            if not events:
                return 0

            for event in events:
                message = aio_pika.Message(
                    body=json.dumps(event.payload).encode(),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={"x-retry-count": 0, "event_type": event.event_type},
                )
                await self._exchange.publish(message, routing_key=event.routing_key)
                await repo.mark_published(event)

            await session.commit()
            return len(events)
