"""Топология RabbitMQ.

    payments (topic exchange)
        └── payments.new (queue)            — основная очередь обработки

    payments.retry (topic exchange)
        └── payments.retry (queue)          — "парковочная" очередь.
            У каждого сообщения здесь СВОЙ TTL (per-message `expiration`,
            выставляется при публикации: 2s / 4s / 8s), а не TTL очереди —
            это и даёт экспоненциальный backoff без плагинов. По истечении
            TTL RabbitMQ сам дед-леттерит сообщение обратно в payments/payments.new.

    payments.dlx (topic exchange)
        └── payments.dlq (queue)            — финальная очередь для сообщений,
            не обработанных после max_retries попыток. Требует ручного разбора.

Consumer не полагается на автоматический dead-lettering по nack — решение
"ещё раз попробовать" или "отправить в DLQ" принимается явно в коде
(app/broker/consumer.py), т.к. только так можно посчитать номер попытки
и выставить экспоненциальную задержку.
"""

import aio_pika

from app.config import settings

DeclaredExchanges = tuple[
    aio_pika.abc.AbstractExchange,
    aio_pika.abc.AbstractExchange,
    aio_pika.abc.AbstractExchange,
]


async def declare_topology(
    channel: aio_pika.abc.AbstractChannel,
) -> DeclaredExchanges:
    """Идемпотентно объявляет все exchange/queue. Безопасно вызывать
    из нескольких процессов (api и consumer) при старте.

    Возвращает (exchange_main, exchange_retry, exchange_dlx).
    """
    exchange_main = await channel.declare_exchange(
        settings.exchange_main, aio_pika.ExchangeType.TOPIC, durable=True
    )
    exchange_retry = await channel.declare_exchange(
        settings.exchange_retry, aio_pika.ExchangeType.TOPIC, durable=True
    )
    exchange_dlx = await channel.declare_exchange(
        settings.exchange_dlx, aio_pika.ExchangeType.TOPIC, durable=True
    )

    queue_new = await channel.declare_queue(settings.queue_new, durable=True)
    await queue_new.bind(exchange_main, routing_key=settings.routing_key_new)

    queue_retry = await channel.declare_queue(
        settings.queue_retry,
        durable=True,
        arguments={
            "x-dead-letter-exchange": settings.exchange_main,
            "x-dead-letter-routing-key": settings.routing_key_new,
        },
    )
    await queue_retry.bind(exchange_retry, routing_key=settings.routing_key_retry)

    queue_dlq = await channel.declare_queue(settings.queue_dlq, durable=True)
    await queue_dlq.bind(exchange_dlx, routing_key=settings.routing_key_dlq)

    return exchange_main, exchange_retry, exchange_dlx
