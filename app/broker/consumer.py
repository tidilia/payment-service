"""Consumer-приложение на FastStream.

Слушает очередь payments.new. Технические сбои обработки (исключения —
недоступность БД, баг и т.п.) обрабатываются ВРУЧНУЮ: мы сами решаем,
публиковать ли сообщение в payments.retry (с экспоненциальным per-message
TTL) или, после max_retries попыток, в payments.dlq — и никогда не даём
исключению вылететь из хендлера наружу, поэтому FastStream всегда
подтверждает (ack) исходное сообщение из payments.new — повторной
доставкой брокера мы управляем сами через retry-очередь.

Бизнес-неуспех платежа (гейтвей отказал, 10% вероятность по ТЗ) —
ОЖИДАЕМЫЙ финальный результат (status=failed), а не техническая ошибка:
в этом случае retry/DLQ не задействуются, платёж считается обработанным.

Запуск: `faststream run app.broker.consumer:app` (см. Dockerfile/CMD).
"""

import asyncio
import json
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Mapping

import aio_pika
from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitMessage, RabbitQueue
from pydantic import BaseModel

from app.broker.retry_policy import (
    calculate_retry_delay_ms,
    get_retry_count,
    should_send_to_dlq,
)
from app.broker.topology import declare_topology
from app.config import settings
from app.database import session_scope
from app.models.payment import PaymentStatus
from app.repositories.payment_repository import PaymentRepository
from app.webhook.sender import send_webhook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

broker = RabbitBroker(settings.rabbitmq_url)
app = FastStream(broker)

exchange_main = RabbitExchange(settings.exchange_main, type="topic", durable=True)
queue_new = RabbitQueue(
    settings.queue_new, durable=True, routing_key=settings.routing_key_new
)

# Отдельное aio-pika соединение специально для публикации в retry/DLQ —
# так мы полностью контролируем заголовки и per-message TTL, не завися
# от деталей форвардинга kwargs в Broker.publish() конкретной версии
# FastStream. Топология объявляется через тот же declare_topology(), что
# использует api-сервис (app/services/outbox_relay.py) — одна точка правды.
_retry_connection: aio_pika.abc.AbstractRobustConnection | None = None
_retry_exchange: aio_pika.abc.AbstractExchange | None = None
_dlx_exchange: aio_pika.abc.AbstractExchange | None = None


class PaymentEvent(BaseModel):
    payment_id: str


def _extract_headers(msg: RabbitMessage) -> Mapping[str, object]:
    headers = getattr(msg, "headers", None)
    if not headers:
        raw = getattr(msg, "raw_message", None)
        headers = getattr(raw, "headers", None) or {}
    return headers


@app.on_startup
async def setup_retry_topology() -> None:
    global _retry_connection, _retry_exchange, _dlx_exchange
    _retry_connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    channel = await _retry_connection.channel()
    _, _retry_exchange, _dlx_exchange = await declare_topology(channel)
    logger.info("Consumer: топология RabbitMQ объявлена, retry-канал готов")


@app.on_shutdown
async def teardown_retry_topology() -> None:
    if _retry_connection is not None:
        await _retry_connection.close()


async def _process_payment(payment_id: uuid.UUID) -> None:
    async with session_scope() as session:
        repo = PaymentRepository(session)
        payment = await repo.get_by_id(payment_id)
        if payment is None:
            raise RuntimeError(f"Payment {payment_id} не найден в БД")

        if payment.status != PaymentStatus.PENDING:
            logger.info(
                "Платёж %s уже обработан (status=%s), повторная доставка "
                "сообщения — пропускаем (идемпотентность на уровне consumer)",
                payment_id,
                payment.status,
            )
            return

        await asyncio.sleep(
            random.uniform(
                settings.gateway_min_delay_sec, settings.gateway_max_delay_sec
            )
        )
        succeeded = random.random() >= settings.gateway_failure_rate
        new_status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
        processed_at = datetime.now(timezone.utc)

        await repo.update_status(payment, status=new_status, processed_at=processed_at)
        await session.commit()
        await session.refresh(payment)

    webhook_payload = {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "processed_at": payment.processed_at.isoformat(),
        "amount": str(payment.amount),
        "currency": payment.currency.value,
    }
    await send_webhook(payment.webhook_url, webhook_payload)


async def _schedule_retry(payment_id: str, attempt: int) -> None:
    delay_ms = calculate_retry_delay_ms(
        attempt, base_delay_ms=settings.retry_base_delay_ms
    )
    message = aio_pika.Message(
        body=json.dumps({"payment_id": payment_id}).encode(),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        headers={"x-retry-count": attempt},
        expiration=delay_ms,
    )
    await _retry_exchange.publish(message, routing_key=settings.routing_key_retry)
    logger.warning(
        "Платёж %s: попытка %s/%s не удалась, повтор через %sмс",
        payment_id,
        attempt,
        settings.max_retries,
        delay_ms,
    )


async def _send_to_dlq(payment_id: str, attempt: int, error: str) -> None:
    message = aio_pika.Message(
        body=json.dumps(
            {"payment_id": payment_id, "error": error, "attempts": attempt}
        ).encode(),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        headers={"x-retry-count": attempt},
    )
    await _dlx_exchange.publish(message, routing_key=settings.routing_key_dlq)
    logger.error(
        "Платёж %s: исчерпаны попытки (%s), сообщение отправлено в DLQ. Ошибка: %s",
        payment_id,
        attempt,
        error,
    )


@broker.subscriber(queue_new, exchange_main)
async def handle_payment_new(event: PaymentEvent, msg: RabbitMessage) -> None:
    attempt = get_retry_count(_extract_headers(msg))
    try:
        await _process_payment(uuid.UUID(event.payment_id))
    except Exception as exc:  # технический сбой обработки сообщения
        next_attempt = attempt + 1
        if should_send_to_dlq(next_attempt, max_retries=settings.max_retries):
            await _send_to_dlq(event.payment_id, next_attempt, str(exc))
        else:
            await _schedule_retry(event.payment_id, next_attempt)
        # Не пробрасываем исключение дальше: решение о повторе/DLQ уже
        # принято и опубликовано нами вручную, поэтому исходное сообщение
        # из payments.new подтверждается (ack) как штатно обработанное.
