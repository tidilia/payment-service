import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.models.outbox import Outbox
from app.models.payment import Currency, Payment, PaymentStatus
from app.services.payment_service import PaymentService


def _payload(idempotency_key: str = "idem-key-1") -> dict:
    return dict(
        amount=Decimal("1500.00"),
        currency=Currency.RUB,
        description="Оплата заказа #42",
        metadata={"order_id": 42},
        webhook_url="https://example.com/webhook",
        idempotency_key=idempotency_key,
    )


async def test_create_payment_creates_pending_payment_and_outbox_event(db_session):
    service = PaymentService(db_session)

    payment, created = await service.create_payment(**_payload())

    assert created is True
    assert payment.status == PaymentStatus.PENDING
    assert payment.amount == Decimal("1500.00")

    # Outbox pattern: событие payment.created должно быть записано в той
    # же транзакции, что и сам платёж.
    result = await db_session.execute(select(Outbox))
    outbox_events = result.scalars().all()
    assert len(outbox_events) == 1
    assert outbox_events[0].payload == {"payment_id": str(payment.id)}
    assert outbox_events[0].routing_key == "payments.new"


async def test_create_payment_is_idempotent(db_session):
    service = PaymentService(db_session)
    payload = _payload(idempotency_key="same-key")

    first, first_created = await service.create_payment(**payload)
    second, second_created = await service.create_payment(**payload)

    assert first_created is True
    assert second_created is False
    assert first.id == second.id

    # Повторный вызов не должен создавать ни второй платёж, ни второе
    # outbox-событие.
    payments = (await db_session.execute(select(Payment))).scalars().all()
    outbox_events = (await db_session.execute(select(Outbox))).scalars().all()
    assert len(payments) == 1
    assert len(outbox_events) == 1


async def test_create_payment_different_keys_create_different_payments(db_session):
    service = PaymentService(db_session)

    first, _ = await service.create_payment(**_payload(idempotency_key="key-a"))
    second, _ = await service.create_payment(**_payload(idempotency_key="key-b"))

    assert first.id != second.id
    payments = (await db_session.execute(select(Payment))).scalars().all()
    assert len(payments) == 2


async def test_get_payment_returns_none_for_unknown_id(db_session):
    service = PaymentService(db_session)

    result = await service.get_payment(uuid.uuid4())

    assert result is None


async def test_get_payment_returns_created_payment(db_session):
    service = PaymentService(db_session)
    created, _ = await service.create_payment(**_payload())

    fetched = await service.get_payment(created.id)

    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.idempotency_key == created.idempotency_key
