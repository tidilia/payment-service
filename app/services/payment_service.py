from decimal import Decimal
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Currency, Payment
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.payment_repository import PaymentRepository


class PaymentService:
    """Бизнес-логика платежей. Не знает про SQLAlchemy напрямую — только
    через репозитории; не знает про RabbitMQ напрямую — только через
    outbox (фактическая публикация делается outbox_relay).
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create_payment(
        self,
        *,
        amount: Decimal,
        currency: Currency,
        description: str | None,
        metadata: dict[str, Any] | None,
        webhook_url: str,
        idempotency_key: str,
    ) -> tuple[Payment, bool]:
        """Идемпотентное создание платежа.

        Если платёж с таким idempotency_key уже существует — возвращает
        его же, не создавая новую запись и не публикуя повторно событие.
        Возвращает (payment, created), где created=False означает "это
        повтор, платёж уже существовал".

        Payment и Outbox-событие вставляются в ОДНОЙ транзакции — это и
        есть Outbox pattern: событие не потеряется, даже если сервис
        упадёт сразу после коммита, до публикации в RabbitMQ.
        """
        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            return existing, False

        payment = self._payments.build(
            amount=amount,
            currency=currency,
            description=description,
            metadata=metadata,
            webhook_url=webhook_url,
            idempotency_key=idempotency_key,
        )
        self._payments.add(payment)
        await self._session.flush()  # получаем payment.id до коммита

        outbox_event = self._outbox.build(
            event_type="payment.created",
            routing_key="payments.new",
            payload={"payment_id": str(payment.id)},
        )
        self._outbox.add(outbox_event)

        try:
            await self._session.commit()
        except IntegrityError:
            # Гонка: параллельный запрос с тем же Idempotency-Key успел
            # закоммититься первым (нарушение UNIQUE constraint).
            # Не ошибка клиента — просто возвращаем уже существующий платёж.
            await self._session.rollback()
            existing = await self._payments.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                return existing, False
            raise
        except Exception:
            await self._session.rollback()
            raise

        await self._session.refresh(payment)
        return payment, True

    async def get_payment(self, payment_id) -> Payment | None:
        return await self._payments.get_by_id(payment_id)
