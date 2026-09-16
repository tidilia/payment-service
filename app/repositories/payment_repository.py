import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Currency, Payment, PaymentStatus


class PaymentRepository:
    """Доступ к таблице payments. Никакой бизнес-логики — только CRUD."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, payment_id: uuid.UUID) -> Payment | None:
        result = await self._session.execute(
            select(Payment).where(Payment.id == payment_id)
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        result = await self._session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()

    def build(
        self,
        *,
        amount: Decimal,
        currency: Currency,
        description: str | None,
        metadata: dict[str, Any] | None,
        webhook_url: str,
        idempotency_key: str,
    ) -> Payment:
        """Создаёт объект Payment (без flush/commit — транзакцией управляет сервис)."""
        return Payment(
            amount=amount,
            currency=currency,
            description=description,
            payment_metadata=metadata,
            webhook_url=webhook_url,
            idempotency_key=idempotency_key,
            status=PaymentStatus.PENDING,
        )

    def add(self, payment: Payment) -> None:
        self._session.add(payment)

    async def update_status(
        self,
        payment: Payment,
        *,
        status: PaymentStatus,
        processed_at,
    ) -> None:
        payment.status = status
        payment.processed_at = processed_at
        self._session.add(payment)
