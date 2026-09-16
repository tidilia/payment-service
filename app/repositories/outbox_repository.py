from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import Outbox, OutboxStatus


class OutboxRepository:
    """Доступ к таблице outbox."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def build(
        self, *, event_type: str, routing_key: str, payload: dict[str, Any]
    ) -> Outbox:
        return Outbox(
            event_type=event_type,
            routing_key=routing_key,
            payload=payload,
            status=OutboxStatus.PENDING,
        )

    def add(self, outbox_event: Outbox) -> None:
        self._session.add(outbox_event)

    async def fetch_unpublished(self, limit: int) -> list[Outbox]:
        """Забирает пачку неопубликованных событий с блокировкой строк,
        чтобы при нескольких инстансах relay-процесса они не конфликтовали
        (SELECT ... FOR UPDATE SKIP LOCKED).
        """
        result = await self._session.execute(
            select(Outbox)
            .where(Outbox.status == OutboxStatus.PENDING)
            .order_by(Outbox.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.scalars().all())

    async def mark_published(self, outbox_event: Outbox) -> None:
        outbox_event.status = OutboxStatus.PUBLISHED
        outbox_event.published_at = datetime.now(timezone.utc)
        self._session.add(outbox_event)

    async def mark_failed(self, outbox_event: Outbox) -> None:
        outbox_event.status = OutboxStatus.FAILED
        self._session.add(outbox_event)
