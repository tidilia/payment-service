"""initial: payments and outbox tables

Revision ID: 0001
Revises:
Create Date: 2026-09-16

"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    currency_enum = postgresql.ENUM(
        "RUB", "USD", "EUR", name="currency", create_type=True
    )
    payment_status_enum = postgresql.ENUM(
        "pending", "succeeded", "failed", name="payment_status", create_type=True
    )
    outbox_status_enum = postgresql.ENUM(
        "pending", "published", "failed", name="outbox_status", create_type=True
    )

    bind = op.get_bind()
    currency_enum.create(bind, checkfirst=True)
    payment_status_enum.create(bind, checkfirst=True)
    outbox_status_enum.create(bind, checkfirst=True)

    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column(
            "currency",
            postgresql.ENUM(
                "RUB", "USD", "EUR", name="currency", create_type=False
            ),
            nullable=False,
        ),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column(
            "status",
            postgresql.ENUM(
                "pending",
                "succeeded",
                "failed",
                name="payment_status",
                create_type=False,
            ),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("webhook_url", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_payments_idempotency_key",
        "payments",
        ["idempotency_key"],
        unique=True,
    )

    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column("routing_key", sa.String(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "status",
            postgresql.ENUM(
                "pending",
                "published",
                "failed",
                name="outbox_status",
                create_type=False,
            ),
            nullable=False,
            server_default="pending",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_outbox_status_created_at", "outbox", ["status", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_status_created_at", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("ix_payments_idempotency_key", table_name="payments")
    op.drop_table("payments")

    postgresql.ENUM(name="outbox_status").drop(op.get_bind(), checkfirst=True)
    postgresql.ENUM(name="payment_status").drop(op.get_bind(), checkfirst=True)
    postgresql.ENUM(name="currency").drop(op.get_bind(), checkfirst=True)
