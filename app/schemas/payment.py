import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.models.payment import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    """Тело запроса POST /api/v1/payments."""

    amount: Decimal = Field(..., gt=0, description="Сумма платежа")
    currency: Currency
    description: str | None = None
    metadata: dict[str, Any] | None = None
    webhook_url: HttpUrl


class PaymentCreateResponse(BaseModel):
    """Ответ 202 Accepted на создание платежа."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentDetailResponse(BaseModel):
    """Ответ GET /api/v1/payments/{payment_id}."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any] | None = Field(
        default=None, validation_alias="payment_metadata"
    )
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
