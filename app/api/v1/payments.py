import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.api.deps import get_payment_service, verify_api_key
from app.schemas.payment import (
    PaymentCreate,
    PaymentCreateResponse,
    PaymentDetailResponse,
)
from app.services.payment_service import PaymentService

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post(
    "",
    response_model=PaymentCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    service: PaymentService = Depends(get_payment_service),
) -> PaymentCreateResponse:
    payment, _created = await service.create_payment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata=body.metadata,
        webhook_url=str(body.webhook_url),
        idempotency_key=idempotency_key,
    )
    return PaymentCreateResponse.model_validate(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentDetailResponse,
)
async def get_payment(
    payment_id: uuid.UUID,
    service: PaymentService = Depends(get_payment_service),
) -> PaymentDetailResponse:
    payment = await service.get_payment(payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Платёж не найден"
        )
    return PaymentDetailResponse.model_validate(payment)
