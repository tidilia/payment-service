from collections.abc import AsyncGenerator

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.services.payment_service import PaymentService


async def verify_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> None:
    if x_api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный или отсутствующий X-API-Key",
        )


async def get_payment_service(
    session: AsyncSession = Depends(get_db),
) -> AsyncGenerator[PaymentService, None]:
    yield PaymentService(session)
