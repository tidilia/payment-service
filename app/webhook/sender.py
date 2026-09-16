import logging
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.config import settings

logger = logging.getLogger(__name__)


class WebhookDeliveryError(Exception):
    """Итоговая ошибка: webhook не удалось доставить после всех попыток."""


@retry(
    stop=stop_after_attempt(settings.webhook_max_attempts),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(httpx.HTTPError),
    reraise=True,
)
async def _post_with_retry(url: str, payload: dict[str, Any]) -> None:
    async with httpx.AsyncClient(timeout=settings.webhook_timeout_sec) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()


async def send_webhook(url: str, payload: dict[str, Any]) -> bool:
    """Отправляет webhook с retry (до webhook_max_attempts попыток,
    экспоненциальная задержка). Это отдельный retry-контур от retry
    обработки сообщения из очереди: платёж уже финализирован в БД,
    поэтому неудачная доставка webhook не должна приводить к повторной
    обработке платежа.

    Возвращает True при успехе, False — если все попытки исчерпаны
    (ошибка логируется, но не пробрасывается дальше).
    """
    try:
        await _post_with_retry(url, payload)
        return True
    except httpx.HTTPError as exc:
        logger.error(
            "Не удалось доставить webhook на %s после %s попыток: %s",
            url,
            settings.webhook_max_attempts,
            exc,
        )
        return False
