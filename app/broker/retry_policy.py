"""Чистые функции retry-политики — вынесены отдельно от I/O-кода
consumer'а, чтобы их можно было протестировать без RabbitMQ/БД.
"""

from typing import Mapping


def calculate_retry_delay_ms(attempt: int, *, base_delay_ms: int) -> int:
    """Экспоненциальный backoff: attempt=1 -> base, attempt=2 -> 2*base,
    attempt=3 -> 4*base (т.е. base * 2**(attempt-1)).

    Например, при base_delay_ms=2000: 2000мс, 4000мс, 8000мс.
    """
    if attempt < 1:
        raise ValueError("attempt должен быть >= 1")
    return base_delay_ms * (2 ** (attempt - 1))


def get_retry_count(headers: Mapping[str, object] | None) -> int:
    """Достаёт текущий номер попытки из заголовков сообщения.
    Отсутствие заголовка (первая доставка) трактуется как 0.
    """
    if not headers:
        return 0
    try:
        return int(headers.get("x-retry-count", 0) or 0)
    except (TypeError, ValueError):
        return 0


def should_send_to_dlq(next_attempt: int, *, max_retries: int) -> bool:
    return next_attempt > max_retries
