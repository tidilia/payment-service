from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.webhook.sender import send_webhook


@pytest.mark.asyncio
async def test_send_webhook_success_on_first_try():
    mock_response = httpx.Response(200, request=httpx.Request("POST", "https://x/hook"))
    with patch(
        "httpx.AsyncClient.post", new=AsyncMock(return_value=mock_response)
    ) as mock_post:
        ok = await send_webhook("https://x/hook", {"payment_id": "1"})

    assert ok is True
    mock_post.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_webhook_retries_then_succeeds():
    request = httpx.Request("POST", "https://x/hook")
    # httpx.Response.raise_for_status() raises httpx.HTTPStatusError on its
    # own for a >=400 response — no extra mocking needed for that part.
    failure = httpx.Response(500, request=request)
    success = httpx.Response(200, request=request)

    mock_post = AsyncMock(side_effect=[failure, success])
    with patch("httpx.AsyncClient.post", new=mock_post):
        ok = await send_webhook("https://x/hook", {"payment_id": "1"})

    assert ok is True
    assert mock_post.await_count == 2


@pytest.mark.asyncio
async def test_send_webhook_gives_up_after_max_attempts():
    request = httpx.Request("POST", "https://x/hook")
    failure = httpx.Response(500, request=request)

    with patch(
        "httpx.AsyncClient.post", new=AsyncMock(return_value=failure)
    ) as mock_post:
        ok = await send_webhook("https://x/hook", {"payment_id": "1"})

    assert ok is False
    # webhook_max_attempts по умолчанию = 3
    assert mock_post.await_count == 3
