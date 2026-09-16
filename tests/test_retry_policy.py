import pytest

from app.broker.retry_policy import (
    calculate_retry_delay_ms,
    get_retry_count,
    should_send_to_dlq,
)


@pytest.mark.parametrize(
    "attempt, expected_ms",
    [
        (1, 2000),
        (2, 4000),
        (3, 8000),
    ],
)
def test_calculate_retry_delay_ms_exponential_backoff(attempt, expected_ms):
    assert calculate_retry_delay_ms(attempt, base_delay_ms=2000) == expected_ms


def test_calculate_retry_delay_ms_rejects_non_positive_attempt():
    with pytest.raises(ValueError):
        calculate_retry_delay_ms(0, base_delay_ms=2000)


def test_get_retry_count_defaults_to_zero_without_headers():
    assert get_retry_count(None) == 0
    assert get_retry_count({}) == 0


def test_get_retry_count_reads_header():
    assert get_retry_count({"x-retry-count": 2}) == 2


def test_get_retry_count_tolerates_malformed_header():
    assert get_retry_count({"x-retry-count": "not-a-number"}) == 0


@pytest.mark.parametrize(
    "next_attempt, max_retries, expected",
    [
        (1, 3, False),
        (3, 3, False),
        (4, 3, True),
    ],
)
def test_should_send_to_dlq(next_attempt, max_retries, expected):
    assert should_send_to_dlq(next_attempt, max_retries=max_retries) is expected
