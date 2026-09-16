from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки приложения, читаются из переменных окружения / .env."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Postgres
    database_url: str = (
        "postgresql+asyncpg://payments:payments@postgres:5432/payments"
    )

    # RabbitMQ
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"

    # Auth
    api_key: str = "change-me"

    # Broker topology
    exchange_main: str = "payments"
    exchange_retry: str = "payments.retry"
    exchange_dlx: str = "payments.dlx"

    queue_new: str = "payments.new"
    queue_retry: str = "payments.retry"
    queue_dlq: str = "payments.dlq"

    routing_key_new: str = "payments.new"
    routing_key_retry: str = "payments.retry"
    routing_key_dlq: str = "payments.dlq"

    # Retry policy
    max_retries: int = 3
    retry_base_delay_ms: int = 2000  # 2s, 4s, 8s (exponential)

    # Payment gateway emulation
    gateway_min_delay_sec: float = 2.0
    gateway_max_delay_sec: float = 5.0
    gateway_failure_rate: float = 0.10

    # Webhook delivery
    webhook_max_attempts: int = 3
    webhook_timeout_sec: float = 5.0

    # Outbox relay
    outbox_poll_interval_sec: float = 0.5
    outbox_batch_size: int = 20


settings = Settings()
