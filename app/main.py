import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.payments import router as payments_router
from app.services.outbox_relay import OutboxRelay

logging.basicConfig(level=logging.INFO)

outbox_relay = OutboxRelay()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await outbox_relay.start()
    yield
    await outbox_relay.stop()


app = FastAPI(
    title="Payment Processing Service",
    description="Асинхронный сервис процессинга платежей",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(payments_router)


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}
