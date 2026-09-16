import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base

# In-memory SQLite вместо реального Postgres — модели используют
# кросс-диалектные типы (sqlalchemy.Uuid, generic Enum), поэтому схема
# создаётся идентично на обоих бэкендах. Для юнит-тестов бизнес-логики
# (идемпотентность, outbox) реального Postgres не требуется; сама
# Postgres-специфика (миграции, ENUM в БД) проверяется docker compose,
# а не этим набором тестов.


@pytest_asyncio.fixture
async def db_session() -> AsyncSession:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=engine, expire_on_commit=False, autoflush=False
    )
    async with session_factory() as session:
        yield session

    await engine.dispose()
