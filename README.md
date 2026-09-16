# Payment Processing Service

Асинхронный микросервис процессинга платежей: принимает запрос на оплату,
обрабатывает его через эмуляцию внешнего платёжного шлюза (RabbitMQ +
consumer) и уведомляет клиента о результате через webhook.

## Стек

FastAPI + Pydantic v2 · SQLAlchemy 2.0 (async) + asyncpg · PostgreSQL ·
RabbitMQ + FastStream · Alembic · Docker / docker-compose.

## Архитектура

```
Client
  │  POST /api/v1/payments (X-API-Key, Idempotency-Key)
  ▼
[api] ── одна транзакция ──▶ payments (status=pending) + outbox (payment.created)
  │
  │  background task (та же поставка api)
  ▼
[Outbox relay] ──▶ exchange "payments" ──▶ queue "payments.new"
                                                   │
                                                   ▼
                                            [consumer]
                                     эмуляция шлюза (2-5с, 90%/10%)
                                     update status ──▶ POST webhook_url
                                                   │
                                     ошибка обработки (техническая)
                                                   ▼
                              queue "payments.retry" (per-message TTL,
                              экспоненциальный backoff 2с/4с/8с)
                                                   │  TTL истёк
                                                   ▼
                                      обратно в "payments.new"
                                                   │  после 3 попыток
                                                   ▼
                                         queue "payments.dlq"
```

Слои кода: `api` → `services` → `repositories` → `models`. Сервисы не
знают про SQLAlchemy напрямую (только через репозитории) и про RabbitMQ
напрямую (только через outbox — публикацией занимается `outbox_relay`).

## Ключевые архитектурные решения

**Outbox pattern.** `PaymentService.create_payment` вставляет строки в
`payments` и `outbox` в одной транзакции (см.
`app/services/payment_service.py`). Событие никогда не потеряется, даже
если процесс упадёт сразу после коммита — `OutboxRelay` (фоновый asyncio
task внутри `api`, `app/services/outbox_relay.py`) поллит таблицу
`outbox` (`SELECT ... FOR UPDATE SKIP LOCKED`, чтобы можно было безопасно
масштабировать publisher) и публикует неотправленные события в RabbitMQ.

**Идемпотентность в двух местах:**
- API: уникальный `idempotency_key` — повторный `POST` с тем же ключом
  возвращает уже существующий платёж, новая запись не создаётся (учтена
  и гонка параллельных запросов — `IntegrityError` при коммите
  перехватывается, возвращается ранее созданный платёж).
- Consumer: перед обработкой проверяется `status` платежа — если он уже
  не `pending` (повторная доставка сообщения брокером), обработка и
  повторная отправка webhook пропускаются.

**Retry с экспоненциальным backoff без плагинов RabbitMQ.** Технический
сбой обработки сообщения (не бизнес-отказ платежа!) ловится в
`app/broker/consumer.py`: сообщение публикуется в "парковочную" очередь
`payments.retry` с per-message TTL (`expiration`, выставляется на
каждое сообщение индивидуально: 2с → 4с → 8с), у которой
`x-dead-letter-exchange` указывает обратно на `payments`/`payments.new`.
По истечении TTL RabbitMQ сам возвращает сообщение в основную очередь —
классический "parking lot" паттерн, не требующий
`delayed-message-exchange` плагина. После `max_retries` (3) попыток
сообщение публикуется в финальную `payments.dlq` для ручного разбора.

Важно: бизнес-неуспех платежа (шлюз вернул отказ, 10% по условию) — это
**ожидаемый финальный статус** (`failed`), а не техническая ошибка.
В этом случае webhook о неуспехе отправляется как обычно, а
retry/DLQ-механизм не участвует.

**Webhook retry** — отдельный контур (`app/webhook/sender.py`, `httpx` +
`tenacity`, до 3 попыток с экспоненциальной задержкой), не связанный с
retry обработки сообщения: платёж на момент отправки webhook уже
финализирован в БД, поэтому проблема с доставкой уведомления не должна
приводить к повторной обработке платежа.

**Repository pattern** — тонкий, без generic-абстракций и DI-контейнера:
`PaymentRepository` / `OutboxRepository` инкапсулируют SQL-запросы,
сервисы работают с ними, а не с `AsyncSession` напрямую.

## Запуск

```bash
cp .env.example .env      # при необходимости поменять API_KEY
docker compose up --build
```

Поднимутся: `postgres`, `rabbitmq` (UI на http://localhost:15672,
guest/guest), `api` (применяет миграции Alembic при старте, затем
поднимает FastAPI на :8000), `consumer` (FastStream-приложение).

Проверка:

```bash
curl http://localhost:8000/health
```

## Примеры запросов

Создание платежа (замените `webhook_url` на реально доступный вам URL,
например https://webhook.site/... для ручной проверки):

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me" \
  -H "Idempotency-Key: 3f6a6e2e-0f1a-4b8a-9c7e-1a2b3c4d5e6f" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Оплата заказа #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://webhook.site/your-unique-id"
      }'
```

Ответ `202 Accepted`:

```json
{"payment_id": "…", "status": "pending", "created_at": "…"}
```

Повторный вызов с тем же `Idempotency-Key` вернёт тот же `payment_id` и
не создаст новый платёж.

Получение статуса:

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: change-me"
```

Через 2-5 секунд `status` станет `succeeded` (90%) или `failed` (10%),
и на `webhook_url` придёт POST-запрос с телом:

```json
{
  "payment_id": "…",
  "status": "succeeded",
  "processed_at": "…",
  "amount": "1500.00",
  "currency": "RUB"
}
```

## Структура проекта

```
app/
├── main.py                 # FastAPI + lifespan (запуск outbox relay)
├── config.py                # настройки (pydantic-settings)
├── database.py               # async engine/session
├── models/                   # SQLAlchemy 2.0 модели: payment.py, outbox.py
├── schemas/                  # Pydantic v2 DTO
├── repositories/              # PaymentRepository, OutboxRepository
├── services/
│   ├── payment_service.py     # идемпотентное создание платежа + outbox
│   └── outbox_relay.py         # фоновый publisher: outbox → RabbitMQ
├── api/
│   ├── deps.py                 # X-API-Key auth, DI сервисов
│   └── v1/payments.py           # POST/GET эндпоинты
├── broker/
│   ├── topology.py              # exchange/queue/DLX/DLQ (общий источник правды)
│   ├── retry_policy.py          # чистые функции retry-политики (без I/O — юнит-тестируемые)
│   └── consumer.py              # FastStream-приложение (entrypoint контейнера consumer)
└── webhook/sender.py            # webhook POST + retry
migrations/                      # Alembic (payments, outbox)
docker/                          # entrypoint-скрипты, ожидание готовности зависимостей
tests/                           # юнит-тесты (pytest)
```

## Тесты

Юнит-тесты не лезут в Docker/Postgres/RabbitMQ — модели используют
кросс-диалектные типы SQLAlchemy 2.0 (`Uuid`, generic `Enum`), поэтому
для `PaymentService` тесты гоняются на in-memory SQLite; retry-политика
и webhook-отправка — чистые/замоканные, без сети вообще.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -v
```

Покрыто (самое необходимое, не претендует на полноту):
- `tests/test_payment_service.py` — идемпотентность создания платежа
  (повторный `Idempotency-Key` не создаёт вторую запись ни в `payments`,
  ни в `outbox`), Outbox pattern (событие пишется вместе с платежом),
  `get_payment`.
- `tests/test_retry_policy.py` — формула экспоненциального backoff
  (2с/4с/8с), разбор `x-retry-count` из заголовков (включая некорректные
  значения), граница перехода в DLQ после `max_retries`.
- `tests/test_webhook_sender.py` — webhook доставляется с первого раза,
  восстанавливается после временной ошибки, сдаётся после исчерпания
  попыток (`httpx.AsyncClient.post` замокан, реальных сетевых вызовов нет).

## Известные упрощения (за рамками ТЗ)

- Consumer в один поток обрабатывает по одному сообщению за раз
  (`prefetch_count` по умолчанию у FastStream) — для теста этого
  достаточно, горизонтальное масштабирование добавляется увеличением
  числа реплик контейнера `consumer`.
- Нет отдельной таблицы для отслеживания доставки webhook (только
  логирование финальной неудачи) — при необходимости легко добавляется
  как ещё одна колонка/таблица без изменения остальной архитектуры.
- API-ключ статический и один на всех клиентов, как и указано в ТЗ
  ("Статический API ключ в заголовке X-API-Key для всех эндпоинтов").
