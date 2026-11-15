"""
CinemaAbyss events-service — MVP проверки Kafka.

API (см. api-specification.yaml):
  GET  /api/events/health   -> {"status": true}
  POST /api/events/movie    -> публикация в топик movie-events
  POST /api/events/user     -> публикация в топик user-events
  POST /api/events/payment  -> публикация в топик payment-events

Producer публикует событие и возвращает partition/offset (201).
Consumer в фоне читает все три топика и пишет обработанные события в лог.
"""

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logging.getLogger("aiokafka").setLevel(logging.WARNING)
log = logging.getLogger("events")

PORT = int(os.getenv("PORT", "8082"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "events-service")
CONNECT_RETRY_SECONDS = float(os.getenv("KAFKA_CONNECT_RETRY_SECONDS", "5"))

TOPICS = {
    "movie": "movie-events",
    "user": "user-events",
    "payment": "payment-events",
}


# --- Модели запросов (api-specification.yaml) ---

class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[list[str]] = None
    description: Optional[str] = None


class UserEvent(BaseModel):
    user_id: int
    action: str
    timestamp: datetime
    username: Optional[str] = None
    email: Optional[str] = None


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: datetime
    method_type: Optional[str] = None


# --- Kafka ---

class KafkaClients:
    def __init__(self) -> None:
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer_ready = False
        self.consumer_ready = False

    async def start_producer(self) -> None:
        while True:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode(),
                key_serializer=lambda k: k.encode() if k else None,
                acks="all",
            )
            try:
                await producer.start()
            except KafkaError as exc:
                log.warning("Producer: Kafka недоступна (%s), повтор через %ss", exc, CONNECT_RETRY_SECONDS)
                await producer.stop()
                await asyncio.sleep(CONNECT_RETRY_SECONDS)
                continue
            self.producer = producer
            self.producer_ready = True
            log.info("Producer подключён к %s", KAFKA_BROKERS)
            return

    async def run_consumer(self) -> None:
        while True:
            consumer = AIOKafkaConsumer(
                *TOPICS.values(),
                bootstrap_servers=KAFKA_BROKERS,
                group_id=CONSUMER_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode()),
            )
            try:
                await consumer.start()
            except KafkaError as exc:
                log.warning("Consumer: Kafka недоступна (%s), повтор через %ss", exc, CONNECT_RETRY_SECONDS)
                await consumer.stop()
                await asyncio.sleep(CONNECT_RETRY_SECONDS)
                continue

            self.consumer = consumer
            self.consumer_ready = True
            log.info("Consumer подписан на топики %s (group=%s)", list(TOPICS.values()), CONSUMER_GROUP)
            try:
                async for msg in consumer:
                    process_event(msg.topic, msg.partition, msg.offset, msg.value)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — consumer не должен умирать молча
                log.exception("Consumer: ошибка чтения, переподключение")
                await asyncio.sleep(CONNECT_RETRY_SECONDS)
            finally:
                self.consumer_ready = False
                await consumer.stop()

    async def stop(self) -> None:
        if self.producer:
            await self.producer.stop()


def process_event(topic: str, partition: int, offset: int, event: dict[str, Any]) -> None:
    """Обработка события consumer'ом: в MVP — запись в лог."""
    log.info(
        "Processed event: topic=%s partition=%s offset=%s id=%s type=%s payload=%s",
        topic, partition, offset, event.get("id"), event.get("type"),
        json.dumps(event.get("payload"), ensure_ascii=False, default=str),
    )


kafka = KafkaClients()


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer_task = asyncio.create_task(kafka.start_producer())
    consumer_task = asyncio.create_task(kafka.run_consumer())
    log.info("events-service started on port %s, brokers=%s", PORT, KAFKA_BROKERS)
    yield
    for task in (producer_task, consumer_task):
        task.cancel()
    await asyncio.gather(producer_task, consumer_task, return_exceptions=True)
    await kafka.stop()


app = FastAPI(title="CinemaAbyss events-service", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=400, content={"error": f"Invalid request: {exc.errors()}"})


async def publish(event_type: str, entity_id: Any, action: str, payload: dict[str, Any]) -> JSONResponse:
    if not kafka.producer_ready or kafka.producer is None:
        return JSONResponse(status_code=503, content={"error": "Kafka producer is not ready"})

    event = {
        "id": f"{event_type}-{entity_id}-{action}-{uuid.uuid4().hex[:8]}",
        "type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    topic = TOPICS[event_type]
    try:
        meta = await kafka.producer.send_and_wait(topic, value=event, key=str(entity_id))
    except KafkaError as exc:
        log.error("Ошибка публикации в %s: %s", topic, exc)
        return JSONResponse(status_code=500, content={"error": f"Kafka publish failed: {exc}"})

    log.info("Published event: topic=%s partition=%s offset=%s id=%s", topic, meta.partition, meta.offset, event["id"])
    return JSONResponse(
        status_code=201,
        content={"status": "success", "partition": meta.partition, "offset": meta.offset, "event": event},
    )


@app.get("/api/events/health")
async def health():
    return {
        "status": True,
        "kafka": {"producer": kafka.producer_ready, "consumer": kafka.consumer_ready},
    }


@app.post("/api/events/movie", status_code=201)
async def create_movie_event(body: MovieEvent):
    return await publish("movie", body.movie_id, body.action, body.model_dump(mode="json", exclude_none=True))


@app.post("/api/events/user", status_code=201)
async def create_user_event(body: UserEvent):
    return await publish("user", body.user_id, body.action, body.model_dump(mode="json", exclude_none=True))


@app.post("/api/events/payment", status_code=201)
async def create_payment_event(body: PaymentEvent):
    return await publish("payment", body.payment_id, body.status, body.model_dump(mode="json", exclude_none=True))
