# events-service (MVP Kafka)

Python 3.12, FastAPI + aiokafka. Producer и consumer работают в одном процессе.

## API

| Метод | Путь | Топик | Ответ |
|---|---|---|---|
| GET | `/api/events/health` | — | `{"status": true, "kafka": {...}}` |
| POST | `/api/events/movie` | `movie-events` | `201 {"status":"success","partition","offset","event"}` |
| POST | `/api/events/user` | `user-events` | `201` |
| POST | `/api/events/payment` | `payment-events` | `201` |

Тело запросов соответствует схемам `MovieEvent`, `UserEvent`, `PaymentEvent` из `api-specification.yaml`.
Неверное тело → `400`, Kafka недоступна → `503`.

## Как работает

1. POST-запрос валидируется, событие оборачивается в `{id, type, timestamp, payload}`
   и публикуется producer'ом в топик (`acks=all`, ключ — id сущности).
2. Consumer (group `events-service`) читает все три топика и пишет в лог:
   `Processed event: topic=... partition=... offset=... id=... payload=...`
3. Если Kafka ещё не поднялась, producer и consumer переподключаются каждые
   `KAFKA_CONNECT_RETRY_SECONDS` секунд, сервис при этом не падает.

## Переменные окружения

`PORT` (8082), `KAFKA_BROKERS` (`kafka:9092`), `KAFKA_CONSUMER_GROUP` (`events-service`),
`KAFKA_CONNECT_RETRY_SECONDS` (5), `LOG_LEVEL` (INFO).

## Проверка

```bash
curl -X POST http://localhost:8082/api/events/movie \
  -H 'Content-Type: application/json' \
  -d '{"movie_id":1,"title":"Inception","action":"viewed","user_id":1}'
docker logs -f cinemaabyss-events-service | grep "Processed event"
```
