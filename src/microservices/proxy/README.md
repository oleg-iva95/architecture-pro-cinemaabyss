# proxy-service (API Gateway, Strangler Fig)

Python 3.12, FastAPI + httpx.

## Маршрутизация

| Путь | Куда |
|---|---|
| `GET /health` | ответ прокси: `{"status": true, ...}` |
| `/api/movies/health` | movies-service |
| `/api/movies[/...]` | монолит или movies-service по фиче-флагу |
| `/api/events[/...]` | events-service |
| всё остальное | монолит |

## Фиче-флаг

| `GRADUAL_MIGRATION` | `MOVIES_MIGRATION_PERCENT` | Поведение |
|---|---|---|
| `true` | `0..100` | указанный процент запросов `/api/movies` идёт в movies-service, остальные в монолит |
| `false` | любое | весь трафик `/api/movies` идёт в movies-service |

Если в запросе есть заголовок `X-User-Id`, выбор цели зависит от хеша user_id:
один пользователь всегда попадает в одну и ту же цель.
Без заголовка цель выбирается случайно для каждого запроса.

В каждом ответе есть заголовок `X-Proxy-Upstream` (`monolith` / `movies-service` / `events-service`),
куда ушёл запрос. Если цель недоступна, прокси отвечает `502`.

## Переменные окружения

`PORT`, `MONOLITH_URL`, `MOVIES_SERVICE_URL`, `EVENTS_SERVICE_URL`,
`GRADUAL_MIGRATION`, `MOVIES_MIGRATION_PERCENT`, `UPSTREAM_TIMEOUT` (сек, по умолчанию 10), `LOG_LEVEL`.

## Проверка распределения

```bash
for i in $(seq 100); do curl -s -o /dev/null -D - http://localhost:8000/api/movies | grep -i x-proxy-upstream; done | sort | uniq -c
```
