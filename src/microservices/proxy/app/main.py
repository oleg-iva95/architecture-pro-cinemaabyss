"""
CinemaAbyss proxy-service (API Gateway) — Strangler Fig.

Маршрутизация:
  /health                -> ответ самого прокси
  /api/movies[/...]      -> монолит или movies-service (фиче-флаг + процент миграции)
  /api/events[/...]      -> events-service
  всё остальное          -> монолит

Фиче-флаг (переменные окружения):
  GRADUAL_MIGRATION=true   -> MOVIES_MIGRATION_PERCENT% запросов идёт в movies-service,
                              остальные в монолит
  GRADUAL_MIGRATION=false  -> весь трафик домена movies идёт в movies-service
                              (поведение по README.md)

Если в запросе есть заголовок X-User-Id, выбор цели детерминирован
(хеш user_id), чтобы пользователь не «прыгал» между монолитом и сервисом.
"""

import logging
import os
import random
import zlib
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("proxy")


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _env_percent(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, default))
    except ValueError:
        log.warning("Некорректное значение %s, используется %s", name, default)
        value = default
    return max(0, min(100, value))


PORT = int(os.getenv("PORT", "8000"))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080").rstrip("/")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081").rstrip("/")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082").rstrip("/")
GRADUAL_MIGRATION = _env_bool("GRADUAL_MIGRATION", False)
MOVIES_MIGRATION_PERCENT = _env_percent("MOVIES_MIGRATION_PERCENT", 0)
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "10"))

# Заголовки, которые нельзя пробрасывать через прокси (RFC 7230, 6.1)
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient(timeout=UPSTREAM_TIMEOUT, follow_redirects=False)
    log.info(
        "proxy-service started: port=%s monolith=%s movies=%s events=%s "
        "gradual_migration=%s movies_migration_percent=%s",
        PORT, MONOLITH_URL, MOVIES_SERVICE_URL, EVENTS_SERVICE_URL,
        GRADUAL_MIGRATION, MOVIES_MIGRATION_PERCENT,
    )
    yield
    await app.state.client.aclose()


app = FastAPI(title="CinemaAbyss proxy-service", lifespan=lifespan)


def _is_prefix(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix + "/")


def _route_to_movies_service(user_id: str | None) -> bool:
    """Решение фиче-флага: идёт ли запрос домена movies в новый сервис."""
    if not GRADUAL_MIGRATION:
        return True
    if MOVIES_MIGRATION_PERCENT >= 100:
        return True
    if MOVIES_MIGRATION_PERCENT <= 0:
        return False
    if user_id:
        bucket = zlib.crc32(user_id.encode()) % 100
    else:
        bucket = random.randrange(100)
    return bucket < MOVIES_MIGRATION_PERCENT


def resolve_upstream(path: str, user_id: str | None) -> tuple[str, str]:
    """Возвращает (имя цели, базовый URL) для пути запроса."""
    if _is_prefix(path, "/api/events"):
        return "events-service", EVENTS_SERVICE_URL
    if path == "/api/movies/health":
        return "movies-service", MOVIES_SERVICE_URL
    if _is_prefix(path, "/api/movies"):
        if _route_to_movies_service(user_id):
            return "movies-service", MOVIES_SERVICE_URL
        return "monolith", MONOLITH_URL
    return "monolith", MONOLITH_URL


@app.get("/health")
async def health():
    return {
        "status": True,
        "gradual_migration": GRADUAL_MIGRATION,
        "movies_migration_percent": MOVIES_MIGRATION_PERCENT,
    }


@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def proxy(full_path: str, request: Request):
    path = "/" + full_path
    target_name, target_base = resolve_upstream(path, request.headers.get("x-user-id"))
    url = target_base + path
    if request.url.query:
        url += "?" + request.url.query

    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() != "host"
    }
    client_host = request.client.host if request.client else ""
    headers["x-forwarded-for"] = ", ".join(
        filter(None, [request.headers.get("x-forwarded-for"), client_host])
    )
    body = await request.body()

    try:
        upstream = await request.app.state.client.request(
            request.method, url, headers=headers, content=body
        )
    except httpx.RequestError as exc:
        log.error("%s %s -> %s FAILED: %s", request.method, path, target_name, exc)
        return JSONResponse(
            status_code=502,
            content={"error": "upstream unavailable", "upstream": target_name},
            headers={"X-Proxy-Upstream": target_name},
        )

    log.info("%s %s -> %s [%s]", request.method, path, target_name, upstream.status_code)

    response_headers = {
        k: v for k, v in upstream.headers.items()
        if k.lower() not in HOP_BY_HOP
        and k.lower() not in ("content-length", "content-encoding")
    }
    response_headers["X-Proxy-Upstream"] = target_name
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=response_headers,
    )
