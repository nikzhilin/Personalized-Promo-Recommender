"""FastAPI read layer for atomically published Redis recommendation snapshots."""

from __future__ import annotations

import os
import secrets
import time
import uuid
from asyncio import Lock
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import TypeAdapter, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from services.api.repository import (
    EventRepository,
    FeedbackConflictError,
    FeedbackMismatchError,
    PostgresEventRepository,
)
from services.contracts import (
    FeedbackRequest,
    FeedbackResponse,
    Recommendation,
    RecommendRequest,
    RecommendResponse,
)

RECOMMENDATION_LIST = TypeAdapter(list[Recommendation])
REQUESTS = Counter(
    "promo_api_http_requests_total", "HTTP requests", ("method", "path", "status")
)
LATENCY = Histogram(
    "promo_api_http_request_duration_seconds", "HTTP request latency", ("method", "path")
)
REDIS_LATENCY = Histogram("promo_api_redis_latency_seconds", "Redis operation latency")
DATABASE_LATENCY = Histogram("promo_api_database_latency_seconds", "Database operation latency")
FALLBACKS = Counter("promo_api_fallback_total", "Fallback recommendation responses")
ERRORS = Counter("promo_api_errors_total", "API errors", ("kind",))
FEEDBACK = Counter(
    "promo_api_feedback_events_total", "Feedback API results", ("status", "verification")
)


@dataclass(frozen=True)
class SnapshotCacheValue:
    snapshot_id: str
    metadata: dict[Any, Any]
    expires_at: float


class SnapshotCache:
    def __init__(self, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            raise ValueError("snapshot cache TTL must be positive")
        self.ttl_seconds = ttl_seconds
        self.value: SnapshotCacheValue | None = None
        self.lock = Lock()

    def current(self) -> SnapshotCacheValue | None:
        value = self.value
        return value if value is not None and value.expires_at > time.monotonic() else None

    def replace(self, snapshot_id: str, metadata: dict[Any, Any]) -> SnapshotCacheValue:
        value = SnapshotCacheValue(
            snapshot_id=snapshot_id,
            metadata=metadata,
            expires_at=time.monotonic() + self.ttl_seconds,
        )
        self.value = value
        return value


def _decode(value: Any) -> str | None:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


def create_app(
    redis_client: Any | None = None,
    event_repository: EventRepository | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        owned = redis_client is None
        owned_repository = event_repository is None
        client = redis_client or Redis.from_url(
            os.getenv("REDIS_URL", "redis://redis:6379/0"),
            socket_connect_timeout=float(os.getenv("REDIS_CONNECT_TIMEOUT", "0.2")),
            socket_timeout=float(os.getenv("REDIS_SOCKET_TIMEOUT", "0.2")),
            decode_responses=False,
        )
        repository = event_repository or PostgresEventRepository(os.environ["DATABASE_URL"])
        app.state.redis = client
        app.state.events = repository
        app.state.snapshot_cache = SnapshotCache(
            float(os.getenv("SNAPSHOT_CACHE_TTL_SECONDS", "30"))
        )
        repository_opened = False
        try:
            if owned_repository:
                await repository.open()
                repository_opened = True
            yield
        finally:
            if repository_opened:
                await repository.close()
            if owned:
                await client.aclose()

    app = FastAPI(title="Personalized Promo Recommender", version="0.1.0", lifespan=lifespan)
    key_prefix = os.getenv("REDIS_KEY_PREFIX", "promo")

    @app.middleware("http")
    async def observe(request: Request, call_next: Any) -> Response:
        started = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            path = request.url.path
            REQUESTS.labels(request.method, path, str(status)).inc()
            LATENCY.labels(request.method, path).observe(time.perf_counter() - started)

    async def redis_call(operation: Any) -> Any:
        started = time.perf_counter()
        try:
            return await operation
        except RedisError as error:
            ERRORS.labels("redis").inc()
            raise HTTPException(
                status_code=503, detail="recommendation cache unavailable"
            ) from error
        finally:
            REDIS_LATENCY.observe(time.perf_counter() - started)

    async def database_call(operation: Any) -> Any:
        started = time.perf_counter()
        try:
            return await operation
        except (FeedbackConflictError, FeedbackMismatchError):
            raise
        except Exception as error:
            ERRORS.labels("database").inc()
            raise HTTPException(status_code=503, detail="event database unavailable") from error
        finally:
            DATABASE_LATENCY.observe(time.perf_counter() - started)

    async def load_snapshot() -> SnapshotCacheValue:
        snapshot_id = _decode(
            await redis_call(app.state.redis.get(f"{key_prefix}:active_snapshot"))
        )
        if not snapshot_id:
            raise HTTPException(status_code=503, detail="active recommendation snapshot missing")
        metadata = await redis_call(
            app.state.redis.hgetall(f"{key_prefix}:{snapshot_id}:meta")
        )
        if not metadata:
            raise HTTPException(status_code=503, detail="active snapshot metadata missing")
        return app.state.snapshot_cache.replace(snapshot_id, metadata)

    async def active_snapshot(*, force: bool = False) -> SnapshotCacheValue:
        cache: SnapshotCache = app.state.snapshot_cache
        if not force and (current := cache.current()) is not None:
            return current
        async with cache.lock:
            if not force and (current := cache.current()) is not None:
                return current
            return await load_snapshot()

    async def postgres_fallback(limit: int) -> tuple[str, list[Recommendation]]:
        result = await database_call(app.state.events.get_fallback_recommendations(limit))
        if result is None:
            raise HTTPException(status_code=503, detail="fallback recommendations unavailable")
        FALLBACKS.inc()
        return result

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready() -> dict[str, str]:
        snapshot = await active_snapshot(force=True)
        await database_call(app.state.events.ping())
        return {"status": "ready", "snapshot_id": snapshot.snapshot_id}

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/v1/recommend", response_model=RecommendResponse)
    async def recommend(payload: RecommendRequest) -> RecommendResponse:
        try:
            snapshot = await active_snapshot()
            snapshot_id = snapshot.snapshot_id
            prefix = f"{key_prefix}:{snapshot_id}"
            encoded = await redis_call(
                app.state.redis.get(f"{prefix}:user:{payload.client_id}")
            )
            is_fallback = encoded is None
            if is_fallback:
                encoded = await redis_call(app.state.redis.get(f"{prefix}:fallback"))
                FALLBACKS.inc()
            if encoded is None:
                raise HTTPException(status_code=503, detail="snapshot payload missing")
            recommendations = RECOMMENDATION_LIST.validate_json(encoded)
            if not recommendations:
                raise HTTPException(status_code=503, detail="empty snapshot payload")
        except (HTTPException, ValidationError):
            ERRORS.labels("invalid_snapshot").inc()
            snapshot_id, recommendations = await postgres_fallback(payload.limit)
            is_fallback = True
        request_id = uuid.uuid4()
        response = RecommendResponse(
            request_id=str(request_id),
            client_id=payload.client_id,
            snapshot_id=snapshot_id,
            recommendations=recommendations[: payload.limit],
            is_fallback=is_fallback,
        )
        prediction_payload = {
            "recommendations": [
                recommendation.model_dump(mode="json")
                for recommendation in response.recommendations
            ],
            "limit": payload.limit,
            "context": payload.context,
            "is_fallback": is_fallback,
        }
        await database_call(
            app.state.events.log_prediction(
                request_id=request_id,
                client_id=payload.client_id,
                snapshot_id=snapshot_id,
                payload=prediction_payload,
                is_fallback=is_fallback,
            )
        )
        return response

    @app.post("/v1/admin/cache/reload", status_code=204)
    async def reload_cache(request: Request) -> Response:
        configured_key = os.getenv("ADMIN_API_KEY")
        supplied_key = request.headers.get("X-Admin-API-Key")
        if (
            not configured_key
            or not supplied_key
            or not secrets.compare_digest(configured_key, supplied_key)
        ):
            raise HTTPException(status_code=401, detail="invalid admin API key")
        await active_snapshot(force=True)
        return Response(status_code=204)

    @app.post("/v1/feedback", response_model=FeedbackResponse, status_code=202)
    async def feedback(payload: FeedbackRequest) -> FeedbackResponse:
        try:
            result = await database_call(app.state.events.record_feedback(payload))
        except FeedbackConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except FeedbackMismatchError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        warnings = (
            ["prediction request is missing; feedback was stored without verification"]
            if result.verification_status == "UNVERIFIED_MISSING_REQUEST"
            else []
        )
        FEEDBACK.labels(result.status, result.verification_status).inc()
        return FeedbackResponse(
            event_id=payload.event_id,
            status=result.status,
            verification_status=result.verification_status,
            warnings=warnings,
        )

    return app


app = create_app()
