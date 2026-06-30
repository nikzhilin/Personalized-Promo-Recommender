from __future__ import annotations

import json
import uuid
from typing import Any

from fastapi.testclient import TestClient
from redis.exceptions import RedisError

from services.api.app import create_app
from services.api.repository import (
    FeedbackConflictError,
    FeedbackMismatchError,
    FeedbackWriteResult,
)

SNAPSHOT_ID = "0123456789abcdef0123456789abcdef"
KNOWN_PAYLOAD = json.dumps(
    [
        {
            "product_id": "product-1",
            "discount": 0.1,
            "expected_profit": 4.25,
            "recsys_score": 0.8,
            "reason_code": "HIGH_INCREMENTAL_PROFIT",
            "rank": 1,
        },
        {
            "product_id": "product-2",
            "discount": 0.0,
            "expected_profit": 2.0,
            "recsys_score": 0.5,
            "reason_code": "REPEAT_PURCHASE",
            "rank": 2,
        },
    ]
).encode()
FALLBACK_PAYLOAD = json.dumps(
    [
        {
            "product_id": "popular-1",
            "discount": 0.0,
            "expected_profit": None,
            "recsys_score": 1.0,
            "reason_code": "COLD_START_POPULAR",
            "rank": 1,
        }
    ]
).encode()


class FakeRedis:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.values: dict[str, bytes] = {
            "promo:active_snapshot": SNAPSHOT_ID.encode(),
            f"promo:{SNAPSHOT_ID}:user:client-1": KNOWN_PAYLOAD,
            f"promo:{SNAPSHOT_ID}:fallback": FALLBACK_PAYLOAD,
        }
        self.hashes: dict[str, dict[bytes, bytes]] = {
            f"promo:{SNAPSHOT_ID}:meta": {b"client_count": b"1"}
        }

    def _check(self) -> None:
        if self.fail:
            raise RedisError("unavailable")

    async def get(self, key: str) -> bytes | None:
        self._check()
        return self.values.get(key)

    async def hgetall(self, key: str) -> dict[bytes, bytes]:
        self._check()
        return self.hashes.get(key, {})


class FakeEventRepository:
    def __init__(self) -> None:
        self.predictions: list[dict[str, Any]] = []
        self.feedback_result = FeedbackWriteResult("accepted", "VERIFIED")
        self.error: Exception | None = None
        self.fallback: tuple[str, list[Any]] | None = (
            "postgres-fallback-snapshot",
            json.loads(FALLBACK_PAYLOAD),
        )

    async def open(self) -> None: ...

    async def close(self) -> None: ...

    async def ping(self) -> None:
        if self.error:
            raise self.error

    async def log_prediction(self, **values: Any) -> None:
        if self.error:
            raise self.error
        self.predictions.append(values)

    async def record_feedback(self, feedback: Any) -> FeedbackWriteResult:
        if self.error:
            raise self.error
        return self.feedback_result

    async def get_fallback_recommendations(self, limit: int) -> tuple[str, list[Any]] | None:
        if self.error:
            raise self.error
        if self.fallback is None:
            return None
        snapshot_id, recommendations = self.fallback
        from services.contracts import Recommendation

        return snapshot_id, [
            Recommendation.model_validate(item) for item in recommendations[:limit]
        ]


def client(redis: Any, repository: FakeEventRepository | None = None) -> TestClient:
    return TestClient(create_app(redis, repository or FakeEventRepository()))


def test_health_and_metrics_endpoints() -> None:
    with client(FakeRedis()) as api:
        assert api.get("/health/live").json() == {"status": "ok"}
        ready = api.get("/health/ready")
        assert ready.status_code == 200
        assert ready.json()["snapshot_id"] == SNAPSHOT_ID
        metrics = api.get("/metrics")
        assert metrics.status_code == 200
        assert "promo_api_http_requests_total" in metrics.text


def test_known_client_honors_limit() -> None:
    repository = FakeEventRepository()
    with client(FakeRedis(), repository) as api:
        response = api.post(
            "/v1/recommend",
            json={"client_id": "client-1", "limit": 1, "context": {"page": "main"}},
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["snapshot_id"] == SNAPSHOT_ID
    assert payload["is_fallback"] is False
    assert [item["product_id"] for item in payload["recommendations"]] == ["product-1"]
    assert len(repository.predictions) == 1
    assert repository.predictions[0]["payload"]["context"] == {"page": "main"}
    assert len(repository.predictions[0]["payload"]["recommendations"]) == 1


def test_unknown_client_receives_fallback() -> None:
    with client(FakeRedis()) as api:
        response = api.post("/v1/recommend", json={"client_id": "unknown", "limit": 10})
    assert response.status_code == 200
    assert response.json()["is_fallback"] is True
    assert response.json()["recommendations"][0]["expected_profit"] is None


def test_invalid_limit_and_redis_outage() -> None:
    with client(FakeRedis()) as api:
        assert api.post(
            "/v1/recommend", json={"client_id": "client-1", "limit": 11}
        ).status_code == 422
    repository = FakeEventRepository()
    with client(FakeRedis(fail=True), repository) as api:
        assert api.get("/health/ready").status_code == 503
        response = api.post(
            "/v1/recommend", json={"client_id": "client-1", "limit": 1}
        )
        assert response.status_code == 200
        assert response.json()["snapshot_id"] == "postgres-fallback-snapshot"
        assert response.json()["is_fallback"] is True


def test_missing_redis_payload_uses_postgres_and_missing_db_returns_503() -> None:
    redis = FakeRedis()
    redis.values.pop(f"promo:{SNAPSHOT_ID}:user:client-1")
    redis.values.pop(f"promo:{SNAPSHOT_ID}:fallback")
    repository = FakeEventRepository()
    with client(redis, repository) as api:
        response = api.post("/v1/recommend", json={"client_id": "client-1", "limit": 1})
        assert response.status_code == 200
        assert response.json()["snapshot_id"] == "postgres-fallback-snapshot"

        repository.fallback = None
        assert api.post(
            "/v1/recommend", json={"client_id": "client-1", "limit": 1}
        ).status_code == 503


def test_admin_reload_requires_key_and_keeps_cached_snapshot_on_failure(monkeypatch: Any) -> None:
    monkeypatch.setenv("ADMIN_API_KEY", "test-admin-key")
    redis = FakeRedis()
    with client(redis) as api:
        assert api.post("/v1/admin/cache/reload").status_code == 401
        assert api.post(
            "/v1/admin/cache/reload", headers={"X-Admin-API-Key": "wrong"}
        ).status_code == 401
        assert api.post(
            "/v1/admin/cache/reload", headers={"X-Admin-API-Key": "test-admin-key"}
        ).status_code == 204
        redis.fail = True
        assert api.post(
            "/v1/admin/cache/reload", headers={"X-Admin-API-Key": "test-admin-key"}
        ).status_code == 503
        redis.fail = False
        assert api.post(
            "/v1/recommend", json={"client_id": "client-1", "limit": 1}
        ).status_code == 200


def feedback_payload() -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "client_id": "client-1",
        "product_id": "product-1",
        "event_type": "purchase",
        "shown_discount": 0.1,
        "purchase_value": 80.0,
        "discount_cost": 8.0,
        "timestamp": "2026-07-03T12:00:00Z",
    }


def test_feedback_accepts_verified_duplicate_and_missing_request() -> None:
    repository = FakeEventRepository()
    with client(FakeRedis(), repository) as api:
        accepted = api.post("/v1/feedback", json=feedback_payload())
        assert accepted.status_code == 202
        assert accepted.json()["verification_status"] == "VERIFIED"

        repository.feedback_result = FeedbackWriteResult("duplicate", "VERIFIED")
        duplicate = api.post("/v1/feedback", json=feedback_payload())
        assert duplicate.status_code == 202
        assert duplicate.json()["status"] == "duplicate"

        repository.feedback_result = FeedbackWriteResult(
            "accepted", "UNVERIFIED_MISSING_REQUEST"
        )
        missing = api.post("/v1/feedback", json=feedback_payload())
        assert missing.status_code == 202
        assert missing.json()["warnings"]


def test_feedback_maps_conflict_mismatch_and_database_failure() -> None:
    repository = FakeEventRepository()
    with client(FakeRedis(), repository) as api:
        repository.error = FeedbackConflictError("conflict")
        assert api.post("/v1/feedback", json=feedback_payload()).status_code == 409
        repository.error = FeedbackMismatchError("mismatch")
        assert api.post("/v1/feedback", json=feedback_payload()).status_code == 422
        repository.error = RuntimeError("database unavailable")
        assert api.post("/v1/feedback", json=feedback_payload()).status_code == 503
        assert api.post(
            "/v1/recommend", json={"client_id": "client-1", "limit": 1}
        ).status_code == 503
