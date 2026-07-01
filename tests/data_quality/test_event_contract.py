from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from db.migrate import discover_migrations
from services.api.repository import (
    FeedbackMismatchError,
    feedback_fingerprint,
    verify_feedback_prediction,
)
from services.contracts import FeedbackRequest


def feedback(**overrides: object) -> FeedbackRequest:
    values: dict[str, object] = {
        "event_id": uuid.uuid4(),
        "request_id": uuid.uuid4(),
        "client_id": "client-1",
        "product_id": "product-1",
        "event_type": "purchase",
        "shown_discount": 0.1,
        "purchase_value": 80.0,
        "discount_cost": 8.0,
        "timestamp": datetime(2026, 7, 3, 12, tzinfo=UTC),
    }
    values.update(overrides)
    return FeedbackRequest.model_validate(values)


def test_feedback_fingerprint_is_stable_and_payload_sensitive() -> None:
    event = feedback()
    assert feedback_fingerprint(event) == feedback_fingerprint(event)
    changed = event.model_copy(update={"purchase_value": 81.0})
    assert feedback_fingerprint(event) != feedback_fingerprint(changed)


def test_prediction_verification_checks_client_product_and_discount() -> None:
    event = feedback()
    payload = {
        "recommendations": [
            {"product_id": "product-1", "discount": 0.1},
        ]
    }
    verify_feedback_prediction(event, "client-1", payload)
    with pytest.raises(FeedbackMismatchError, match="client_id"):
        verify_feedback_prediction(event, "client-2", payload)
    with pytest.raises(FeedbackMismatchError, match="product_id"):
        verify_feedback_prediction(event, "client-1", {"recommendations": []})
    with pytest.raises(FeedbackMismatchError, match="shown_discount"):
        verify_feedback_prediction(
            event,
            "client-1",
            {"recommendations": [{"product_id": "product-1", "discount": 0.05}]},
        )


def test_feedback_requires_timezone_and_nonnegative_values() -> None:
    with pytest.raises(ValueError, match="timezone"):
        feedback(timestamp=datetime(2026, 7, 3, 12))
    with pytest.raises(ValueError, match="greater than or equal"):
        feedback(discount_cost=-1)


def test_migrations_are_numbered_and_immutable_inputs() -> None:
    migrations = discover_migrations(Path("db/migrations"))
    assert [migration.version for migration in migrations] == ["001", "002", "003", "004"]
    assert len(migrations[0].checksum) == 64
    assert "prediction_events" in migrations[0].sql
    assert "feedback_events" in migrations[0].sql
    assert "feedback_export_state" in migrations[1].sql
    assert "fallback_recommendations" in migrations[2].sql
    assert "fallback_snapshot_state" in migrations[2].sql
