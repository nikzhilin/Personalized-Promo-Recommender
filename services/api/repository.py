"""Async PostgreSQL persistence for prediction and feedback events."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from services.contracts import FeedbackRequest, Recommendation


class FeedbackConflictError(Exception):
    """The event ID already exists with a different payload."""


class FeedbackMismatchError(Exception):
    """Feedback does not match an existing prediction event."""


@dataclass(frozen=True)
class FeedbackWriteResult:
    status: str
    verification_status: str


class EventRepository(Protocol):
    async def open(self) -> None: ...

    async def close(self) -> None: ...

    async def ping(self) -> None: ...

    async def log_prediction(
        self,
        *,
        request_id: uuid.UUID,
        client_id: str,
        snapshot_id: str,
        payload: dict[str, Any],
        is_fallback: bool,
    ) -> None: ...

    async def record_feedback(self, feedback: FeedbackRequest) -> FeedbackWriteResult: ...

    async def get_fallback_recommendations(
        self, limit: int
    ) -> tuple[str, list[Recommendation]] | None: ...


def feedback_fingerprint(feedback: FeedbackRequest) -> str:
    canonical = json.dumps(
        feedback.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def verify_feedback_prediction(
    feedback: FeedbackRequest, prediction_client_id: str, payload: dict[str, Any]
) -> None:
    if prediction_client_id != feedback.client_id:
        raise FeedbackMismatchError("feedback client_id does not match prediction")
    recommendations = payload.get("recommendations")
    if not isinstance(recommendations, list):
        raise FeedbackMismatchError("prediction payload has no recommendations")
    match = next(
        (
            recommendation
            for recommendation in recommendations
            if recommendation.get("product_id") == feedback.product_id
        ),
        None,
    )
    if match is None:
        raise FeedbackMismatchError("feedback product_id was not shown")
    if Decimal(str(match.get("discount"))) != Decimal(str(feedback.shown_discount)):
        raise FeedbackMismatchError("feedback shown_discount does not match prediction")


class PostgresEventRepository:
    def __init__(self, database_url: str) -> None:
        self.pool = AsyncConnectionPool(database_url, min_size=1, max_size=4, open=False)

    async def open(self) -> None:
        await self.pool.open(wait=True)

    async def close(self) -> None:
        await self.pool.close()

    async def ping(self) -> None:
        async with self.pool.connection() as connection:
            await connection.execute("SELECT 1")

    async def log_prediction(
        self,
        *,
        request_id: uuid.UUID,
        client_id: str,
        snapshot_id: str,
        payload: dict[str, Any],
        is_fallback: bool,
    ) -> None:
        async with self.pool.connection() as connection:
            await connection.execute(
                """
                INSERT INTO prediction_events(
                    event_id, request_id, client_id, snapshot_id, payload,
                    is_fallback, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, now())
                """,
                (
                    uuid.uuid4(),
                    request_id,
                    client_id,
                    snapshot_id,
                    Jsonb(payload),
                    is_fallback,
                ),
            )

    async def get_fallback_recommendations(
        self, limit: int
    ) -> tuple[str, list[Recommendation]] | None:
        async with self.pool.connection() as connection, connection.transaction():
            state = await (
                await connection.execute(
                    "SELECT snapshot_id FROM fallback_snapshot_state WHERE singleton = TRUE"
                )
            ).fetchone()
            if state is None:
                return None
            rows = await (
                await connection.execute(
                    """
                    SELECT product_id, discount, expected_profit, recsys_score,
                           reason_code, rank
                    FROM fallback_recommendations
                    WHERE snapshot_id = %s
                    ORDER BY rank
                    LIMIT %s
                    """,
                    (state[0], limit),
                )
            ).fetchall()
            if not rows:
                return None
            return str(state[0]), [
                Recommendation(
                    product_id=row[0],
                    discount=float(row[1]),
                    expected_profit=None if row[2] is None else float(row[2]),
                    recsys_score=float(row[3]),
                    reason_code=row[4],
                    rank=int(row[5]),
                )
                for row in rows
            ]

    async def record_feedback(self, feedback: FeedbackRequest) -> FeedbackWriteResult:
        fingerprint = feedback_fingerprint(feedback)
        async with self.pool.connection() as connection, connection.transaction():
                existing = await (
                    await connection.execute(
                        """
                        SELECT event_fingerprint, verification_status
                        FROM feedback_events WHERE event_id = %s
                        """,
                        (feedback.event_id,),
                    )
                ).fetchone()
                if existing is not None:
                    if existing[0] != fingerprint:
                        raise FeedbackConflictError("event_id already has another payload")
                    return FeedbackWriteResult("duplicate", existing[1])

                prediction = await (
                    await connection.execute(
                        """
                        SELECT client_id, payload
                        FROM prediction_events WHERE request_id = %s
                        """,
                        (feedback.request_id,),
                    )
                ).fetchone()
                if prediction is None:
                    verification_status = "UNVERIFIED_MISSING_REQUEST"
                else:
                    verify_feedback_prediction(feedback, prediction[0], prediction[1])
                    verification_status = "VERIFIED"

                inserted = await (
                    await connection.execute(
                        """
                        INSERT INTO feedback_events(
                            event_id, request_id, client_id, product_id, event_type,
                            shown_discount, purchase_value, discount_cost, created_at,
                            verification_status, event_fingerprint
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING
                        RETURNING event_id
                        """,
                        (
                            feedback.event_id,
                            feedback.request_id,
                            feedback.client_id,
                            feedback.product_id,
                            feedback.event_type,
                            feedback.shown_discount,
                            feedback.purchase_value,
                            feedback.discount_cost,
                            feedback.timestamp,
                            verification_status,
                            fingerprint,
                        ),
                    )
                ).fetchone()
                if inserted is not None:
                    return FeedbackWriteResult("accepted", verification_status)

                raced = await (
                    await connection.execute(
                        """
                        SELECT event_fingerprint, verification_status
                        FROM feedback_events WHERE event_id = %s
                        """,
                        (feedback.event_id,),
                    )
                ).fetchone()
                if raced is None or raced[0] != fingerprint:
                    raise FeedbackConflictError("event_id already has another payload")
                return FeedbackWriteResult("duplicate", raced[1])
