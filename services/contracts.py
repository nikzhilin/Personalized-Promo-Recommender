"""Compact Redis and HTTP recommendation contracts."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

ReasonCode = Literal[
    "HIGH_INCREMENTAL_PROFIT",
    "ORGANIC_PURCHASE_NO_DISCOUNT",
    "CATEGORY_RELEVANCE",
    "REPEAT_PURCHASE",
    "COLD_START_POPULAR",
]


class Recommendation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: str = Field(min_length=1)
    discount: float = Field(ge=0, lt=1)
    expected_profit: float | None
    recsys_score: float = Field(ge=0)
    reason_code: ReasonCode
    rank: int = Field(gt=0, le=100)


class RecommendRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client_id: str = Field(min_length=1)
    limit: int = Field(default=10, ge=1, le=10)
    context: dict[str, Any] | None = None


class RecommendResponse(BaseModel):
    request_id: str
    client_id: str
    snapshot_id: str
    recommendations: list[Recommendation]
    is_fallback: bool


class FeedbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: uuid.UUID
    request_id: uuid.UUID
    client_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    event_type: Literal["click", "cart", "purchase"]
    shown_discount: float = Field(ge=0, lt=1)
    purchase_value: float | None = Field(default=None, ge=0)
    discount_cost: float | None = Field(default=None, ge=0)
    timestamp: datetime

    @field_validator("timestamp")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include timezone")
        return value


class FeedbackResponse(BaseModel):
    event_id: uuid.UUID
    status: Literal["accepted", "duplicate"]
    verification_status: Literal["VERIFIED", "UNVERIFIED_MISSING_REQUEST"]
    warnings: list[str]
