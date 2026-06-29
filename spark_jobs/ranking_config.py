"""Validated versioned policy for final recommendation ranking."""

from __future__ import annotations

import math
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class RankingWeights(StrictModel):
    profit: float = Field(ge=0, le=1)
    relevance: float = Field(ge=0, le=1)
    promo_abuse_penalty: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_weights(self) -> RankingWeights:
        if not math.isclose(
            self.profit + self.relevance + self.promo_abuse_penalty,
            1.0,
            abs_tol=1e-9,
        ):
            raise ValueError("ranking weights must sum to 1")
        return self


class RankingConfig(StrictModel):
    version: str = Field(min_length=1)
    top_n: int = Field(gt=0)
    max_items_per_level_2: int = Field(gt=0)
    unknown_level_2: str = Field(min_length=1)
    weights: RankingWeights

    @field_validator("unknown_level_2")
    @classmethod
    def validate_unknown_level(cls, value: str) -> str:
        if value.strip() != value or not value:
            raise ValueError("unknown_level_2 must be a non-empty trimmed string")
        return value


def load_ranking_config(path: str) -> RankingConfig:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"ranking config does not exist: {source}")
    with source.open(encoding="utf-8") as stream:
        payload = yaml.safe_load(stream)
    if not isinstance(payload, dict):
        raise ValueError("ranking config root must be a mapping")
    return RankingConfig.model_validate(payload)
