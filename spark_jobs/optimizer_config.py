"""Validated versioned policy for discount optimization."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class CategoryMaxDiscount(StrictModel):
    default: float = Field(ge=0, lt=1)
    overrides: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_overrides(self) -> CategoryMaxDiscount:
        invalid = {
            category: value
            for category, value in self.overrides.items()
            if not category.strip() or not 0 <= value < 1
        }
        if invalid:
            raise ValueError(f"invalid category max discount overrides: {invalid}")
        return self


class OptimizerConfig(StrictModel):
    version: str = Field(min_length=1)
    min_margin_rate: float = Field(ge=0, lt=1)
    category_max_discount: CategoryMaxDiscount
    promo_abuse_threshold: float = Field(ge=0, le=1)
    max_discounted_items_per_user: int = Field(gt=0)
    global_budget: float = Field(gt=0)
    min_promo_roi: float
    incremental_profit_tolerance: float = Field(ge=0)


def load_optimizer_config(path: str) -> OptimizerConfig:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"optimizer config does not exist: {source}")
    with source.open(encoding="utf-8") as stream:
        payload = yaml.safe_load(stream)
    if not isinstance(payload, dict):
        raise ValueError("optimizer config root must be a mapping")
    return OptimizerConfig.model_validate(payload)
