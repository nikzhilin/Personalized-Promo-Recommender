"""Validated versioned configuration for simulation jobs."""

from __future__ import annotations

import math
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DiscountResponseConfig(StrictModel):
    grid: list[float]
    uplift_multipliers: list[float]
    relevance_floor: float = Field(ge=0, le=1)
    relevance_weight: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_grid(self) -> DiscountResponseConfig:
        if not self.grid or len(self.grid) != len(self.uplift_multipliers):
            raise ValueError("discount grid and uplift multipliers must have equal non-zero length")
        if self.grid != sorted(set(self.grid)) or self.grid[0] != 0:
            raise ValueError("discount grid must be unique, increasing, and start at zero")
        if any(not math.isfinite(value) or not 0 <= value < 1 for value in self.grid):
            raise ValueError("discounts must be finite and in [0, 1)")
        if any(
            not math.isfinite(value) or not 0 <= value <= 1
            for value in self.uplift_multipliers
        ):
            raise ValueError("uplift multipliers must be finite and in [0, 1]")
        if self.uplift_multipliers[0] != 0:
            raise ValueError("zero discount must use zero uplift multiplier")
        return self


class PromoAbuseConfig(StrictModel):
    promo_sensitivity_weight: float = Field(ge=0, le=1)
    purchase_burst_weight: float = Field(ge=0, le=1)
    redeemed_points_weight: float = Field(ge=0, le=1)
    uplift_outlier_weight: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_weights(self) -> PromoAbuseConfig:
        total = sum(
            (
                self.promo_sensitivity_weight,
                self.purchase_burst_weight,
                self.redeemed_points_weight,
                self.uplift_outlier_weight,
            )
        )
        if not math.isclose(total, 1.0, abs_tol=1e-9):
            raise ValueError("promo abuse weights must sum to 1")
        return self


class PriceConfig(StrictModel):
    lower_quantile: float = Field(ge=0, le=1)
    upper_quantile: float = Field(ge=0, le=1)
    median_accuracy: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_quantiles(self) -> PriceConfig:
        if self.lower_quantile >= self.upper_quantile:
            raise ValueError("price lower_quantile must be smaller than upper_quantile")
        return self


class EconomicsConfig(StrictModel):
    roi_epsilon: float = Field(gt=0)


class SimulationConfig(StrictModel):
    version: str = Field(min_length=1)
    discount_response: DiscountResponseConfig
    promo_abuse: PromoAbuseConfig
    price: PriceConfig
    economics: EconomicsConfig


def load_simulation_config(path: str) -> SimulationConfig:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"simulation config does not exist: {source}")
    with source.open(encoding="utf-8") as stream:
        payload = yaml.safe_load(stream)
    if not isinstance(payload, dict):
        raise ValueError("simulation config root must be a mapping")
    return SimulationConfig.model_validate(payload)
