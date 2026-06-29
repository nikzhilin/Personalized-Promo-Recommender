from __future__ import annotations

import math

import pytest
from pydantic import ValidationError

from spark_jobs.build_simulation import (
    discount_probability,
    economic_values,
    promo_abuse_value,
    purchase_burst_raw,
)
from spark_jobs.simulation_config import load_simulation_config


def test_repository_simulation_config_is_valid() -> None:
    config = load_simulation_config("configs/simulation.yaml")
    assert config.version == "1.0"
    assert config.discount_response.grid == [0.0, 0.05, 0.1, 0.15]
    assert sum(config.promo_abuse.model_dump().values()) == pytest.approx(1.0)


def test_simulation_config_rejects_misaligned_discount_mapping(
    tmp_path: pytest.TempPathFactory,
) -> None:
    source = tmp_path / "simulation.yaml"
    source.write_text(
        """
version: v1
discount_response:
  grid: [0.0, 0.1]
  uplift_multipliers: [0.0]
  relevance_floor: 0.5
  relevance_weight: 0.5
promo_abuse:
  promo_sensitivity_weight: 0.35
  purchase_burst_weight: 0.25
  redeemed_points_weight: 0.2
  uplift_outlier_weight: 0.2
price: {lower_quantile: 0.01, upper_quantile: 0.99, median_accuracy: 10000}
economics: {roi_epsilon: 0.000001}
""",
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="equal non-zero length"):
        load_simulation_config(str(source))


def test_discount_response_clips_and_ignores_negative_uplift() -> None:
    assert discount_probability(0.2, -0.5, 1.0, 0.8, 0.5, 0.5) == 0.2
    assert discount_probability(0.9, 0.5, 1.0, 1.0, 0.5, 0.5) == 1.0


def test_abuse_proxies_and_economics_follow_contract() -> None:
    config = load_simulation_config("configs/simulation.yaml")
    assert purchase_burst_raw(3, 6) == pytest.approx(1.6)
    assert promo_abuse_value(1.0, 0.5, 0.25, 0.75, config) == pytest.approx(0.675)
    values = economic_values(
        probability=0.3,
        baseline_probability=0.2,
        unit_price=100.0,
        margin_rate=0.25,
        discount=0.05,
        roi_epsilon=1e-9,
    )
    assert values["gross_margin"] == 25.0
    assert values["expected_profit"] == pytest.approx(6.0)
    assert values["expected_discount_cost"] == pytest.approx(1.5)
    assert values["incremental_profit"] == pytest.approx(1.0)
    assert math.isclose(values["roi"], 2.0 / 3.0)
