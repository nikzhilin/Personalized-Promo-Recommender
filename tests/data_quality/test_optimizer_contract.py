from __future__ import annotations

from typing import NamedTuple

import pytest
from pydantic import ValidationError

from spark_jobs.optimize_discounts import (
    category_cap,
    choose_tolerant_discount,
    greedy_allocate,
    is_locally_eligible,
)
from spark_jobs.optimizer_config import load_optimizer_config


class Candidate(NamedTuple):
    client_id: str
    product_id: str
    expected_discount_cost: float


def test_repository_optimizer_policy_is_valid() -> None:
    config = load_optimizer_config("configs/optimizer.yaml")
    assert config.version == "1.0"
    assert config.category_max_discount.default == 0.10
    assert config.global_budget == 100_000.0
    assert config.incremental_profit_tolerance == 0.01


def test_optimizer_policy_rejects_invalid_override(tmp_path: pytest.TempPathFactory) -> None:
    source = tmp_path / "optimizer.yaml"
    source.write_text(
        """
version: v1
min_margin_rate: 0.02
category_max_discount:
  default: 0.10
  overrides: {category-a: 1.1}
promo_abuse_threshold: 0.8
max_discounted_items_per_user: 3
global_budget: 100.0
min_promo_roi: 0.0
incremental_profit_tolerance: 0.01
""",
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="invalid category"):
        load_optimizer_config(str(source))


def test_greedy_allocation_enforces_cap_and_skips_over_budget_rows() -> None:
    decisions = list(
        greedy_allocate(
            [
                Candidate("client-1", "product-1", 6.0),
                Candidate("client-1", "product-2", 4.0),
                Candidate("client-2", "product-3", 5.0),
                Candidate("client-3", "product-4", 3.0),
            ],
            global_budget=10.0,
            user_cap=1,
        )
    )
    assert [decision.optimizer_decision for decision in decisions] == [
        "DISCOUNT_ACCEPTED",
        "USER_CAP_REJECTED",
        "BUDGET_REJECTED",
        "DISCOUNT_ACCEPTED",
    ]
    assert [decision.budget_spent_after for decision in decisions] == [6.0, 6.0, 6.0, 9.0]
    assert [decision.allocation_rank for decision in decisions] == [1, 2, 3, 4]


def test_local_constraints_and_tolerance_choose_smaller_discount() -> None:
    config = load_optimizer_config("configs/optimizer.yaml")
    assert category_cap("unknown", config) == 0.10
    assert is_locally_eligible(
        discount=0.10,
        is_discount_eligible=True,
        margin_rate=0.25,
        maximum_discount=0.10,
        promo_abuse_score=0.8,
        incremental_profit=1.0,
        roi=0.0,
        config=config,
    )
    assert not is_locally_eligible(
        discount=0.15,
        is_discount_eligible=True,
        margin_rate=0.25,
        maximum_discount=0.10,
        promo_abuse_score=0.2,
        incremental_profit=2.0,
        roi=1.0,
        config=config,
    )
    assert choose_tolerant_discount([(0.05, 1.0), (0.10, 1.009)], 0.01) == 0.05
    assert choose_tolerant_discount([(0.05, 1.0), (0.10, 1.02)], 0.01) == 0.10


def test_greedy_allocation_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="budget"):
        list(greedy_allocate([], global_budget=0, user_cap=1))
    with pytest.raises(ValueError, match="non-negative"):
        list(
            greedy_allocate(
                [Candidate("client-1", "product-1", -1.0)],
                global_budget=10,
                user_cap=1,
            )
        )
