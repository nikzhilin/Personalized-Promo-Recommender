from __future__ import annotations

import pytest

from services.contracts import Recommendation
from services.online_config import load_online_store_config
from services.publisher.publish_redis import (
    choose_published_top_n,
    compact_payload,
)


def test_repository_online_store_config_is_valid() -> None:
    config = load_online_store_config("configs/online_store.yaml")
    assert config.snapshot_ttl_seconds == 48 * 60 * 60
    assert config.max_snapshot_bytes == 160 * 1024 * 1024
    assert config.min_top_n == 1
    assert config.max_top_n == 10


def test_compact_payload_round_trips_without_extra_fields() -> None:
    payload = compact_payload(
        [
            Recommendation(
                product_id="product-1",
                discount=0.1,
                expected_profit=4.25,
                recsys_score=0.8,
                reason_code="HIGH_INCREMENTAL_PROFIT",
                rank=1,
            )
        ]
    )
    assert " " not in payload
    assert "product-1" in payload
    assert "promo_abuse" not in payload


def test_snapshot_size_shrinks_top_n_and_fails_below_minimum() -> None:
    estimates = {1: 60, 2: 90, 3: 120}
    assert choose_published_top_n(
        estimates,
        max_snapshot_bytes=100,
        available_bytes=1_000,
        min_top_n=1,
    ) == 2
    assert choose_published_top_n(
        estimates,
        max_snapshot_bytes=1_000,
        available_bytes=70,
        min_top_n=1,
    ) == 1
    with pytest.raises(ValueError, match="minimum top-N"):
        choose_published_top_n(
            estimates,
            max_snapshot_bytes=50,
            available_bytes=1_000,
            min_top_n=1,
        )
