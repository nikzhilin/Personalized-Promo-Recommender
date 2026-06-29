from __future__ import annotations

import pytest
from pydantic import ValidationError

from spark_jobs.rank_recommendations import assign_reason_code, final_score_value
from spark_jobs.ranking_config import load_ranking_config


def test_repository_ranking_policy_is_valid() -> None:
    config = load_ranking_config("configs/ranking.yaml")
    assert config.version == "1.0"
    assert config.top_n == 10
    assert config.max_items_per_level_2 == 3
    assert config.unknown_level_2 == "__UNKNOWN__"


def test_ranking_policy_rejects_weights_that_do_not_sum_to_one(
    tmp_path: pytest.TempPathFactory,
) -> None:
    source = tmp_path / "ranking.yaml"
    source.write_text(
        """
version: v1
top_n: 10
max_items_per_level_2: 3
unknown_level_2: __UNKNOWN__
weights: {profit: 0.5, relevance: 0.3, promo_abuse_penalty: 0.1}
""",
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="sum to 1"):
        load_ranking_config(str(source))


def test_final_score_uses_documented_weights() -> None:
    config = load_ranking_config("configs/ranking.yaml")
    assert final_score_value(0.8, 0.5, 0.2, config) == pytest.approx(0.66)


@pytest.mark.parametrize(
    ("cold_start", "discount", "sources", "expected"),
    [
        (True, 0.1, ["global_popular", "repeat_purchase"], "COLD_START_POPULAR"),
        (False, 0.1, ["repeat_purchase"], "HIGH_INCREMENTAL_PROFIT"),
        (False, 0.0, ["repeat_purchase", "category_popular"], "REPEAT_PURCHASE"),
        (False, 0.0, ["category_popular"], "CATEGORY_RELEVANCE"),
        (False, 0.0, ["item_to_item"], "ORGANIC_PURCHASE_NO_DISCOUNT"),
    ],
)
def test_reason_code_precedence(
    cold_start: bool, discount: float, sources: list[str], expected: str
) -> None:
    assert (
        assign_reason_code(
            cold_start=cold_start,
            discount=discount,
            candidate_sources=sources,
        )
        == expected
    )
