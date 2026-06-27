from __future__ import annotations

from datetime import date, datetime

import pytest

from spark_jobs.gold_common import (
    gold_data_uri,
    gold_metadata_uri,
    overlapping_purchase_months,
    read_margin_seed,
)


def test_gold_paths_use_cutoff_partition() -> None:
    base = "hdfs://namenode:9000/promo"
    snapshot = date(2019, 3, 1)

    assert gold_data_uri(base, "item_features", snapshot) == (
        f"{base}/gold/item_features/snapshot_date=2019-03-01"
    )
    assert gold_metadata_uri(base, "recsys_candidates", snapshot) == (
        f"{base}/gold/metadata/recsys_candidates/snapshot_date=2019-03-01"
    )


def test_gold_month_selection_uses_half_open_lookback() -> None:
    assert overlapping_purchase_months(
        datetime(2019, 3, 1), 180, ["2018-08", "2018-09", "2019-02", "2019-03"]
    ) == ["2018-09", "2019-02"]


def test_margin_seed_has_default_and_overrides(tmp_path: pytest.TempPathFactory) -> None:
    config = tmp_path / "margin.csv"
    config.write_text("level_2,margin_rate\n__DEFAULT__,0.25\nl2-a,0.30\n", encoding="utf-8")

    assert read_margin_seed(str(config)) == (0.25, {"l2-a": 0.3})


@pytest.mark.parametrize(
    "content, message",
    [
        ("level_2,margin_rate\nl2-a,0.2\n", "__DEFAULT__"),
        ("level_2,margin_rate\n__DEFAULT__,0\n", r"in \(0, 1\]"),
        (
            "level_2,margin_rate\n__DEFAULT__,0.25\n__DEFAULT__,0.3\n",
            "duplicate",
        ),
    ],
)
def test_margin_seed_rejects_invalid_contract(
    tmp_path: pytest.TempPathFactory, content: str, message: str
) -> None:
    config = tmp_path / "margin.csv"
    config.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        read_margin_seed(str(config))
