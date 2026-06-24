from __future__ import annotations

import argparse

import pytest

from spark_jobs.ingest_purchases import (
    parse_purchase_month,
    purchase_partition_uri,
    select_purchase_months,
)


def test_purchase_partition_uri_matches_documented_layout() -> None:
    assert purchase_partition_uri("hdfs://namenode:9000/promo", "2019-02") == (
        "hdfs://namenode:9000/promo/bronze/purchases/purchase_month=2019-02"
    )


@pytest.mark.parametrize("value", ["2019-2", "2019-13", "201902", "not-a-month"])
def test_parse_purchase_month_rejects_noncanonical_values(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="YYYY-MM"):
        parse_purchase_month(value)


def test_default_month_selection_uses_all_discovered_months() -> None:
    assert select_purchase_months(None, ["2018-12", "2019-01"]) == [
        "2018-12",
        "2019-01",
    ]


def test_selected_months_preserve_order_and_remove_duplicates() -> None:
    selected = select_purchase_months(
        ["2019-01", "2018-12", "2019-01"], ["2018-12", "2019-01"]
    )

    assert selected == ["2019-01", "2018-12"]


def test_missing_selected_month_is_rejected() -> None:
    with pytest.raises(ValueError, match="2019-03"):
        select_purchase_months(["2019-03"], ["2019-01", "2019-02"])
