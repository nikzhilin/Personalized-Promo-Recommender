from __future__ import annotations

import argparse
from datetime import date

import pytest

from spark_jobs.ingest_bronze import (
    BRONZE_DATASETS,
    bronze_partition_uri,
    parse_ingest_date,
    selected_datasets,
)


def test_default_selection_contains_only_small_source_files() -> None:
    selected = selected_datasets(None)

    assert [dataset.name for dataset in selected] == [
        "clients",
        "products",
        "uplift_train",
        "uplift_test",
    ]
    assert "purchases.csv" not in {dataset.source_file for dataset in selected}


def test_dataset_selection_preserves_order_and_removes_duplicates() -> None:
    selected = selected_datasets(["products", "clients", "products"])

    assert [dataset.name for dataset in selected] == ["products", "clients"]


def test_bronze_partition_uri_matches_documented_layout() -> None:
    target = bronze_partition_uri(
        "hdfs://namenode:9000/promo/",
        BRONZE_DATASETS["uplift_train"],
        date(2026, 7, 1),
    )

    assert target == (
        "hdfs://namenode:9000/promo/bronze/uplift/train/ingest_date=2026-07-01"
    )


def test_bronze_partition_uri_requires_hdfs() -> None:
    with pytest.raises(ValueError, match="must start with hdfs"):
        bronze_partition_uri("/promo", BRONZE_DATASETS["clients"], date(2026, 7, 1))


def test_parse_ingest_date_requires_iso_date() -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="YYYY-MM-DD"):
        parse_ingest_date("01.07.2026")
