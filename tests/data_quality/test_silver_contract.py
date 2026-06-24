from __future__ import annotations

import argparse

import pytest

from spark_jobs.build_silver_dimensions import DIMENSIONS
from spark_jobs.clean_silver_purchases import REASON_CODES
from spark_jobs.silver_common import (
    parse_iso_date,
    silver_data_uri,
    silver_metadata_uri,
    silver_reject_uri,
)


def test_silver_paths_are_separated_by_artifact_type() -> None:
    base = "hdfs://namenode:9000/promo"
    partition = "snapshot_date=2026-07-01"

    assert silver_data_uri(base, "clients", partition) == (
        f"{base}/silver/clients/{partition}"
    )
    assert silver_reject_uri(base, "clients", partition) == (
        f"{base}/silver/rejects/clients/{partition}"
    )
    assert silver_metadata_uri(base, "dimensions", partition) == (
        f"{base}/silver/metadata/dimensions/{partition}"
    )


def test_uplift_train_and_test_use_separate_silver_paths() -> None:
    paths = {spec.name: spec.silver_path for spec in DIMENSIONS}

    assert paths["uplift_train"] == "uplift/train"
    assert paths["uplift_test"] == "uplift/test"


def test_purchase_reasons_cover_documented_rejections() -> None:
    assert set(REASON_CODES) == {
        "NONPOSITIVE_QUANTITY",
        "UNKNOWN_CLIENT",
        "UNKNOWN_PRODUCT",
        "FUTURE_TRANSACTION",
    }


def test_parse_iso_date_rejects_noncanonical_date() -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="YYYY-MM-DD"):
        parse_iso_date("01.07.2026")
