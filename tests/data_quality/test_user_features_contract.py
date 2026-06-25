from __future__ import annotations

import argparse
from datetime import date, datetime

import pytest

from spark_jobs.build_user_features import (
    gold_user_features_uri,
    gold_user_metadata_uri,
    overlapping_purchase_months,
    parse_feature_cutoff,
    positive_int,
)


def test_feature_cutoff_is_timezone_free_utc_datetime() -> None:
    assert parse_feature_cutoff("2019-03-01T00:00:00") == datetime(2019, 3, 1)

    with pytest.raises(argparse.ArgumentTypeError, match="timezone-free"):
        parse_feature_cutoff("2019-03-01T00:00:00+03:00")
    with pytest.raises(argparse.ArgumentTypeError, match="YYYY-MM-DDTHH:MM:SS"):
        parse_feature_cutoff("2019-03-01")


@pytest.mark.parametrize("value", ["0", "-1", "one"])
def test_lookback_must_be_positive(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="positive integer"):
        positive_int(value)


def test_purchase_month_selection_intersects_half_open_lookback() -> None:
    available = ["2018-08", "2018-09", "2019-02", "2019-03", "2019-04"]

    assert overlapping_purchase_months(datetime(2019, 3, 1), 180, available) == [
        "2018-09",
        "2019-02",
    ]


def test_gold_paths_are_partitioned_by_cutoff_utc_date() -> None:
    base = "hdfs://namenode:9000/promo"
    snapshot = date(2019, 3, 1)

    assert gold_user_features_uri(base, snapshot) == (
        f"{base}/gold/user_features/snapshot_date=2019-03-01"
    )
    assert gold_user_metadata_uri(base, snapshot) == (
        f"{base}/gold/metadata/user_features/snapshot_date=2019-03-01"
    )
