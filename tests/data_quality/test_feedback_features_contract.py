from __future__ import annotations

from datetime import datetime
from pathlib import Path

from spark_jobs.build_feedback_features import (
    FEEDBACK_FEATURE_COLUMNS,
    overlapping_event_dates,
)


def test_feedback_partition_selection_covers_half_open_window_boundary() -> None:
    available = ["2018-08-31", "2018-09-02", "2019-02-28", "2019-03-01", "2019-03-02"]

    assert overlapping_event_dates(datetime(2019, 3, 1), 180, available) == [
        "2018-09-02",
        "2019-02-28",
        "2019-03-01",
    ]


def test_feedback_feature_contract_is_compact_and_stable() -> None:
    assert len(FEEDBACK_FEATURE_COLUMNS) == 10
    assert FEEDBACK_FEATURE_COLUMNS[:2] == (
        "feedback_clicks_30d",
        "feedback_clicks_90d",
    )
    assert "feedback_avg_purchase_value_180d" in FEEDBACK_FEATURE_COLUMNS


def test_feedback_job_applies_dual_cutoff_and_verified_policy() -> None:
    source = Path("spark_jobs/build_feedback_features.py").read_text(encoding="utf-8")

    assert 'functions.col("created_at") < functions.lit(feature_cutoff)' in source
    assert 'functions.col("received_at") < functions.lit(feature_cutoff)' in source
    assert 'functions.col("verification_status") == "VERIFIED"' in source
    assert '"verification_policy": "VERIFIED_ONLY"' in source
