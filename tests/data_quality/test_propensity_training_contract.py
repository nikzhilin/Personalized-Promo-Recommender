from __future__ import annotations

import pytest

from training.train_propensity import (
    allocate_label_quotas,
    calibration_bins,
    feature_names,
)


def test_model_features_exclude_keys_label_and_temporal_split() -> None:
    columns = [
        "client_id",
        "product_id",
        "age",
        "candidate_sources",
        "observation_cutoff",
        "dataset_split",
        "label",
    ]
    assert feature_names(columns) == ["age", "candidate_sources"]


def test_training_cap_preserves_both_classes_and_limit() -> None:
    assert allocate_label_quotas({0: 90, 1: 10}, 20) == {0: 18, 1: 2}
    assert allocate_label_quotas({0: 3, 1: 2}, 10) == {0: 3, 1: 2}

    with pytest.raises(ValueError, match="both binary classes"):
        allocate_label_quotas({0: 5}, 4)


def test_calibration_bins_have_stable_empty_bins_and_edge_handling() -> None:
    bins = calibration_bins([0, 1, 1], [0.0, 0.55, 1.0], bin_count=2)
    assert bins == [
        {
            "lower": 0.0,
            "upper": 0.5,
            "count": 1,
            "mean_probability": 0.0,
            "positive_rate": 0.0,
        },
        {
            "lower": 0.5,
            "upper": 1.0,
            "count": 2,
            "mean_probability": 0.775,
            "positive_rate": 1.0,
        },
    ]
