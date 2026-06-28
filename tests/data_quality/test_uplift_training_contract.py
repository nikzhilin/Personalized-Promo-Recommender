from __future__ import annotations

import math

import pytest

from training.train_uplift import (
    choose_class_weights,
    curve_areas,
    uplift_at_fractions,
    uplift_curve,
)


def test_class_weights_are_symmetric_below_threshold() -> None:
    assert choose_class_weights(0.09, 0.2) == "Balanced"
    assert choose_class_weights(0.1, 0.2) is None
    with pytest.raises(ValueError, match="positive rates"):
        choose_class_weights(math.nan, 0.2)


def test_gain_curve_and_qini_use_random_policy_baseline() -> None:
    labels = [1, 0, 0, 1]
    treatments = [1, 0, 1, 0]
    scores = [0.9, 0.8, 0.2, 0.1]
    curve = uplift_curve(labels, treatments, scores, point_count=4)
    assert curve[-1] == {"population_fraction": 1.0, "rows": 4, "gain": 0.0}
    areas = curve_areas(curve)
    assert areas["random_baseline_area"] == 0.0
    assert areas["qini"] == areas["auuc"]


def test_uplift_at_k_reports_response_rate_gap() -> None:
    result = uplift_at_fractions(
        [1, 0, 0, 1], [1, 0, 1, 0], [0.9, 0.8, 0.2, 0.1], fractions=(0.5, 1.0)
    )
    assert result == {"uplift_at_50pct": 1.0, "uplift_at_100pct": 0.0}
