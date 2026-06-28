from __future__ import annotations

import argparse
import math

import pytest

from training.build_uplift_dataset import (
    fraction,
    smd_warnings,
    standardized_mean_difference,
    validation_quota,
)


def test_validation_quota_preserves_both_sides_of_each_stratum() -> None:
    assert validation_quota(10, 0.2) == 2
    assert validation_quota(2, 0.2) == 1
    assert validation_quota(3, 0.9) == 2

    with pytest.raises(ValueError, match="at least two"):
        validation_quota(1, 0.2)


def test_standardized_mean_difference_uses_pooled_variance() -> None:
    assert standardized_mean_difference(10.0, 12.0, 4.0, 4.0) == 1.0
    assert standardized_mean_difference(10.0, 10.0, 0.0, 0.0) == 0.0
    assert standardized_mean_difference(10.0, 12.0, 0.0, 0.0) is None
    assert standardized_mean_difference(math.nan, 12.0, 1.0, 1.0) is None


def test_smd_warnings_do_not_fail_comparability() -> None:
    assert smd_warnings({"age": 0.2, "spend": 0.05, "constant": None}, 0.1) == [
        {
            "code": "SMD_ABOVE_THRESHOLD",
            "feature": "age",
            "value": 0.2,
            "threshold": 0.1,
        },
        {"code": "SMD_UNDEFINED", "feature": "constant"},
    ]


@pytest.mark.parametrize("value", ["0", "1", "-0.1", "nan", "text"])
def test_fraction_requires_open_unit_interval(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match=r"\(0, 1\)"):
        fraction(value)
