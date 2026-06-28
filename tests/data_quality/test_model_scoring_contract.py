from __future__ import annotations

import argparse
import math

import pytest

from training.score_models import (
    model_run_id,
    require_finite_probabilities,
    validate_feature_manifest,
)


def test_model_run_id_requires_uuid_hex() -> None:
    value = "0123456789abcdef0123456789abcdef"
    assert model_run_id(value) == value
    with pytest.raises(argparse.ArgumentTypeError, match="32 lowercase"):
        model_run_id("../current")


def test_feature_manifest_requires_complete_unique_schema() -> None:
    payload = {
        "features": ["age", "gender"],
        "categorical_features": ["gender"],
        "feature_types": {"age": "double", "gender": "string"},
    }
    assert validate_feature_manifest(payload, "uplift") == (["age", "gender"], ["gender"])
    with pytest.raises(ValueError, match="duplicate"):
        validate_feature_manifest({**payload, "features": ["age", "age"]}, "uplift")
    with pytest.raises(ValueError, match="feature_types"):
        validate_feature_manifest({**payload, "feature_types": {"age": "double"}}, "uplift")


def test_probabilities_must_be_finite_and_bounded() -> None:
    require_finite_probabilities([0.0, 0.5, 1.0], "propensity")
    for invalid in (-0.1, 1.1, math.nan, math.inf):
        with pytest.raises(ValueError, match="finite"):
            require_finite_probabilities([invalid], "propensity")
