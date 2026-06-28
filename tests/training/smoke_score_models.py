"""Exercise propensity and uplift scoring with real CatBoost estimators."""

from __future__ import annotations

import pandas as pandas
from catboost import CatBoostClassifier

from training.score_models import score_propensity_frame, score_uplift_frame


def fit_model(frame: pandas.DataFrame, labels: list[int]) -> CatBoostClassifier:
    model = CatBoostClassifier(
        iterations=10,
        depth=2,
        random_seed=42,
        allow_writing_files=False,
        verbose=False,
    )
    model.fit(frame[["value", "segment"]], labels, cat_features=["segment"])
    return model


def main() -> int:
    frame = pandas.DataFrame(
        {
            "client_id": ["c1", "c2", "c3", "c4"],
            "product_id": ["p1", "p2", "p3", "p4"],
            "value": [0.1, 0.2, 0.8, 0.9],
            "segment": ["a", "a", "b", "b"],
        }
    )
    control = fit_model(frame, [1, 1, 0, 0])
    treatment = fit_model(frame, [0, 0, 1, 1])
    propensity = score_propensity_frame(
        treatment, frame, ["value", "segment"], ["segment"]
    )
    uplift = score_uplift_frame(
        control, treatment, frame, ["value", "segment"], ["segment"]
    )
    assert list(propensity.columns) == ["client_id", "product_id", "p_base_purchase"]
    assert list(uplift.columns) == ["client_id", "p_control", "p_treatment", "uplift_score"]
    assert all(0 <= value <= 1 for value in propensity["p_base_purchase"])
    assert any(value < 0 for value in uplift["uplift_score"])
    assert any(value > 0 for value in uplift["uplift_score"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
