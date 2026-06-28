"""Train a tiny model inside the dedicated training image."""

from __future__ import annotations

import math
import tempfile
from pathlib import Path

import pandas as pd

from training.train_propensity import train_and_evaluate


def main() -> int:
    rows = []
    for split, count in (("train", 80), ("validation", 24)):
        for index in range(count):
            rows.append(
                {
                    "age": 20.0 + index % 40,
                    "recsys_score": (index % 10) / 10,
                    "gender": "M" if index % 2 else "F",
                    "label": int(index % 4 == 0 or index % 11 == 0),
                    "dataset_split": split,
                }
            )
    frame = pd.DataFrame(rows)
    model, metrics = train_and_evaluate(
        frame,
        features=["age", "recsys_score", "gender"],
        categorical_features=["gender"],
        iterations=30,
        depth=4,
        learning_rate=0.1,
        early_stopping_rounds=5,
        random_seed=42,
        thread_count=2,
    )
    assert all(
        math.isfinite(metrics[name])
        for name in ("roc_auc", "pr_auc", "log_loss", "brier_score")
    )
    assert len(metrics["calibration_bins"]) == 10
    with tempfile.TemporaryDirectory() as temporary:
        target = Path(temporary) / "model.cbm"
        model.save_model(str(target))
        assert target.stat().st_size > 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
