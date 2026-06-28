"""Train tiny T-learner models inside the dedicated training image."""

from __future__ import annotations

import math
import tempfile
from pathlib import Path

import pandas as pd

from training.train_uplift import train_uplift_models


def main() -> int:
    rows = []
    for split, copies in (("train", 30), ("validation", 12)):
        for treatment in (0, 1):
            for index in range(copies):
                rows.append(
                    {
                        "age": 20.0 + index % 40,
                        "purchase_frequency": (index % 7) / 7,
                        "gender": "M" if index % 2 else "F",
                        "treatment_flg": treatment,
                        "target": int((index + treatment) % 4 == 0 or index % 13 == 0),
                        "dataset_split": split,
                    }
                )
    frame = pd.DataFrame(rows)
    control, treatment, metrics = train_uplift_models(
        frame,
        features=["age", "purchase_frequency", "gender"],
        categorical_features=["gender"],
        iterations=30,
        depth=4,
        learning_rate=0.1,
        early_stopping_rounds=5,
        random_seed=42,
        thread_count=2,
    )
    assert all(math.isfinite(metrics["uplift"][name]) for name in ("auuc", "qini"))
    assert math.isfinite(metrics["treatment_overlap"]["roc_auc"])
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        control.save_model(str(directory / "model_control.cbm"))
        treatment.save_model(str(directory / "model_treatment.cbm"))
        assert all(path.stat().st_size > 0 for path in directory.iterdir())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
