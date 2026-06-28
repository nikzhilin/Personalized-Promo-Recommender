"""Train and atomically publish the current CatBoost T-learner uplift models."""

from __future__ import annotations

import argparse
import json
import math
import tempfile
import uuid
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import (
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    replace_hdfs_paths,
)
from spark_jobs.build_user_features import positive_int
from spark_jobs.gold_common import gold_data_uri, gold_metadata_uri
from spark_jobs.silver_common import parse_iso_date
from spark_jobs.time_compat import UTC
from training.build_uplift_dataset import fraction as parse_fraction
from training.train_propensity import positive_float

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

REQUIRED_COLUMNS = frozenset({"client_id", "treatment_flg", "target", "dataset_split"})


def choose_class_weights(
    control_positive_rate: float, treatment_positive_rate: float, threshold: float = 0.1
) -> str | None:
    rates = (control_positive_rate, treatment_positive_rate)
    if any(not math.isfinite(rate) or not 0 <= rate <= 1 for rate in rates):
        raise ValueError("positive rates must be finite and in [0, 1]")
    if not 0 < threshold < 1:
        raise ValueError("class weight threshold must be in (0, 1)")
    return "Balanced" if min(rates) < threshold else None


def _prefix_gain(labels: Sequence[int], treatments: Sequence[int]) -> float | None:
    treated_count = sum(treatments)
    control_count = len(treatments) - treated_count
    if treated_count == 0 or control_count == 0:
        return None
    treated_responders = sum(
        label for label, treatment in zip(labels, treatments, strict=True) if treatment == 1
    )
    control_responders = sum(
        label for label, treatment in zip(labels, treatments, strict=True) if treatment == 0
    )
    return treated_responders - control_responders * treated_count / control_count


def uplift_curve(
    labels: Sequence[int],
    treatments: Sequence[int],
    uplift_scores: Sequence[float],
    point_count: int = 100,
) -> list[dict[str, float | int]]:
    if not (len(labels) == len(treatments) == len(uplift_scores)) or len(labels) == 0:
        raise ValueError("uplift inputs must have equal non-zero length")
    if point_count <= 0:
        raise ValueError("point_count must be positive")
    for label, treatment, score in zip(labels, treatments, uplift_scores, strict=True):
        if label not in {0, 1} or treatment not in {0, 1}:
            raise ValueError("labels and treatments must be binary")
        if not math.isfinite(score):
            raise ValueError("uplift scores must be finite")
    order = sorted(range(len(labels)), key=lambda index: (-uplift_scores[index], index))
    ordered_labels = [labels[index] for index in order]
    ordered_treatments = [treatments[index] for index in order]
    prefixes = sorted(
        {
            min(len(labels), max(1, math.ceil(len(labels) * point / point_count)))
            for point in range(1, point_count + 1)
        }
    )
    curve: list[dict[str, float | int]] = [
        {"population_fraction": 0.0, "rows": 0, "gain": 0.0}
    ]
    for prefix in prefixes:
        gain = _prefix_gain(ordered_labels[:prefix], ordered_treatments[:prefix])
        if gain is not None:
            curve.append(
                {
                    "population_fraction": prefix / len(labels),
                    "rows": prefix,
                    "gain": gain,
                }
            )
    if len(curve) == 1 or curve[-1]["rows"] != len(labels):
        raise ValueError("validation must contain both treatment and control observations")
    return curve


def curve_areas(curve: Sequence[dict[str, float | int]]) -> dict[str, float]:
    if len(curve) < 2:
        raise ValueError("uplift curve must contain at least two points")
    auuc = 0.0
    for left, right in zip(curve, curve[1:], strict=False):
        width = float(right["population_fraction"]) - float(left["population_fraction"])
        auuc += width * (float(left["gain"]) + float(right["gain"])) / 2
    final_gain = float(curve[-1]["gain"])
    baseline_area = final_gain / 2
    return {"auuc": auuc, "random_baseline_area": baseline_area, "qini": auuc - baseline_area}


def uplift_at_fractions(
    labels: Sequence[int],
    treatments: Sequence[int],
    uplift_scores: Sequence[float],
    fractions: Sequence[float] = (0.1, 0.2, 0.3),
) -> dict[str, float | None]:
    if not (len(labels) == len(treatments) == len(uplift_scores)) or len(labels) == 0:
        raise ValueError("uplift inputs must have equal non-zero length")
    order = sorted(range(len(labels)), key=lambda index: (-uplift_scores[index], index))
    result: dict[str, float | None] = {}
    for fraction in fractions:
        if not 0 < fraction <= 1:
            raise ValueError("uplift fractions must be in (0, 1]")
        prefix = max(1, math.ceil(len(labels) * fraction))
        selected = order[:prefix]
        treated = [labels[index] for index in selected if treatments[index] == 1]
        control = [labels[index] for index in selected if treatments[index] == 0]
        key = f"uplift_at_{int(fraction * 100)}pct"
        result[key] = (
            sum(treated) / len(treated) - sum(control) / len(control)
            if treated and control
            else None
        )
    return result


def _prepare_features(frame: Any, features: list[str], categorical: list[str]) -> Any:
    selected = frame.loc[:, features].copy()
    for column in categorical:
        selected[column] = selected[column].astype("string").fillna("__MISSING__")
    return selected


def _classification_metrics(labels: Any, probabilities: Any) -> dict[str, float | int]:
    from sklearn.metrics import (
        average_precision_score,
        brier_score_loss,
        log_loss,
        roc_auc_score,
    )

    label_values = labels.astype("int8").tolist()
    probability_values = probabilities.tolist()
    return {
        "roc_auc": float(roc_auc_score(label_values, probability_values)),
        "pr_auc": float(average_precision_score(label_values, probability_values)),
        "log_loss": float(log_loss(label_values, probability_values, labels=[0, 1])),
        "brier_score": float(brier_score_loss(label_values, probability_values)),
        "rows": len(label_values),
    }


def train_uplift_models(
    pandas_frame: Any,
    *,
    features: list[str],
    categorical_features: list[str],
    iterations: int,
    depth: int,
    learning_rate: float,
    early_stopping_rounds: int,
    random_seed: int,
    thread_count: int,
    class_weight_threshold: float = 0.1,
) -> tuple[Any, Any, dict[str, Any]]:
    from catboost import CatBoostClassifier

    train = pandas_frame[pandas_frame["dataset_split"] == "train"]
    validation = pandas_frame[pandas_frame["dataset_split"] == "validation"]
    rates = {
        treatment: float(
            train[train["treatment_flg"] == treatment]["target"].astype("int8").mean()
        )
        for treatment in (0, 1)
    }
    class_weights = choose_class_weights(rates[0], rates[1], class_weight_threshold)
    models: dict[int, Any] = {}
    branch_metrics: dict[str, Any] = {}
    for treatment, name in ((0, "control"), (1, "treatment")):
        train_branch = train[train["treatment_flg"] == treatment]
        validation_branch = validation[validation["treatment_flg"] == treatment]
        model = CatBoostClassifier(
            iterations=iterations,
            depth=depth,
            learning_rate=learning_rate,
            loss_function="Logloss",
            eval_metric="Logloss",
            auto_class_weights=class_weights,
            random_seed=random_seed,
            thread_count=thread_count,
            allow_writing_files=False,
            verbose=False,
        )
        model.fit(
            _prepare_features(train_branch, features, categorical_features),
            train_branch["target"].astype("int8"),
            cat_features=categorical_features,
            eval_set=(
                _prepare_features(validation_branch, features, categorical_features),
                validation_branch["target"].astype("int8"),
            ),
            early_stopping_rounds=early_stopping_rounds,
            use_best_model=True,
        )
        probabilities = model.predict_proba(
            _prepare_features(validation_branch, features, categorical_features)
        )[:, 1]
        branch_metrics[name] = {
            **_classification_metrics(validation_branch["target"], probabilities),
            "best_iteration": int(model.get_best_iteration()),
            "train_positive_rate": rates[treatment],
        }
        models[treatment] = model

    validation_x = _prepare_features(validation, features, categorical_features)
    p_control = models[0].predict_proba(validation_x)[:, 1]
    p_treatment = models[1].predict_proba(validation_x)[:, 1]
    uplift_scores = (p_treatment - p_control).tolist()
    labels = validation["target"].astype("int8").tolist()
    treatments = validation["treatment_flg"].astype("int8").tolist()
    curve = uplift_curve(labels, treatments, uplift_scores)

    diagnostic = CatBoostClassifier(
        iterations=min(iterations, 300),
        depth=min(depth, 6),
        learning_rate=learning_rate,
        loss_function="Logloss",
        eval_metric="AUC",
        random_seed=random_seed,
        thread_count=thread_count,
        allow_writing_files=False,
        verbose=False,
    )
    diagnostic.fit(
        _prepare_features(train, features, categorical_features),
        train["treatment_flg"].astype("int8"),
        cat_features=categorical_features,
        eval_set=(validation_x, validation["treatment_flg"].astype("int8")),
        early_stopping_rounds=early_stopping_rounds,
        use_best_model=True,
    )
    treatment_probabilities = diagnostic.predict_proba(validation_x)[:, 1]
    quantile_levels = (0.01, 0.05, 0.5, 0.95, 0.99)
    from pandas import Series

    propensity = Series(treatment_probabilities)
    overlap = {
        "roc_auc": _classification_metrics(
            validation["treatment_flg"], treatment_probabilities
        )["roc_auc"],
        "propensity_quantiles": {
            str(level): float(value)
            for level, value in zip(
                quantile_levels,
                propensity.quantile(quantile_levels).tolist(),
                strict=True,
            )
        },
        "fraction_outside_0_1_0_9": float(
            ((treatment_probabilities < 0.1) | (treatment_probabilities > 0.9)).mean()
        ),
        "best_iteration": int(diagnostic.get_best_iteration()),
    }
    metrics = {
        "branches": branch_metrics,
        "class_weight_policy": {
            "threshold": class_weight_threshold,
            "auto_class_weights": class_weights,
        },
        "uplift": {
            **curve_areas(curve),
            **uplift_at_fractions(labels, treatments, uplift_scores),
            "curve": curve,
            "validation_mean_uplift": sum(uplift_scores) / len(uplift_scores),
            "validation_negative_uplift_share": sum(score < 0 for score in uplift_scores)
            / len(uplift_scores),
        },
        "treatment_overlap": overlap,
    }
    return models[0], models[1], metrics


def _read_hdfs_json(spark: SparkSession, uri: str) -> dict[str, Any]:
    hdfs = filesystem(spark, uri)
    stream = hdfs.open(hadoop_path(spark, uri))
    reader = spark._jvm.java.io.BufferedReader(  # noqa: SLF001
        spark._jvm.java.io.InputStreamReader(stream)  # noqa: SLF001
    )
    try:
        return json.loads(reader.readLine())
    finally:
        reader.close()


def _copy_local_artifacts(spark: SparkSession, directory: Path, staging_uri: str) -> None:
    hdfs = filesystem(spark, staging_uri)
    staging = hadoop_path(spark, staging_uri)
    if not hdfs.mkdirs(staging) and not hdfs.exists(staging):
        raise RuntimeError(f"cannot create HDFS staging directory: {staging_uri}")
    for source in sorted(directory.iterdir()):
        hdfs.copyFromLocalFile(
            False,
            True,
            hadoop_path(spark, source.as_uri()),
            hadoop_path(spark, f"{staging_uri}/{source.name}"),
        )


def _delete_previous_runs(spark: SparkSession, model_root: str, current_run_id: str) -> None:
    hdfs = filesystem(spark, model_root)
    statuses = hdfs.globStatus(hadoop_path(spark, f"{model_root}/run_id=*")) or []
    current_name = f"run_id={current_run_id}"
    for status in statuses:
        path = status.getPath()
        if path.getName() != current_name and not hdfs.delete(path, True):
            raise RuntimeError(f"cannot remove previous uplift model: {path}")


def train_uplift(
    *,
    hdfs_base_uri: str,
    dataset_snapshot_date: date,
    iterations: int = 500,
    depth: int = 7,
    learning_rate: float = 0.05,
    early_stopping_rounds: int = 50,
    random_seed: int = 42,
    thread_count: int = 2,
    class_weight_threshold: float = 0.1,
) -> dict[str, Any]:
    from importlib.metadata import version

    from pyspark.sql import SparkSession

    base = normalize_hdfs_base_uri(hdfs_base_uri)
    run_id = uuid.uuid4().hex
    created_at = datetime.now(UTC).replace(tzinfo=None)
    dataset_uri = gold_data_uri(base, "uplift_dataset", dataset_snapshot_date)
    dataset_metadata_uri = (
        f"{gold_metadata_uri(base, 'uplift_dataset', dataset_snapshot_date)}/_metadata.json"
    )
    model_root = f"{base}/models/uplift"
    staging_uri = f"{base}/tmp/uplift-model/{run_id}"
    backup_uri = f"{base}/tmp/uplift-model-backup/{run_id}"
    target_uri = f"{model_root}/run_id={run_id}"
    spark = (
        SparkSession.builder.appName(f"train-uplift-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging_uri)
    try:
        source_metadata = _read_hdfs_json(spark, dataset_metadata_uri)
        if source_metadata.get("snapshot_date") != dataset_snapshot_date.isoformat():
            raise ValueError(
                "uplift dataset metadata snapshot mismatch: "
                f"expected {dataset_snapshot_date}, found {source_metadata.get('snapshot_date')}"
            )
        if not source_metadata.get("run_id"):
            raise ValueError("uplift dataset metadata is missing run_id")
        numeric = list(source_metadata.get("numeric_features") or [])
        categorical = list(source_metadata.get("categorical_features") or [])
        features = numeric + categorical
        if not features:
            raise ValueError("uplift dataset metadata contains no model features")
        if len(features) != len(set(features)):
            raise ValueError("uplift dataset metadata contains duplicate model features")
        frame = spark.read.parquet(dataset_uri)
        missing = sorted((REQUIRED_COLUMNS | set(features)) - set(frame.columns))
        if missing:
            raise ValueError(f"uplift dataset is missing columns: {missing}")
        from pyspark.sql.types import NumericType, StringType

        schema = {field.name: field.dataType for field in frame.schema.fields}
        invalid_numeric = [
            feature for feature in numeric if not isinstance(schema[feature], NumericType)
        ]
        invalid_categorical = [
            feature for feature in categorical if not isinstance(schema[feature], StringType)
        ]
        if invalid_numeric or invalid_categorical:
            raise ValueError(
                "uplift feature schema does not match dataset metadata: "
                f"numeric={invalid_numeric}, categorical={invalid_categorical}"
            )
        strata = {
            (row["dataset_split"], int(row["treatment_flg"]), int(row["target"]))
            for row in frame.select("dataset_split", "treatment_flg", "target")
            .distinct()
            .collect()
        }
        expected = {
            (split, treatment, target)
            for split in ("train", "validation")
            for treatment in (0, 1)
            for target in (0, 1)
        }
        if strata != expected:
            raise ValueError(
                f"uplift training requires all split/treatment/target strata: {strata}"
            )
        pandas_frame = (
            frame.select(
                "client_id", *features, "treatment_flg", "target", "dataset_split"
            )
            .orderBy("dataset_split", "client_id")
            .drop("client_id")
            .toPandas()
        )
        control_model, treatment_model, metrics = train_uplift_models(
            pandas_frame,
            features=features,
            categorical_features=categorical,
            iterations=iterations,
            depth=depth,
            learning_rate=learning_rate,
            early_stopping_rounds=early_stopping_rounds,
            random_seed=random_seed,
            thread_count=thread_count,
            class_weight_threshold=class_weight_threshold,
        )
        manifest = {
            "features": features,
            "categorical_features": categorical,
            "feature_types": {feature: schema[feature].simpleString() for feature in features},
            "excluded_columns": sorted(set(frame.columns) - set(features)),
        }
        parameters = {
            "iterations": iterations,
            "depth": depth,
            "learning_rate": learning_rate,
            "early_stopping_rounds": early_stopping_rounds,
            "random_seed": random_seed,
            "thread_count": thread_count,
            "class_weight_threshold": class_weight_threshold,
        }
        metadata = {
            "job": "train_uplift",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "dataset_snapshot_date": dataset_snapshot_date.isoformat(),
            "source_dataset_run_id": source_metadata["run_id"],
            "source_dataset_uri": dataset_uri,
            "model_uri": target_uri,
            "parameters": parameters,
            "libraries": {
                package: version(package)
                for package in ("catboost", "numpy", "pandas", "pyarrow", "scikit-learn")
            },
        }
        with tempfile.TemporaryDirectory(prefix="uplift-model-") as temporary:
            directory = Path(temporary)
            control_model.save_model(str(directory / "model_control.cbm"))
            treatment_model.save_model(str(directory / "model_treatment.cbm"))
            for name, payload in (
                ("feature_manifest.json", manifest),
                ("metrics.json", metrics),
                ("run_metadata.json", metadata),
            ):
                (directory / name).write_text(
                    json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            _copy_local_artifacts(spark, directory, staging_uri)
        replace_hdfs_paths(spark, [(staging_uri, target_uri)], backup_uri)
        _delete_previous_runs(spark, model_root, run_id)
        return {**metadata, "metrics": metrics, "feature_manifest": manifest}
    finally:
        hdfs.delete(hadoop_path(spark, staging_uri), True)
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--dataset-snapshot-date", required=True, type=parse_iso_date)
    parser.add_argument("--iterations", default=500, type=positive_int)
    parser.add_argument("--depth", default=7, type=positive_int)
    parser.add_argument("--learning-rate", default=0.05, type=positive_float)
    parser.add_argument("--early-stopping-rounds", default=50, type=positive_int)
    parser.add_argument("--random-seed", default=42, type=int)
    parser.add_argument("--thread-count", default=2, type=positive_int)
    parser.add_argument("--class-weight-threshold", default=0.1, type=parse_fraction)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = train_uplift(
        hdfs_base_uri=args.hdfs_base_uri,
        dataset_snapshot_date=args.dataset_snapshot_date,
        iterations=args.iterations,
        depth=args.depth,
        learning_rate=args.learning_rate,
        early_stopping_rounds=args.early_stopping_rounds,
        random_seed=args.random_seed,
        thread_count=args.thread_count,
        class_weight_threshold=args.class_weight_threshold,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
