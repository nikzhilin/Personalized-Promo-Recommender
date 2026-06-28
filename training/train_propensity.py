"""Train and atomically publish the current CatBoost propensity model."""

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

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

NON_FEATURE_COLUMNS = frozenset(
    {"client_id", "product_id", "label", "dataset_split", "observation_cutoff"}
)


def positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a positive number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive number")
    return parsed


def feature_names(columns: Sequence[str]) -> list[str]:
    selected = [column for column in columns if column not in NON_FEATURE_COLUMNS]
    if not selected:
        raise ValueError("propensity dataset contains no model features")
    return selected


def allocate_label_quotas(counts: dict[int, int], limit: int) -> dict[int, int]:
    if set(counts) != {0, 1} or any(value <= 0 for value in counts.values()):
        raise ValueError("both binary classes must be present")
    total = sum(counts.values())
    if limit >= total:
        return dict(counts)
    if limit < 2:
        raise ValueError("row limit must retain both binary classes")
    positive_quota = round(limit * counts[1] / total)
    positive_quota = min(counts[1], max(1, positive_quota))
    negative_quota = min(counts[0], limit - positive_quota)
    if negative_quota == 0:
        negative_quota = 1
        positive_quota -= 1
    unused = limit - positive_quota - negative_quota
    if unused:
        add_positive = min(unused, counts[1] - positive_quota)
        positive_quota += add_positive
        negative_quota += min(unused - add_positive, counts[0] - negative_quota)
    return {0: negative_quota, 1: positive_quota}


def calibration_bins(
    labels: Sequence[int], probabilities: Sequence[float], bin_count: int = 10
) -> list[dict[str, int | float | None]]:
    if len(labels) != len(probabilities):
        raise ValueError("labels and probabilities must have equal length")
    if bin_count <= 0:
        raise ValueError("bin_count must be positive")
    totals = [0.0] * bin_count
    positives = [0] * bin_count
    counts = [0] * bin_count
    for label, probability in zip(labels, probabilities, strict=True):
        if label not in {0, 1}:
            raise ValueError("labels must be binary")
        if not math.isfinite(probability) or not 0 <= probability <= 1:
            raise ValueError("probabilities must be finite and in [0, 1]")
        index = min(int(probability * bin_count), bin_count - 1)
        totals[index] += probability
        positives[index] += label
        counts[index] += 1
    return [
        {
            "lower": index / bin_count,
            "upper": (index + 1) / bin_count,
            "count": counts[index],
            "mean_probability": totals[index] / counts[index] if counts[index] else None,
            "positive_rate": positives[index] / counts[index] if counts[index] else None,
        }
        for index in range(bin_count)
    ]


def _require_binary_split(frame: DataFrame, split: str) -> dict[int, int]:
    rows = frame.where(frame.dataset_split == split).groupBy("label").count().collect()
    counts = {int(row["label"]): int(row["count"]) for row in rows}
    if set(counts) != {0, 1}:
        raise ValueError(f"{split} split must contain both binary classes, found {counts}")
    return counts


def _cap_training_rows(
    frame: DataFrame, *, max_training_rows: int, random_seed: int
) -> tuple[DataFrame, dict[str, Any]]:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    train_counts = _require_binary_split(frame, "train")
    validation_counts = _require_binary_split(frame, "validation")
    validation_rows = sum(validation_counts.values())
    if validation_rows >= max_training_rows:
        raise ValueError(
            "validation rows must be smaller than max_training_rows: "
            f"validation={validation_rows}, maximum={max_training_rows}"
        )
    train_limit = max_training_rows - validation_rows
    quotas = allocate_label_quotas(train_counts, train_limit)
    train = frame.where(functions.col("dataset_split") == "train")
    if sum(train_counts.values()) > train_limit:
        quota_rows = [(label, quota) for label, quota in sorted(quotas.items())]
        quota_frame = frame.sparkSession.createDataFrame(quota_rows, ["label", "_quota"])
        order = Window.partitionBy("label").orderBy(
            functions.xxhash64(
                "client_id", "product_id", "observation_cutoff", functions.lit(random_seed)
            ),
            "client_id",
            "product_id",
            "observation_cutoff",
        )
        train = (
            train.join(quota_frame, "label")
            .withColumn("_row_number", functions.row_number().over(order))
            .where(functions.col("_row_number") <= functions.col("_quota"))
            .drop("_quota", "_row_number")
        )
    validation = frame.where(functions.col("dataset_split") == "validation")
    capped = train.unionByName(validation)
    return capped, {
        "source": {"train": train_counts, "validation": validation_counts},
        "selected": {"train": quotas, "validation": validation_counts},
        "max_training_rows": max_training_rows,
    }


def _prepare_pandas(frame: Any, features: list[str], categorical: list[str]) -> Any:
    selected = frame.loc[:, features].copy()
    for column in categorical:
        selected[column] = selected[column].astype("string").fillna("__MISSING__")
    return selected


def train_and_evaluate(
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
) -> tuple[Any, dict[str, Any]]:
    from catboost import CatBoostClassifier
    from sklearn.metrics import (
        average_precision_score,
        brier_score_loss,
        log_loss,
        roc_auc_score,
    )

    train = pandas_frame[pandas_frame["dataset_split"] == "train"]
    validation = pandas_frame[pandas_frame["dataset_split"] == "validation"]
    train_x = _prepare_pandas(train, features, categorical_features)
    validation_x = _prepare_pandas(validation, features, categorical_features)
    train_y = train["label"].astype("int8")
    validation_y = validation["label"].astype("int8")
    model = CatBoostClassifier(
        iterations=iterations,
        depth=depth,
        learning_rate=learning_rate,
        loss_function="Logloss",
        eval_metric="Logloss",
        random_seed=random_seed,
        thread_count=thread_count,
        allow_writing_files=False,
        verbose=False,
    )
    model.fit(
        train_x,
        train_y,
        cat_features=categorical_features,
        eval_set=(validation_x, validation_y),
        early_stopping_rounds=early_stopping_rounds,
        use_best_model=True,
    )
    probabilities = model.predict_proba(validation_x)[:, 1]
    labels = validation_y.tolist()
    probability_values = probabilities.tolist()
    metrics = {
        "roc_auc": float(roc_auc_score(labels, probability_values)),
        "pr_auc": float(average_precision_score(labels, probability_values)),
        "log_loss": float(log_loss(labels, probability_values, labels=[0, 1])),
        "brier_score": float(brier_score_loss(labels, probability_values)),
        "calibration_bins": calibration_bins(labels, probability_values),
        "best_iteration": int(model.get_best_iteration()),
        "validation_rows": len(validation),
    }
    return model, metrics


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


def _copy_local_artifacts(
    spark: SparkSession, local_directory: Path, staging_uri: str
) -> None:
    hdfs = filesystem(spark, staging_uri)
    staging = hadoop_path(spark, staging_uri)
    if not hdfs.mkdirs(staging) and not hdfs.exists(staging):
        raise RuntimeError(f"cannot create HDFS staging directory: {staging_uri}")
    for source in sorted(local_directory.iterdir()):
        destination = hadoop_path(spark, f"{staging_uri}/{source.name}")
        hdfs.copyFromLocalFile(False, True, hadoop_path(spark, source.as_uri()), destination)


def _delete_previous_runs(spark: SparkSession, model_root: str, current_run_id: str) -> None:
    hdfs = filesystem(spark, model_root)
    statuses = hdfs.globStatus(hadoop_path(spark, f"{model_root}/run_id=*")) or []
    current_name = f"run_id={current_run_id}"
    for status in statuses:
        path = status.getPath()
        if path.getName() != current_name and not hdfs.delete(path, True):
            raise RuntimeError(f"cannot remove previous propensity model: {path}")


def train_propensity(
    *,
    hdfs_base_uri: str,
    dataset_snapshot_date: date,
    max_training_rows: int = 2_000_000,
    iterations: int = 500,
    depth: int = 7,
    learning_rate: float = 0.05,
    early_stopping_rounds: int = 50,
    random_seed: int = 42,
    thread_count: int = 2,
) -> dict[str, Any]:
    from importlib.metadata import version

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType

    base = normalize_hdfs_base_uri(hdfs_base_uri)
    run_id = uuid.uuid4().hex
    created_at = datetime.now(UTC).replace(tzinfo=None)
    dataset_uri = gold_data_uri(base, "propensity_dataset", dataset_snapshot_date)
    metadata_uri = (
        f"{gold_metadata_uri(base, 'propensity_dataset', dataset_snapshot_date)}/_metadata.json"
    )
    model_root = f"{base}/models/propensity"
    staging_uri = f"{base}/tmp/propensity-model/{run_id}"
    backup_uri = f"{base}/tmp/propensity-model-backup/{run_id}"
    target_uri = f"{model_root}/run_id={run_id}"
    spark = (
        SparkSession.builder.appName(f"train-propensity-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging_uri)
    try:
        source_metadata = _read_hdfs_json(spark, metadata_uri)
        if source_metadata.get("snapshot_date") != dataset_snapshot_date.isoformat():
            raise ValueError(
                "propensity dataset metadata snapshot mismatch: "
                f"expected {dataset_snapshot_date}, found {source_metadata.get('snapshot_date')}"
            )
        if not source_metadata.get("run_id"):
            raise ValueError("propensity dataset metadata is missing run_id")
        frame = spark.read.parquet(dataset_uri)
        required = NON_FEATURE_COLUMNS
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"propensity dataset is missing columns: {missing}")
        selected, sampling = _cap_training_rows(
            frame, max_training_rows=max_training_rows, random_seed=random_seed
        )
        features = feature_names(selected.columns)
        schema = {field.name: field.dataType for field in selected.schema.fields}
        categorical = [name for name in features if isinstance(schema[name], StringType)]
        unsupported = [
            name
            for name in features
            if name not in categorical
            and schema[name].typeName()
            not in {"byte", "short", "integer", "long", "float", "double", "decimal", "boolean"}
        ]
        if unsupported:
            raise ValueError(f"unsupported model feature types: {unsupported}")
        from pyspark.sql import functions as functions

        pandas_frame = (
            selected.select(
                *features,
                "label",
                "dataset_split",
                "client_id",
                "product_id",
                "observation_cutoff",
            )
            .orderBy(
                "dataset_split",
                functions.xxhash64(
                    "client_id",
                    "product_id",
                    "observation_cutoff",
                    functions.lit(random_seed),
                ),
            )
            .drop("client_id", "product_id", "observation_cutoff")
            .toPandas()
        )
        model, metrics = train_and_evaluate(
            pandas_frame,
            features=features,
            categorical_features=categorical,
            iterations=iterations,
            depth=depth,
            learning_rate=learning_rate,
            early_stopping_rounds=early_stopping_rounds,
            random_seed=random_seed,
            thread_count=thread_count,
        )
        parameters = {
            "iterations": iterations,
            "depth": depth,
            "learning_rate": learning_rate,
            "early_stopping_rounds": early_stopping_rounds,
            "random_seed": random_seed,
            "thread_count": thread_count,
            "max_training_rows": max_training_rows,
        }
        manifest = {
            "features": features,
            "categorical_features": categorical,
            "feature_types": {name: schema[name].simpleString() for name in features},
            "excluded_columns": sorted(NON_FEATURE_COLUMNS),
        }
        metadata = {
            "job": "train_propensity",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "dataset_snapshot_date": dataset_snapshot_date.isoformat(),
            "source_dataset_run_id": source_metadata.get("run_id"),
            "source_dataset_uri": dataset_uri,
            "model_uri": target_uri,
            "parameters": parameters,
            "sampling": sampling,
            "libraries": {
                package: version(package)
                for package in ("catboost", "numpy", "pandas", "pyarrow", "scikit-learn")
            },
        }
        with tempfile.TemporaryDirectory(prefix="propensity-model-") as temporary:
            directory = Path(temporary)
            model.save_model(str(directory / "model.cbm"))
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
    parser.add_argument("--max-training-rows", default=2_000_000, type=positive_int)
    parser.add_argument("--iterations", default=500, type=positive_int)
    parser.add_argument("--depth", default=7, type=positive_int)
    parser.add_argument("--learning-rate", default=0.05, type=positive_float)
    parser.add_argument("--early-stopping-rounds", default=50, type=positive_int)
    parser.add_argument("--random-seed", default=42, type=int)
    parser.add_argument("--thread-count", default=2, type=positive_int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = train_propensity(
        hdfs_base_uri=args.hdfs_base_uri,
        dataset_snapshot_date=args.dataset_snapshot_date,
        max_training_rows=args.max_training_rows,
        iterations=args.iterations,
        depth=args.depth,
        learning_rate=args.learning_rate,
        early_stopping_rounds=args.early_stopping_rounds,
        random_seed=args.random_seed,
        thread_count=args.thread_count,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
