"""Build a cutoff-safe client-level dataset for T-learner uplift training."""

from __future__ import annotations

import argparse
import json
import math
import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import (
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    replace_hdfs_paths,
)
from spark_jobs.build_user_features import parse_feature_cutoff, positive_int
from spark_jobs.gold_common import (
    gold_data_uri,
    gold_metadata_uri,
    require_gold_contract,
    source_run_ids,
)
from spark_jobs.silver_common import (
    parse_iso_date,
    silver_data_uri,
    stage_parquet,
    write_json_metadata,
)
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

USER_LINEAGE_COLUMNS = frozenset(
    {
        "feature_cutoff",
        "lookback_days",
        "source_dimensions_snapshot_date",
        "feature_run_id",
        "feature_ts",
    }
)


def fraction(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a number in (0, 1)") from error
    if not math.isfinite(parsed) or not 0 < parsed < 1:
        raise argparse.ArgumentTypeError("value must be a number in (0, 1)")
    return parsed


def validation_quota(stratum_size: int, validation_ratio: float) -> int:
    if stratum_size < 2:
        raise ValueError("each treatment/target stratum must contain at least two clients")
    if not 0 < validation_ratio < 1:
        raise ValueError("validation_ratio must be in (0, 1)")
    rounded = math.floor(stratum_size * validation_ratio + 0.5)
    return min(stratum_size - 1, max(1, rounded))


def standardized_mean_difference(
    control_mean: float | None,
    treatment_mean: float | None,
    control_variance: float | None,
    treatment_variance: float | None,
) -> float | None:
    if control_mean is None or treatment_mean is None:
        return None
    if not math.isfinite(control_mean) or not math.isfinite(treatment_mean):
        return None
    if control_variance is not None and not math.isfinite(control_variance):
        return None
    if treatment_variance is not None and not math.isfinite(treatment_variance):
        return None
    pooled_variance = ((control_variance or 0.0) + (treatment_variance or 0.0)) / 2
    if pooled_variance <= 0:
        return 0.0 if control_mean == treatment_mean else None
    return (treatment_mean - control_mean) / math.sqrt(pooled_variance)


def smd_warnings(
    smd_by_feature: dict[str, float | None], threshold: float
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for feature, value in sorted(smd_by_feature.items()):
        if value is None:
            warnings.append({"code": "SMD_UNDEFINED", "feature": feature})
        elif abs(value) > threshold:
            warnings.append(
                {
                    "code": "SMD_ABOVE_THRESHOLD",
                    "feature": feature,
                    "value": value,
                    "threshold": threshold,
                }
            )
    return warnings


def _require_training_contract(uplift: DataFrame) -> dict[str, int]:
    from pyspark.sql import functions as functions

    required = {"client_id", "treatment_flg", "target", "silver_run_id"}
    missing = sorted(required - set(uplift.columns))
    if missing:
        raise ValueError(f"Silver uplift_train is missing columns: {missing}")
    duplicate_clients = (
        uplift.groupBy("client_id").count().where(functions.col("count") != 1).count()
    )
    if duplicate_clients:
        raise ValueError(f"Silver uplift_train contains {duplicate_clients} duplicate clients")
    invalid = uplift.where(
        functions.col("client_id").isNull()
        | functions.col("treatment_flg").isNull()
        | functions.col("target").isNull()
        | ~functions.col("treatment_flg").isin(0, 1)
        | ~functions.col("target").isin(0, 1)
    ).count()
    if invalid:
        raise ValueError(f"Silver uplift_train contains {invalid} invalid labels")
    rows = uplift.groupBy("treatment_flg", "target").count().collect()
    counts = {
        f"treatment={int(row['treatment_flg'])},target={int(row['target'])}": int(row["count"])
        for row in rows
    }
    expected = {
        f"treatment={treatment},target={target}"
        for treatment in (0, 1)
        for target in (0, 1)
    }
    if set(counts) != expected:
        raise ValueError(f"uplift dataset must contain all treatment/target strata: {counts}")
    too_small = {key: count for key, count in counts.items() if count < 2}
    if too_small:
        raise ValueError(f"uplift strata require at least two clients: {too_small}")
    return counts


def _add_split(frame: DataFrame, validation_ratio: float, random_seed: int) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    stratum = Window.partitionBy("treatment_flg", "target")
    order = stratum.orderBy(
        functions.xxhash64("client_id", functions.lit(random_seed)),
        functions.asc("client_id"),
    )
    validation_count = functions.greatest(
        functions.lit(1),
        functions.least(
            functions.count("*").over(stratum) - 1,
            functions.round(
                functions.count("*").over(stratum) * functions.lit(validation_ratio)
            ).cast("long"),
        ),
    )
    return (
        frame.withColumn("_row_number", functions.row_number().over(order))
        .withColumn("_validation_count", validation_count)
        .withColumn(
            "dataset_split",
            functions.when(
                functions.col("_row_number") <= functions.col("_validation_count"),
                functions.lit("validation"),
            ).otherwise(functions.lit("train")),
        )
        .drop("_row_number", "_validation_count")
    )


def _numeric_comparability(frame: DataFrame, columns: list[str]) -> dict[str, Any]:
    from pyspark.sql import functions as functions

    if not columns:
        return {"standardized_mean_difference": {}, "missing_ratio": {}}
    aggregations = []
    for column in columns:
        aggregations.extend(
            [
                functions.avg(column).alias(f"{column}__mean"),
                functions.var_samp(column).alias(f"{column}__variance"),
                functions.sum(
                    functions.when(
                        functions.col(column).isNull()
                        | functions.isnan(functions.col(column).cast("double")),
                        1,
                    ).otherwise(0)
                ).alias(f"{column}__missing"),
            ]
        )
    grouped = {
        int(row["treatment_flg"]): row.asDict()
        for row in frame.groupBy("treatment_flg").agg(*aggregations).collect()
    }
    total_rows = frame.count()
    smd = {
        column: standardized_mean_difference(
            grouped[0][f"{column}__mean"],
            grouped[1][f"{column}__mean"],
            grouped[0][f"{column}__variance"],
            grouped[1][f"{column}__variance"],
        )
        for column in columns
    }
    missing = {
        column: (
            int(grouped[0][f"{column}__missing"] or 0)
            + int(grouped[1][f"{column}__missing"] or 0)
        )
        / total_rows
        for column in columns
    }
    return {"standardized_mean_difference": smd, "missing_ratio": missing}


def _categorical_comparability(frame: DataFrame, columns: list[str]) -> dict[str, Any]:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    result: dict[str, Any] = {}
    total_rows = frame.count()
    for column in columns:
        counts = frame.groupBy("treatment_flg", column).count()
        rank = Window.partitionBy("treatment_flg").orderBy(
            functions.desc("count"), functions.asc_nulls_last(column)
        )
        rows = (
            counts.withColumn("rank", functions.row_number().over(rank))
            .where(functions.col("rank") <= 20)
            .orderBy("treatment_flg", "rank")
        )
        top_values = []
        for row in rows.collect():
            top_values.append(
                {
                    "treatment_flg": int(row["treatment_flg"]),
                    "value": row[column],
                    "count": int(row["count"]),
                }
            )
        missing_count = frame.where(functions.col(column).isNull()).count()
        result[column] = {
            "missing_ratio": missing_count / total_rows,
            "top_values": top_values,
        }
    return result


def _dataset_metrics(
    frame: DataFrame,
    *,
    numeric_features: list[str],
    categorical_features: list[str],
    smd_warning_threshold: float,
) -> dict[str, Any]:
    from pyspark.sql import functions as functions

    group_rows = frame.groupBy("dataset_split", "treatment_flg").agg(
        functions.count("*").alias("rows"), functions.avg("target").alias("target_rate")
    )
    groups = sorted(
        [
            {
                "dataset_split": row["dataset_split"],
                "treatment_flg": int(row["treatment_flg"]),
                "rows": int(row["rows"]),
                "target_rate": float(row["target_rate"]),
            }
            for row in group_rows.collect()
        ],
        key=lambda group: (str(group["dataset_split"]), int(group["treatment_flg"])),
    )
    overall = {
        int(row["treatment_flg"]): float(row["target_rate"])
        for row in frame.groupBy("treatment_flg")
        .agg(functions.avg("target").alias("target_rate"))
        .collect()
    }
    numeric = _numeric_comparability(frame, numeric_features)
    warnings = smd_warnings(
        numeric["standardized_mean_difference"], smd_warning_threshold
    )
    return {
        "output_rows": frame.count(),
        "groups": groups,
        "control_conversion_rate": overall[0],
        "treatment_conversion_rate": overall[1],
        "global_uplift": overall[1] - overall[0],
        "numeric_comparability": numeric,
        "categorical_top_values": _categorical_comparability(frame, categorical_features),
        "warnings": warnings,
    }


def build_uplift_dataset(
    *,
    hdfs_base_uri: str,
    dimensions_snapshot_date: date,
    feature_cutoff: datetime,
    lookback_days: int = 180,
    validation_ratio: float = 0.2,
    smd_warning_threshold: float = 0.1,
    random_seed: int = 42,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as functions
    from pyspark.sql.types import NumericType, StringType

    base = normalize_hdfs_base_uri(hdfs_base_uri)
    snapshot = feature_cutoff.date()
    run_id = uuid.uuid4().hex
    created_at = datetime.now(UTC).replace(tzinfo=None)
    staging = f"{base}/tmp/uplift-dataset/{run_id}"
    backup = f"{base}/tmp/uplift-dataset-backup/{run_id}"
    spark = (
        SparkSession.builder.appName(f"uplift-dataset-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging)
    try:
        partition = f"snapshot_date={dimensions_snapshot_date.isoformat()}"
        uplift = spark.read.parquet(silver_data_uri(base, "uplift/train", partition))
        users = spark.read.parquet(gold_data_uri(base, "user_features", snapshot))
        require_gold_contract(
            users,
            feature_cutoff=feature_cutoff,
            lookback_days=lookback_days,
            dimensions_snapshot_date=dimensions_snapshot_date,
            entity="user_features",
        )
        strata = _require_training_contract(uplift)
        user_run_ids = source_run_ids(users, "feature_run_id")
        uplift_run_ids = source_run_ids(uplift)
        if len(user_run_ids) != 1 or len(uplift_run_ids) != 1:
            raise ValueError(
                "uplift dataset requires one user feature run and one Silver uplift run"
            )
        user_columns = [
            column
            for column in users.columns
            if column != "client_id" and column not in USER_LINEAGE_COLUMNS
        ]
        missing_features = uplift.join(users.select("client_id"), "client_id", "left_anti").count()
        if missing_features:
            raise ValueError(
                f"Silver uplift_train contains {missing_features} clients without user features"
            )
        dataset = (
            uplift.select("client_id", "treatment_flg", "target")
            .join(users.select("client_id", *user_columns), "client_id")
            .withColumns(
                {
                    "observation_cutoff": functions.lit(feature_cutoff).cast("timestamp"),
                    "lookback_days": functions.lit(lookback_days),
                    "source_dimensions_snapshot_date": functions.lit(
                        dimensions_snapshot_date.isoformat()
                    ),
                    "source_uplift_run_id": functions.lit(uplift_run_ids[0]),
                    "source_user_feature_run_id": functions.lit(user_run_ids[0]),
                    "dataset_run_id": functions.lit(run_id),
                    "dataset_ts": functions.lit(created_at).cast("timestamp"),
                }
            )
        )
        dataset = _add_split(dataset, validation_ratio, random_seed).cache()
        if dataset.count() != sum(strata.values()):
            raise ValueError("uplift dataset row coverage changed during feature join")
        feature_schema = {
            field.name: field.dataType
            for field in users.schema.fields
            if field.name in user_columns
        }
        numeric_features = sorted(
            name for name, data_type in feature_schema.items() if isinstance(data_type, NumericType)
        )
        categorical_features = sorted(
            name for name, data_type in feature_schema.items() if isinstance(data_type, StringType)
        )
        metrics = _dataset_metrics(
            dataset,
            numeric_features=numeric_features,
            categorical_features=categorical_features,
            smd_warning_threshold=smd_warning_threshold,
        )
        staged_data, staged_metadata = f"{staging}/data", f"{staging}/metadata"
        stage_parquet(dataset, staged_data)
        metadata = {
            "job": "uplift_dataset",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "snapshot_date": snapshot.isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "lookback_days": lookback_days,
            "dimensions_snapshot_date": dimensions_snapshot_date.isoformat(),
            "validation_ratio": validation_ratio,
            "smd_warning_threshold": smd_warning_threshold,
            "random_seed": random_seed,
            "source_silver_uplift_run_ids": uplift_run_ids,
            "source_user_feature_run_ids": user_run_ids,
            "source_strata": strata,
            "numeric_features": numeric_features,
            "categorical_features": categorical_features,
            "metrics": metrics,
        }
        write_json_metadata(spark, staged_metadata, metadata)
        replace_hdfs_paths(
            spark,
            [
                (staged_data, gold_data_uri(base, "uplift_dataset", snapshot)),
                (staged_metadata, gold_metadata_uri(base, "uplift_dataset", snapshot)),
            ],
            backup,
        )
        return metadata
    finally:
        hdfs.delete(hadoop_path(spark, staging), True)
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--dimensions-snapshot-date", required=True, type=parse_iso_date)
    parser.add_argument("--feature-cutoff", required=True, type=parse_feature_cutoff)
    parser.add_argument("--lookback-days", default=180, type=positive_int)
    parser.add_argument("--validation-ratio", default=0.2, type=fraction)
    parser.add_argument("--smd-warning-threshold", default=0.1, type=fraction)
    parser.add_argument("--random-seed", default=42, type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_uplift_dataset(
        hdfs_base_uri=args.hdfs_base_uri,
        dimensions_snapshot_date=args.dimensions_snapshot_date,
        feature_cutoff=args.feature_cutoff,
        lookback_days=args.lookback_days,
        validation_ratio=args.validation_ratio,
        smd_warning_threshold=args.smd_warning_threshold,
        random_seed=args.random_seed,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
