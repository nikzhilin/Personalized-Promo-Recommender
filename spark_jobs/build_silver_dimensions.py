"""Build validated Silver snapshots for clients, products, and uplift datasets."""

from __future__ import annotations

import argparse
import json
import math
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import (
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    replace_hdfs_paths,
)
from spark_jobs.silver_common import (
    add_lineage,
    add_reject_columns,
    parse_iso_date,
    silver_data_uri,
    silver_metadata_uri,
    silver_reject_uri,
    stage_parquet,
    write_json_metadata,
)
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True)
class DimensionSpec:
    name: str
    bronze_path: str
    silver_path: str
    primary_key: str


DIMENSIONS = (
    DimensionSpec("clients", "bronze/clients", "clients", "client_id"),
    DimensionSpec("products", "bronze/products", "products", "product_id"),
    DimensionSpec("uplift_train", "bronze/uplift/train", "uplift/train", "client_id"),
    DimensionSpec("uplift_test", "bronze/uplift/test", "uplift/test", "client_id"),
)


def _clean_primary_key(
    frame: DataFrame, spec: DimensionSpec, run_id: str, silver_timestamp: datetime
) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    from pyspark.sql import functions as functions

    payload_columns = [
        column for column in frame.columns if column not in {spec.primary_key, "ingest_ts"}
    ]
    payload_hash = functions.sha2(
        functions.to_json(functions.struct(*payload_columns)), 256
    ).alias("_payload_hash")
    hashed = frame.withColumn("_payload_hash", payload_hash)
    key_stats = hashed.groupBy(spec.primary_key).agg(
        functions.count(functions.lit(1)).alias("_key_rows"),
        functions.countDistinct("_payload_hash").alias("_payload_versions"),
    )
    conflicting_keys = key_stats.where(functions.col("_payload_versions") > 1).select(
        spec.primary_key
    )
    conflicting = frame.join(conflicting_keys, spec.primary_key, "inner")
    accepted_source = frame.join(conflicting_keys, spec.primary_key, "left_anti")
    accepted = accepted_source.dropDuplicates([spec.primary_key])
    rejects = add_reject_columns(
        conflicting,
        reason_codes=functions.array(functions.lit("CONFLICTING_PRIMARY_KEY")),
        run_id=run_id,
        silver_timestamp=silver_timestamp,
    )

    input_rows = frame.count()
    output_rows = accepted.count()
    reject_rows = rejects.count()
    metrics = {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "reject_rows": reject_rows,
        "identical_duplicate_rows": input_rows - reject_rows - output_rows,
    }
    return accepted, rejects, metrics


def _client_anomalies(frame: DataFrame, snapshot_date: date) -> dict[str, int]:
    from pyspark.sql import functions as functions

    snapshot_end = datetime.combine(snapshot_date, datetime.max.time()).replace(tzinfo=None)
    row = frame.agg(
        functions.sum(
            functions.when(
                (functions.col("age") < 0) | (functions.col("age") > 100), 1
            ).otherwise(0)
        ).alias("age_outside_0_100"),
        functions.sum(
            functions.when(
                functions.col("first_redeem_date") < functions.col("first_issue_date"), 1
            ).otherwise(0)
        ).alias("redeem_before_issue"),
        functions.sum(
            functions.when(
                functions.col("first_redeem_date") > functions.lit(snapshot_end), 1
            ).otherwise(0)
        ).alias("redeem_after_snapshot"),
    ).first()
    return {name: int(row[name] or 0) for name in row.asDict()}


def _product_anomalies(frame: DataFrame) -> dict[str, int]:
    from pyspark.sql import functions as functions

    tracked = [
        "level_1",
        "level_2",
        "level_3",
        "level_4",
        "segment_id",
        "brand_id",
        "vendor_id",
        "netto",
    ]
    expressions = [
        functions.sum(functions.when(functions.col(column).isNull(), 1).otherwise(0)).alias(
            f"null_{column}"
        )
        for column in tracked
    ]
    expressions.append(
        functions.sum(functions.when(functions.col("netto") <= 0, 1).otherwise(0)).alias(
            "nonpositive_netto"
        )
    )
    row = frame.agg(*expressions).first()
    return {name: int(row[name] or 0) for name in row.asDict()}


def _uplift_metrics(train: DataFrame, clients: DataFrame) -> dict[str, Any]:
    from pyspark.sql import functions as functions

    rates = {
        str(row["treatment_flg"]): {
            "rows": int(row["rows"]),
            "target_rate": float(row["target_rate"]),
        }
        for row in train.groupBy("treatment_flg")
        .agg(functions.count("*").alias("rows"), functions.avg("target").alias("target_rate"))
        .collect()
    }
    joined = train.join(clients.select("client_id", "age", "gender"), "client_id")
    age_stats = {
        int(row["treatment_flg"]): row
        for row in joined.groupBy("treatment_flg")
        .agg(functions.avg("age").alias("mean"), functions.var_samp("age").alias("variance"))
        .collect()
    }
    age_smd = None
    if 0 in age_stats and 1 in age_stats:
        pooled = math.sqrt(
            (float(age_stats[0]["variance"] or 0) + float(age_stats[1]["variance"] or 0))
            / 2
        )
        if pooled:
            age_smd = (float(age_stats[1]["mean"]) - float(age_stats[0]["mean"])) / pooled
    gender_rows = joined.groupBy("treatment_flg", "gender").count().collect()
    gender_counts: dict[int, dict[str, int]] = {0: {}, 1: {}}
    for row in gender_rows:
        gender_counts[int(row["treatment_flg"])][str(row["gender"])] = int(row["count"])
    gender_rate_differences: dict[str, float] = {}
    total_control = sum(gender_counts[0].values())
    total_treatment = sum(gender_counts[1].values())
    for gender in sorted(set(gender_counts[0]) | set(gender_counts[1])):
        control_rate = gender_counts[0].get(gender, 0) / total_control if total_control else 0
        treatment_rate = (
            gender_counts[1].get(gender, 0) / total_treatment if total_treatment else 0
        )
        gender_rate_differences[gender] = treatment_rate - control_rate
    return {
        "label_rates": rates,
        "age_smd": age_smd,
        "gender_rate_differences": gender_rate_differences,
    }


def build_silver_dimensions(
    *, hdfs_base_uri: str, bronze_ingest_date: date, snapshot_date: date
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as functions

    normalized_base = normalize_hdfs_base_uri(hdfs_base_uri)
    run_id = uuid.uuid4().hex
    silver_timestamp = datetime.now(UTC).replace(tzinfo=None)
    snapshot_partition = f"snapshot_date={snapshot_date.isoformat()}"
    staging_root = f"{normalized_base}/tmp/silver-dimensions/{run_id}"
    backup_root = f"{normalized_base}/tmp/silver-dimensions-backup/{run_id}"
    spark = SparkSession.builder.appName(f"silver-dimensions-{run_id}").config(
        "spark.sql.session.timeZone", "UTC"
    ).getOrCreate()
    hdfs = filesystem(spark, staging_root)

    try:
        cleaned: dict[str, DataFrame] = {}
        rejects: dict[str, DataFrame] = {}
        metrics: dict[str, dict[str, Any]] = {}
        for spec in DIMENSIONS:
            source_uri = (
                f"{normalized_base}/{spec.bronze_path}/"
                f"ingest_date={bronze_ingest_date.isoformat()}"
            )
            source = spark.read.parquet(source_uri)
            if spec.name == "uplift_train":
                invalid_labels = source.where(
                    ~functions.col("treatment_flg").isin(0, 1)
                    | ~functions.col("target").isin(0, 1)
                ).count()
                if invalid_labels:
                    raise ValueError(
                        f"uplift_train contains {invalid_labels} invalid treatment/target rows"
                    )
            accepted, rejected, entity_metrics = _clean_primary_key(
                source, spec, run_id, silver_timestamp
            )
            cleaned[spec.name] = accepted
            rejects[spec.name] = rejected
            metrics[spec.name] = entity_metrics

        clients = cleaned["clients"]
        client_ids = clients.select("client_id")
        for uplift_name in ("uplift_train", "uplift_test"):
            missing = cleaned[uplift_name].join(client_ids, "client_id", "left_anti").count()
            if missing:
                raise ValueError(
                    f"{uplift_name} contains {missing} client_id values absent "
                    "from Silver clients"
                )

        metrics["clients"]["anomalies"] = _client_anomalies(clients, snapshot_date)
        metrics["products"]["anomalies"] = _product_anomalies(cleaned["products"])
        metrics["uplift_train"]["quality"] = _uplift_metrics(
            cleaned["uplift_train"], clients
        )

        staged_targets: list[tuple[str, str]] = []
        for spec in DIMENSIONS:
            data = add_lineage(
                cleaned[spec.name],
                run_id=run_id,
                silver_timestamp=silver_timestamp,
                source_ingest_date=bronze_ingest_date.isoformat(),
            )
            rejected = rejects[spec.name].withColumn(
                "source_ingest_date", functions.lit(bronze_ingest_date.isoformat())
            )
            staged_data = f"{staging_root}/data/{spec.silver_path}/{snapshot_partition}"
            staged_reject = f"{staging_root}/rejects/{spec.name}/{snapshot_partition}"
            stage_parquet(data, staged_data)
            stage_parquet(rejected, staged_reject)
            staged_targets.extend(
                [
                    (
                        staged_data,
                        silver_data_uri(
                            hdfs_base_uri, spec.silver_path, snapshot_partition
                        ),
                    ),
                    (
                        staged_reject,
                        silver_reject_uri(
                            hdfs_base_uri, spec.name, snapshot_partition
                        ),
                    ),
                ]
            )

        metadata = {
            "job": "silver_dimensions",
            "run_id": run_id,
            "bronze_ingest_date": bronze_ingest_date.isoformat(),
            "snapshot_date": snapshot_date.isoformat(),
            "created_at": silver_timestamp.isoformat(),
            "entities": metrics,
        }
        staged_metadata = f"{staging_root}/metadata/{snapshot_partition}"
        write_json_metadata(spark, staged_metadata, metadata)
        staged_targets.append(
            (
                staged_metadata,
                silver_metadata_uri(hdfs_base_uri, "dimensions", snapshot_partition),
            )
        )
        replace_hdfs_paths(spark, staged_targets, backup_root)
        return metadata
    finally:
        hdfs.delete(hadoop_path(spark, staging_root), True)
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--bronze-ingest-date", required=True, type=parse_iso_date)
    parser.add_argument("--snapshot-date", required=True, type=parse_iso_date)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_silver_dimensions(
        hdfs_base_uri=args.hdfs_base_uri,
        bronze_ingest_date=args.bronze_ingest_date,
        snapshot_date=args.snapshot_date,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
