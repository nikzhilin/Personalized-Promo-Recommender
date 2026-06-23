"""Ingest the small raw CSV entities into typed HDFS Bronze partitions."""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from spark_jobs.data_contracts import DATA_CONTRACTS, FieldType

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession


@dataclass(frozen=True)
class BronzeDataset:
    name: str
    source_file: str
    relative_path: str


BRONZE_DATASETS: dict[str, BronzeDataset] = {
    "clients": BronzeDataset("clients", "clients.csv", "bronze/clients"),
    "products": BronzeDataset("products", "products.csv", "bronze/products"),
    "uplift_train": BronzeDataset("uplift_train", "uplift_train.csv", "bronze/uplift/train"),
    "uplift_test": BronzeDataset("uplift_test", "uplift_test.csv", "bronze/uplift/test"),
}


def parse_ingest_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("ingest date must use YYYY-MM-DD") from error


def bronze_partition_uri(base_uri: str, dataset: BronzeDataset, ingest_date: date) -> str:
    normalized_base = base_uri.rstrip("/")
    if not normalized_base.startswith("hdfs://"):
        raise ValueError("hdfs_base_uri must start with hdfs://")
    return f"{normalized_base}/{dataset.relative_path}/ingest_date={ingest_date.isoformat()}"


def selected_datasets(names: list[str] | None) -> list[BronzeDataset]:
    if not names:
        return list(BRONZE_DATASETS.values())
    return [BRONZE_DATASETS[name] for name in dict.fromkeys(names)]


def _typed_column(raw_column: Column, field_type: FieldType) -> Column:
    from pyspark.sql import functions as functions

    normalized = functions.when(functions.trim(raw_column) == "", None).otherwise(
        functions.trim(raw_column)
    )
    if field_type == FieldType.STRING:
        return normalized.cast("string")
    if field_type == FieldType.INTEGER:
        return normalized.cast("integer")
    if field_type == FieldType.FLOAT:
        return normalized.cast("double")
    if field_type == FieldType.DATETIME:
        return functions.to_timestamp(normalized)
    raise ValueError(f"unsupported field type: {field_type}")


def _read_typed_dataset(
    spark: SparkSession, source_path: Path, dataset: BronzeDataset, ingest_timestamp: datetime
) -> tuple[DataFrame, int]:
    from pyspark.sql import functions as functions

    contract = DATA_CONTRACTS[dataset.source_file]
    source_uri = source_path.resolve().as_uri()
    raw = spark.read.option("header", True).option("mode", "FAILFAST").csv(source_uri)
    missing_columns = sorted(set(contract.fields) - set(raw.columns))
    if missing_columns:
        raise ValueError(
            f"{dataset.source_file} is missing required columns: {', '.join(missing_columns)}"
        )

    typed_columns: list[Column] = []
    invalid_conditions: dict[str, Column] = {}
    for field_name, field_contract in contract.fields.items():
        raw_value = functions.trim(functions.col(field_name))
        typed_value = _typed_column(functions.col(field_name), field_contract.field_type)
        invalid = (raw_value != "") & typed_value.isNull()
        if field_contract.field_type == FieldType.FLOAT:
            invalid = invalid | functions.isnan(typed_value) | (
                functions.abs(typed_value) == float("inf")
            )
        if not field_contract.nullable:
            invalid = invalid | raw_value.isNull() | (raw_value == "")
        if field_contract.allowed_values is not None:
            invalid = invalid | (
                typed_value.isNotNull()
                & ~typed_value.isin(sorted(field_contract.allowed_values))
            )
        invalid_conditions[field_name] = invalid
        typed_columns.append(typed_value.alias(field_name))

    invalid_counts_row = raw.agg(
        *[
            functions.sum(functions.when(condition, 1).otherwise(0)).alias(field_name)
            for field_name, condition in invalid_conditions.items()
        ]
    ).first()
    invalid_counts = {
        field_name: int(invalid_counts_row[field_name] or 0)
        for field_name in invalid_conditions
        if invalid_counts_row[field_name]
    }
    if invalid_counts:
        rendered = ", ".join(f"{name}={count}" for name, count in invalid_counts.items())
        raise ValueError(f"{dataset.source_file} contains invalid values: {rendered}")

    typed = raw.select(*typed_columns).withColumns(
        {
            "ingest_ts": functions.lit(ingest_timestamp).cast("timestamp"),
            "source_file": functions.lit(dataset.source_file),
        }
    )
    row_count = typed.count()
    if row_count == 0:
        raise ValueError(f"{dataset.source_file} has no data rows")
    return typed, row_count


def _hadoop_path(spark: SparkSession, uri: str) -> Any:
    return spark._jvm.org.apache.hadoop.fs.Path(uri)  # noqa: SLF001


def _filesystem(spark: SparkSession, uri: str) -> Any:
    path = _hadoop_path(spark, uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001


def _replace_partitions(
    spark: SparkSession, staged_targets: list[tuple[str, str]], backup_root: str
) -> None:
    filesystem = _filesystem(spark, backup_root)
    committed: list[tuple[Any, Any | None]] = []
    cleanup_backup = False
    try:
        for partition_index, (staged_uri, target_uri) in enumerate(staged_targets):
            staged_path = _hadoop_path(spark, staged_uri)
            target_path = _hadoop_path(spark, target_uri)
            backup_path = _hadoop_path(spark, f"{backup_root}/{partition_index}")
            parent = target_path.getParent()
            filesystem.mkdirs(parent)

            previous_path = None
            if filesystem.exists(target_path):
                filesystem.mkdirs(backup_path.getParent())
                if not filesystem.rename(target_path, backup_path):
                    raise RuntimeError(f"cannot move existing partition to backup: {target_uri}")
                previous_path = backup_path

            if not filesystem.rename(staged_path, target_path):
                if previous_path is not None and not filesystem.rename(
                    previous_path, target_path
                ):
                    raise RuntimeError(
                        f"cannot publish or restore existing partition: {target_uri}; "
                        f"backup retained under {backup_root}"
                    )
                raise RuntimeError(f"cannot publish staged partition: {target_uri}")
            committed.append((target_path, previous_path))
        cleanup_backup = True
    except Exception as publish_error:
        rollback_failed = False
        for target_path, previous_path in reversed(committed):
            removed = filesystem.delete(target_path, True)
            restored = previous_path is None or filesystem.rename(previous_path, target_path)
            rollback_failed = rollback_failed or not removed or not restored
        if rollback_failed:
            raise RuntimeError(
                f"Bronze publication and rollback failed; backups retained under {backup_root}"
            ) from publish_error
        cleanup_backup = True
        raise
    finally:
        if cleanup_backup:
            filesystem.delete(_hadoop_path(spark, backup_root), True)


def ingest_bronze(
    *,
    data_dir: Path,
    hdfs_base_uri: str,
    ingest_date: date,
    dataset_names: list[str] | None = None,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession

    datasets = selected_datasets(dataset_names)
    run_id = uuid.uuid4().hex
    normalized_base = hdfs_base_uri.rstrip("/")
    staging_root = f"{normalized_base}/tmp/bronze/{run_id}"
    backup_root = f"{normalized_base}/tmp/bronze-backup/{run_id}"
    ingest_timestamp = datetime.now(UTC).replace(tzinfo=None)
    spark = SparkSession.builder.appName(f"ingest-bronze-{run_id}").getOrCreate()
    staged_targets: list[tuple[str, str]] = []
    results: dict[str, dict[str, object]] = {}
    filesystem = _filesystem(spark, staging_root)

    try:
        for dataset in datasets:
            source_path = data_dir / dataset.source_file
            if not source_path.is_file():
                raise FileNotFoundError(f"required source file does not exist: {source_path}")
            frame, row_count = _read_typed_dataset(
                spark, source_path, dataset, ingest_timestamp
            )
            target_uri = bronze_partition_uri(hdfs_base_uri, dataset, ingest_date)
            staged_uri = f"{staging_root}/{dataset.relative_path}/ingest_date={ingest_date}"
            frame.write.mode("errorifexists").option("compression", "snappy").parquet(staged_uri)
            staged_targets.append((staged_uri, target_uri))
            results[dataset.name] = {"rows": row_count, "target": target_uri}

        _replace_partitions(spark, staged_targets, backup_root)
        return {
            "run_id": run_id,
            "ingest_date": ingest_date.isoformat(),
            "datasets": results,
        }
    finally:
        filesystem.delete(_hadoop_path(spark, staging_root), True)
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("/data/raw"))
    parser.add_argument(
        "--hdfs-base-uri", default="hdfs://namenode:9000/promo", help="HDFS /promo root"
    )
    parser.add_argument("--ingest-date", type=parse_ingest_date, required=True)
    parser.add_argument(
        "--dataset",
        dest="datasets",
        action="append",
        choices=tuple(BRONZE_DATASETS),
        help="Dataset to ingest; repeat the option or omit it to ingest all supported datasets",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = ingest_bronze(
        data_dir=args.data_dir,
        hdfs_base_uri=args.hdfs_base_uri,
        ingest_date=args.ingest_date,
        dataset_names=args.datasets,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
