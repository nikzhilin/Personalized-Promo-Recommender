"""Shared typed-CSV and transactional HDFS helpers for Bronze jobs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from spark_jobs.data_contracts import DATA_CONTRACTS, FieldType

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession


@dataclass(frozen=True)
class PreparedCsv:
    frame: DataFrame
    invalid_columns: dict[str, str]

    def data_frame(self) -> DataFrame:
        return self.frame.drop(*self.invalid_columns.values())


@dataclass(frozen=True)
class CsvProfile:
    group: str | None
    rows: int
    invalid_counts: dict[str, int]


def normalize_hdfs_base_uri(base_uri: str) -> str:
    normalized = base_uri.rstrip("/")
    if not normalized.startswith("hdfs://"):
        raise ValueError("hdfs_base_uri must start with hdfs://")
    return normalized


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


def prepare_typed_csv(
    spark: SparkSession,
    source_path: Path,
    source_file: str,
    ingest_timestamp: datetime,
) -> PreparedCsv:
    from pyspark.sql import functions as functions

    if not source_path.is_file():
        raise FileNotFoundError(f"required source file does not exist: {source_path}")

    contract = DATA_CONTRACTS[source_file]
    source_uri = source_path.resolve().as_uri()
    raw = spark.read.option("header", True).option("mode", "FAILFAST").csv(source_uri)
    missing_columns = sorted(set(contract.fields) - set(raw.columns))
    if missing_columns:
        raise ValueError(
            f"{source_file} is missing required columns: {', '.join(missing_columns)}"
        )

    typed_columns: list[Column] = []
    invalid_columns: dict[str, str] = {}
    invalid_expressions: list[Column] = []
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

        invalid_column = f"_invalid__{field_name}"
        invalid_columns[field_name] = invalid_column
        invalid_expressions.append(invalid.alias(invalid_column))
        typed_columns.append(typed_value.alias(field_name))

    frame = raw.select(*typed_columns, *invalid_expressions).withColumns(
        {
            "ingest_ts": functions.lit(ingest_timestamp).cast("timestamp"),
            "source_file": functions.lit(source_file),
        }
    )
    return PreparedCsv(frame=frame, invalid_columns=invalid_columns)


def collect_csv_profiles(
    prepared: PreparedCsv, *, group_column: str | None = None
) -> list[CsvProfile]:
    from pyspark.sql import functions as functions

    aggregations = [functions.count(functions.lit(1)).alias("_rows")]
    aggregations.extend(
        functions.sum(functions.when(functions.col(column), 1).otherwise(0)).alias(column)
        for column in prepared.invalid_columns.values()
    )
    if group_column is None:
        rows = prepared.frame.agg(*aggregations).collect()
    else:
        rows = prepared.frame.groupBy(group_column).agg(*aggregations).collect()

    profiles: list[CsvProfile] = []
    for row in rows:
        group = (
            None
            if group_column is None or row[group_column] is None
            else str(row[group_column])
        )
        invalid_counts = {
            field_name: int(row[invalid_column] or 0)
            for field_name, invalid_column in prepared.invalid_columns.items()
            if row[invalid_column]
        }
        profiles.append(
            CsvProfile(
                group=group,
                rows=int(row["_rows"]),
                invalid_counts=invalid_counts,
            )
        )
    return profiles


def require_valid_profiles(profiles: list[CsvProfile], source_file: str) -> int:
    total_rows = sum(profile.rows for profile in profiles)
    invalid_counts: dict[str, int] = {}
    for profile in profiles:
        for field_name, count in profile.invalid_counts.items():
            invalid_counts[field_name] = invalid_counts.get(field_name, 0) + count

    if invalid_counts:
        rendered = ", ".join(f"{name}={count}" for name, count in invalid_counts.items())
        raise ValueError(f"{source_file} contains invalid values: {rendered}")
    if total_rows == 0:
        raise ValueError(f"{source_file} has no data rows")
    return total_rows


def hadoop_path(spark: SparkSession, uri: str) -> Any:
    return spark._jvm.org.apache.hadoop.fs.Path(uri)  # noqa: SLF001


def filesystem(spark: SparkSession, uri: str) -> Any:
    path = hadoop_path(spark, uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001


def replace_hdfs_paths(
    spark: SparkSession, staged_targets: list[tuple[str, str]], backup_root: str
) -> None:
    hdfs = filesystem(spark, backup_root)
    committed: list[tuple[Any, Any | None]] = []
    cleanup_backup = False
    try:
        for partition_index, (staged_uri, target_uri) in enumerate(staged_targets):
            staged_path = hadoop_path(spark, staged_uri)
            target_path = hadoop_path(spark, target_uri)
            backup_path = hadoop_path(spark, f"{backup_root}/{partition_index}")
            hdfs.mkdirs(target_path.getParent())

            previous_path = None
            if hdfs.exists(target_path):
                hdfs.mkdirs(backup_path.getParent())
                if not hdfs.rename(target_path, backup_path):
                    raise RuntimeError(f"cannot move existing partition to backup: {target_uri}")
                previous_path = backup_path

            if not hdfs.rename(staged_path, target_path):
                if previous_path is not None and not hdfs.rename(previous_path, target_path):
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
            removed = hdfs.delete(target_path, True)
            restored = previous_path is None or hdfs.rename(previous_path, target_path)
            rollback_failed = rollback_failed or not removed or not restored
        if rollback_failed:
            raise RuntimeError(
                f"Bronze publication and rollback failed; backups retained under {backup_root}"
            ) from publish_error
        cleanup_backup = True
        raise
    finally:
        if cleanup_backup:
            hdfs.delete(hadoop_path(spark, backup_root), True)
