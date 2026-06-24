"""Shared paths, lineage columns, and publication helpers for Silver jobs."""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import filesystem, hadoop_path, normalize_hdfs_base_uri

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import DataFrame, SparkSession


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from error


def silver_data_uri(base_uri: str, entity: str, partition: str) -> str:
    return f"{normalize_hdfs_base_uri(base_uri)}/silver/{entity}/{partition}"


def silver_reject_uri(base_uri: str, entity: str, partition: str) -> str:
    return f"{normalize_hdfs_base_uri(base_uri)}/silver/rejects/{entity}/{partition}"


def silver_metadata_uri(base_uri: str, job: str, partition: str) -> str:
    return f"{normalize_hdfs_base_uri(base_uri)}/silver/metadata/{job}/{partition}"


def add_lineage(
    frame: DataFrame,
    *,
    run_id: str,
    silver_timestamp: datetime,
    source_ingest_date: str | None = None,
    source_dimensions_snapshot_date: str | None = None,
) -> DataFrame:
    from pyspark.sql import functions as functions

    columns: dict[str, Any] = {
        "silver_run_id": functions.lit(run_id),
        "silver_ts": functions.lit(silver_timestamp).cast("timestamp"),
    }
    if source_ingest_date is not None:
        columns["source_ingest_date"] = functions.lit(source_ingest_date)
    if source_dimensions_snapshot_date is not None:
        columns["source_dimensions_snapshot_date"] = functions.lit(
            source_dimensions_snapshot_date
        )
    return frame.withColumns(columns)


def add_reject_columns(
    frame: DataFrame,
    *,
    reason_codes: Any,
    run_id: str,
    silver_timestamp: datetime,
) -> DataFrame:
    from pyspark.sql import functions as functions

    return frame.withColumns(
        {
            "reason_codes": reason_codes.cast("array<string>"),
            "silver_run_id": functions.lit(run_id),
            "silver_ts": functions.lit(silver_timestamp).cast("timestamp"),
        }
    )


def write_json_metadata(spark: SparkSession, directory_uri: str, payload: dict[str, Any]) -> None:
    hdfs = filesystem(spark, directory_uri)
    directory = hadoop_path(spark, directory_uri)
    hdfs.mkdirs(directory)
    output = hdfs.create(hadoop_path(spark, f"{directory_uri}/_metadata.json"), True)
    try:
        output.writeBytes(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        output.writeBytes("\n")
    finally:
        output.close()


def stage_parquet(frame: DataFrame, uri: str) -> None:
    frame.write.mode("errorifexists").option("compression", "snappy").parquet(uri)
