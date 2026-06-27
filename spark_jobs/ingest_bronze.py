"""Ingest the small raw CSV entities into typed HDFS Bronze partitions."""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from spark_jobs.bronze_common import (
    collect_csv_profiles,
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    prepare_typed_csv,
    replace_hdfs_paths,
    require_valid_profiles,
)
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


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
    normalized_base = normalize_hdfs_base_uri(base_uri)
    return f"{normalized_base}/{dataset.relative_path}/ingest_date={ingest_date.isoformat()}"


def selected_datasets(names: list[str] | None) -> list[BronzeDataset]:
    if not names:
        return list(BRONZE_DATASETS.values())
    return [BRONZE_DATASETS[name] for name in dict.fromkeys(names)]


def _read_typed_dataset(
    spark: SparkSession, source_path: Path, dataset: BronzeDataset, ingest_timestamp: datetime
) -> tuple[DataFrame, int]:
    prepared = prepare_typed_csv(
        spark,
        source_path,
        dataset.source_file,
        ingest_timestamp,
    )
    profiles = collect_csv_profiles(prepared)
    row_count = require_valid_profiles(profiles, dataset.source_file)
    return prepared.data_frame(), row_count


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
    normalized_base = normalize_hdfs_base_uri(hdfs_base_uri)
    staging_root = f"{normalized_base}/tmp/bronze/{run_id}"
    backup_root = f"{normalized_base}/tmp/bronze-backup/{run_id}"
    ingest_timestamp = datetime.now(UTC).replace(tzinfo=None)
    spark = SparkSession.builder.appName(f"ingest-bronze-{run_id}").getOrCreate()
    staged_targets: list[tuple[str, str]] = []
    results: dict[str, dict[str, object]] = {}
    hdfs = filesystem(spark, staging_root)

    try:
        for dataset in datasets:
            source_path = data_dir / dataset.source_file
            frame, row_count = _read_typed_dataset(
                spark, source_path, dataset, ingest_timestamp
            )
            target_uri = bronze_partition_uri(hdfs_base_uri, dataset, ingest_date)
            staged_uri = f"{staging_root}/{dataset.relative_path}/ingest_date={ingest_date}"
            frame.write.mode("errorifexists").option("compression", "snappy").parquet(staged_uri)
            staged_targets.append((staged_uri, target_uri))
            results[dataset.name] = {"rows": row_count, "target": target_uri}

        replace_hdfs_paths(spark, staged_targets, backup_root)
        return {
            "run_id": run_id,
            "ingest_date": ingest_date.isoformat(),
            "datasets": results,
        }
    finally:
        hdfs.delete(hadoop_path(spark, staging_root), True)
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
