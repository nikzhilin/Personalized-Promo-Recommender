"""Export one bounded PostgreSQL feedback batch into canonical HDFS event partitions."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from hdfs import InsecureClient
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict, Field

LOCK_NAME = "personalized_promo_feedback_export"
ZERO_UUID = uuid.UUID(int=0)

FEEDBACK_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("request_id", pa.string(), nullable=False),
        pa.field("client_id", pa.string(), nullable=False),
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("shown_discount", pa.decimal128(5, 4), nullable=False),
        pa.field("purchase_value", pa.decimal128(12, 2)),
        pa.field("discount_cost", pa.decimal128(12, 2)),
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("received_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("verification_status", pa.string(), nullable=False),
        pa.field("event_fingerprint", pa.string(), nullable=False),
        pa.field("event_date", pa.date32(), nullable=False),
    ]
)


class FeedbackExportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: str = Field(min_length=1)
    export_name: str = Field(min_length=1)
    batch_size: int = Field(gt=0, le=1_000_000)
    hdfs_root: str = Field(pattern=r"^/[^\s]*$")
    compression: str = Field(pattern=r"^(snappy|gzip|zstd)$")
    required_replication: int = Field(gt=0)


@dataclass(frozen=True, order=True)
class Watermark:
    received_at: datetime
    event_id: uuid.UUID

    def __post_init__(self) -> None:
        if self.received_at.tzinfo is None or self.received_at.utcoffset() is None:
            raise ValueError("watermark timestamp must include timezone")


def load_feedback_export_config(path: str) -> FeedbackExportConfig:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"feedback export config does not exist: {source}")
    with source.open(encoding="utf-8") as stream:
        payload = yaml.safe_load(stream)
    if not isinstance(payload, dict):
        raise ValueError("feedback export config root must be a mapping")
    return FeedbackExportConfig.model_validate(payload)


def event_date_utc(created_at: datetime) -> date:
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("feedback created_at must include timezone")
    return created_at.astimezone(UTC).date()


def deterministic_batch_id(lower: Watermark, upper: Watermark) -> str:
    payload = "|".join(
        (
            lower.received_at.astimezone(UTC).isoformat(),
            str(lower.event_id),
            upper.received_at.astimezone(UTC).isoformat(),
            str(upper.event_id),
        )
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:32]


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    created_at = row["created_at"].astimezone(UTC)
    received_at = row["received_at"].astimezone(UTC)
    return {
        "event_id": str(row["event_id"]),
        "request_id": str(row["request_id"]),
        "client_id": str(row["client_id"]),
        "product_id": str(row["product_id"]),
        "event_type": str(row["event_type"]),
        "shown_discount": Decimal(str(row["shown_discount"])),
        "purchase_value": (
            None
            if row["purchase_value"] is None
            else Decimal(str(row["purchase_value"]))
        ),
        "discount_cost": (
            None
            if row["discount_cost"] is None
            else Decimal(str(row["discount_cost"]))
        ),
        "created_at": created_at,
        "received_at": received_at,
        "verification_status": str(row["verification_status"]),
        "event_fingerprint": str(row["event_fingerprint"]),
        "event_date": event_date_utc(created_at),
    }


def merge_feedback_rows(
    existing: list[dict[str, Any]], incoming: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in [*existing, *incoming]:
        event_id = str(row["event_id"])
        previous = merged.get(event_id)
        if previous is not None and previous["event_fingerprint"] != row["event_fingerprint"]:
            raise ValueError(f"feedback fingerprint conflict for event_id={event_id}")
        merged[event_id] = row
    return sorted(merged.values(), key=lambda row: (row["created_at"], row["event_id"]))


def _read_existing_partition(
    client: InsecureClient, partition: str, temporary: Path
) -> list[dict[str, Any]]:
    status = client.status(partition, strict=False)
    if status is None:
        return []
    rows: list[dict[str, Any]] = []
    for name in sorted(client.list(partition)):
        if not name.endswith(".parquet"):
            continue
        local = temporary / f"existing-{hashlib.sha1(name.encode()).hexdigest()}.parquet"
        client.download(f"{partition}/{name}", str(local), overwrite=True)
        rows.extend(pq.read_table(local, schema=FEEDBACK_SCHEMA).to_pylist())
    return rows


def _write_staged_partition(
    client: InsecureClient,
    path: str,
    rows: list[dict[str, Any]],
    temporary: Path,
    compression: str,
    required_replication: int,
) -> None:
    local = temporary / f"{path.rsplit('/', maxsplit=1)[-1]}.parquet"
    table = pa.Table.from_pylist(rows, schema=FEEDBACK_SCHEMA)
    pq.write_table(table, local, compression=compression)
    client.makedirs(path)
    remote_file = f"{path}/part-00000.parquet"
    client.upload(remote_file, str(local), overwrite=True)
    status = client.status(remote_file)
    if int(status.get("replication", 0)) < required_replication:
        raise ValueError(
            f"HDFS feedback file replication is below {required_replication}: {remote_file}"
        )


def replace_hdfs_paths(
    client: InsecureClient, pairs: list[tuple[str, str]], backup_root: str
) -> None:
    committed: list[tuple[str, str | None]] = []
    try:
        for index, (staged, target) in enumerate(pairs):
            backup = f"{backup_root}/{index}"
            previous: str | None = None
            client.makedirs(target.rsplit("/", maxsplit=1)[0])
            if client.status(target, strict=False) is not None:
                client.makedirs(backup.rsplit("/", maxsplit=1)[0])
                client.rename(target, backup)
                previous = backup
            try:
                client.rename(staged, target)
            except Exception:
                if previous is not None:
                    client.rename(previous, target)
                raise
            committed.append((target, previous))
    except Exception:
        for target, previous in reversed(committed):
            client.delete(target, recursive=True)
            if previous is not None:
                client.rename(previous, target)
        raise
    finally:
        client.delete(backup_root, recursive=True)


def _fetch_batch(
    connection: psycopg.Connection[Any], lower: Watermark, batch_size: int
) -> list[dict[str, Any]]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT event_id, request_id, client_id, product_id, event_type,
                   shown_discount, purchase_value, discount_cost, created_at,
                   received_at, verification_status, event_fingerprint
            FROM feedback_events
            WHERE (received_at, event_id) > (%s, %s)
            ORDER BY received_at, event_id
            LIMIT %s
            """,
            (lower.received_at, lower.event_id, batch_size),
        )
        return list(cursor.fetchall())


def export_feedback(
    *,
    database_url: str,
    webhdfs_url: str,
    config_path: str,
    hdfs_user: str = "promo",
) -> dict[str, Any]:
    config = load_feedback_export_config(config_path)
    client = InsecureClient(webhdfs_url, user=hdfs_user)
    with psycopg.connect(database_url, autocommit=True) as connection:
        locked = connection.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s))", (LOCK_NAME,)
        ).fetchone()[0]
        if not locked:
            raise RuntimeError("another feedback export is already running")
        try:
            state = connection.execute(
                """
                SELECT watermark_received_at, watermark_event_id
                FROM feedback_export_state WHERE export_name = %s
                """,
                (config.export_name,),
            ).fetchone()
            if state is None:
                raise ValueError(f"feedback export state is missing: {config.export_name}")
            lower = Watermark(state[0], state[1])
            batch = _fetch_batch(connection, lower, config.batch_size)
            if not batch:
                return {
                    "status": "no_op",
                    "export_name": config.export_name,
                    "rows": 0,
                }
            upper = Watermark(batch[-1]["received_at"], batch[-1]["event_id"])
            batch_id = deterministic_batch_id(lower, upper)
            normalized = [normalize_row(row) for row in batch]
            by_date: dict[date, list[dict[str, Any]]] = defaultdict(list)
            for row in normalized:
                by_date[row["event_date"]].append(row)
            root = config.hdfs_root.rstrip("/")
            staging_root = f"{root}/tmp/feedback-export/{batch_id}"
            backup_root = f"{root}/tmp/feedback-export-backup/{batch_id}"
            client.delete(staging_root, recursive=True)
            client.delete(backup_root, recursive=True)
            pairs: list[tuple[str, str]] = []
            output_rows = 0
            with tempfile.TemporaryDirectory(prefix="feedback-export-") as temporary_name:
                temporary = Path(temporary_name)
                for event_date, incoming in sorted(by_date.items()):
                    partition = f"{root}/feedback/events/event_date={event_date.isoformat()}"
                    existing = _read_existing_partition(client, partition, temporary)
                    merged = merge_feedback_rows(existing, incoming)
                    output_rows += len(merged)
                    staged = (
                        f"{staging_root}/events/event_date={event_date.isoformat()}"
                    )
                    _write_staged_partition(
                        client,
                        staged,
                        merged,
                        temporary,
                        config.compression,
                        config.required_replication,
                    )
                    pairs.append((staged, partition))
                profile: dict[str, int] = defaultdict(int)
                for row in normalized:
                    profile[row["verification_status"]] += 1
                metadata = {
                    "job": "export_feedback",
                    "batch_id": batch_id,
                    "config_version": config.version,
                    "created_at": datetime.now(UTC).isoformat(),
                    "input_rows": len(batch),
                    "affected_event_dates": [value.isoformat() for value in sorted(by_date)],
                    "partition_output_rows": output_rows,
                    "lower_watermark": {
                        "received_at": lower.received_at.isoformat(),
                        "event_id": str(lower.event_id),
                    },
                    "upper_watermark": {
                        "received_at": upper.received_at.isoformat(),
                        "event_id": str(upper.event_id),
                    },
                    "verification_status_counts": dict(sorted(profile.items())),
                }
                metadata_staged = f"{staging_root}/metadata/export_batch={batch_id}"
                client.makedirs(metadata_staged)
                with client.write(
                    f"{metadata_staged}/_metadata.json", encoding="utf-8", overwrite=True
                ) as writer:
                    json.dump(metadata, writer, ensure_ascii=True, sort_keys=True)
                    writer.write("\n")
                pairs.append(
                    (
                        metadata_staged,
                        f"{root}/feedback/metadata/export_batch={batch_id}",
                    )
                )
                replace_hdfs_paths(client, pairs, backup_root)
            with connection.transaction():
                updated = connection.execute(
                    """
                    UPDATE feedback_export_state
                    SET watermark_received_at = %s,
                        watermark_event_id = %s,
                        updated_at = now()
                    WHERE export_name = %s
                      AND watermark_received_at = %s
                      AND watermark_event_id = %s
                    """,
                    (
                        upper.received_at,
                        upper.event_id,
                        config.export_name,
                        lower.received_at,
                        lower.event_id,
                    ),
                )
                if updated.rowcount != 1:
                    raise RuntimeError("feedback watermark compare-and-swap failed")
            client.delete(staging_root, recursive=True)
            return {**metadata, "status": "exported"}
        finally:
            connection.execute("SELECT pg_advisory_unlock(hashtext(%s))", (LOCK_NAME,))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--webhdfs-url", default="http://namenode:9870")
    parser.add_argument("--hdfs-user", default="promo")
    parser.add_argument(
        "--config", default="/workspace/configs/feedback_export.yaml"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = export_feedback(
        database_url=args.database_url,
        webhdfs_url=args.webhdfs_url,
        hdfs_user=args.hdfs_user,
        config_path=args.config,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
