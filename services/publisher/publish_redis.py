"""Publish a replicated HDFS recommendation snapshot into an atomic Redis namespace."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
from collections.abc import Iterator, Sequence
from datetime import date
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter

from services.contracts import Recommendation
from services.online_config import OnlineStoreConfig, load_online_store_config
from spark_jobs.bronze_common import filesystem, hadoop_path, normalize_hdfs_base_uri
from spark_jobs.gold_common import gold_data_uri, gold_metadata_uri
from spark_jobs.silver_common import parse_iso_date

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

RUN_ID_PATTERN = re.compile(r"[0-9a-f]{32}")
RECOMMENDATION_LIST = TypeAdapter(list[Recommendation])


def active_postgres_fallback(database_url: str) -> str | None:
    import psycopg

    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            "SELECT snapshot_id FROM fallback_snapshot_state WHERE singleton = TRUE"
        ).fetchone()
        return None if row is None else str(row[0])


def restore_postgres_fallback(database_url: str, snapshot_id: str | None) -> None:
    import psycopg

    with psycopg.connect(database_url) as connection, connection.transaction():
        if snapshot_id is None:
            connection.execute("DELETE FROM fallback_snapshot_state WHERE singleton = TRUE")
        else:
            connection.execute(
                """
                UPDATE fallback_snapshot_state
                SET snapshot_id = %s, activated_at = now()
                WHERE singleton = TRUE
                """,
                (snapshot_id,),
            )


def publish_postgres_fallback(
    database_url: str,
    snapshot_id: str,
    recommendations: Sequence[Recommendation],
) -> None:
    """Replace and activate one complete fallback snapshot transactionally."""
    import psycopg

    if not recommendations:
        raise ValueError("cannot publish an empty PostgreSQL fallback snapshot")
    with psycopg.connect(database_url) as connection, connection.transaction():
        connection.execute(
            "DELETE FROM fallback_recommendations WHERE snapshot_id = %s",
            (snapshot_id,),
        )
        connection.executemany(
            """
            INSERT INTO fallback_recommendations(
                snapshot_id, product_id, rank, discount, expected_profit,
                recsys_score, reason_code
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    snapshot_id,
                    recommendation.product_id,
                    recommendation.rank,
                    recommendation.discount,
                    recommendation.expected_profit,
                    recommendation.recsys_score,
                    recommendation.reason_code,
                )
                for recommendation in recommendations
            ],
        )
        count = connection.execute(
            "SELECT count(*) FROM fallback_recommendations WHERE snapshot_id = %s",
            (snapshot_id,),
        ).fetchone()[0]
        if count != len(recommendations):
            raise ValueError("PostgreSQL fallback snapshot row count mismatch")
        connection.execute(
            """
            INSERT INTO fallback_snapshot_state(singleton, snapshot_id, activated_at)
            VALUES (TRUE, %s, now())
            ON CONFLICT (singleton) DO UPDATE
            SET snapshot_id = EXCLUDED.snapshot_id, activated_at = EXCLUDED.activated_at
            """,
            (snapshot_id,),
        )


def compact_payload(recommendations: Sequence[Recommendation]) -> str:
    return json.dumps(
        [recommendation.model_dump(mode="json") for recommendation in recommendations],
        ensure_ascii=True,
        separators=(",", ":"),
    )


def choose_published_top_n(
    estimated_bytes: dict[int, int],
    *,
    max_snapshot_bytes: int,
    available_bytes: int,
    min_top_n: int,
) -> int:
    limit = min(max_snapshot_bytes, available_bytes)
    for top_n in sorted(estimated_bytes, reverse=True):
        if top_n >= min_top_n and estimated_bytes[top_n] <= limit:
            return top_n
    raise ValueError(
        "snapshot does not fit Redis memory budget even at configured minimum top-N"
    )


def _read_hdfs_json(spark: SparkSession, uri: str) -> dict[str, Any]:
    hdfs = filesystem(spark, uri)
    path = hadoop_path(spark, uri)
    if not hdfs.exists(path):
        raise FileNotFoundError(f"required HDFS metadata does not exist: {uri}")
    stream = hdfs.open(path)
    reader = spark._jvm.java.io.BufferedReader(  # noqa: SLF001
        spark._jvm.java.io.InputStreamReader(stream)  # noqa: SLF001
    )
    try:
        return json.loads(reader.readLine())
    finally:
        reader.close()


def require_hdfs_replication(spark: SparkSession, uri: str, required: int) -> int:
    hdfs = filesystem(spark, uri)
    root = hadoop_path(spark, uri)
    if not hdfs.exists(root):
        raise FileNotFoundError(f"HDFS snapshot path does not exist: {uri}")
    files = hdfs.listFiles(root, True)
    checked = 0
    while files.hasNext():
        status = files.next()
        if status.getLen() == 0:
            continue
        checked += 1
        if status.getReplication() < required:
            raise ValueError(
                f"HDFS file replication is below {required}: {status.getPath()}"
            )
        for block in status.getBlockLocations():
            if len(block.getHosts()) < required:
                raise ValueError(
                    f"HDFS live block replicas are below {required}: {status.getPath()}"
                )
    if checked == 0:
        raise ValueError(f"HDFS snapshot contains no non-empty files: {uri}")
    return checked


def _row_recommendation(row: Row, rank: int | None = None) -> Recommendation:
    return Recommendation(
        product_id=str(row["product_id"]),
        discount=float(row["discount"]),
        expected_profit=(
            None if row["expected_profit"] is None else float(row["expected_profit"])
        ),
        recsys_score=float(row["recsys_score"]),
        reason_code=str(row["reason_code"]),
        rank=int(rank if rank is not None else row["final_rank"]),
    )


def iter_client_recommendations(frame: DataFrame) -> Iterator[tuple[str, list[Recommendation]]]:
    current_client: str | None = None
    current: list[Recommendation] = []
    ordered = frame.select(
        "client_id",
        "product_id",
        "discount",
        "expected_profit",
        "recsys_score",
        "reason_code",
        "final_rank",
    ).orderBy("client_id", "final_rank")
    for row in ordered.toLocalIterator():
        client_id = str(row["client_id"])
        if current_client is not None and client_id != current_client:
            yield current_client, current
            current = []
        current_client = client_id
        current.append(_row_recommendation(row))
    if current_client is not None:
        yield current_client, current


def _fallback_recommendations(items: DataFrame, top_n: int) -> list[Recommendation]:
    from pyspark.sql import functions as functions

    rows = (
        items.where(functions.col("is_alcohol") == 0)
        .orderBy(
            functions.desc("unique_buyers"),
            functions.desc("total_sales_qty"),
            functions.asc("product_id"),
        )
        .limit(top_n)
        .collect()
    )
    count = len(rows)
    return [
        Recommendation(
            product_id=str(row["product_id"]),
            discount=0.0,
            expected_profit=None,
            recsys_score=(1.0 - index / count if count else 0.0),
            reason_code="COLD_START_POPULAR",
            rank=index + 1,
        )
        for index, row in enumerate(rows)
    ]


def _estimate_sizes(
    frame: DataFrame,
    config: OnlineStoreConfig,
    *,
    key_prefix: str,
    snapshot_id: str,
    fallback: Sequence[Recommendation],
    source_top_n: int,
) -> tuple[dict[int, int], int]:
    maximum = min(config.max_top_n, source_top_n)
    if maximum < config.min_top_n:
        raise ValueError("ranking source top-N is below online-store minimum top-N")
    raw_sizes = {top_n: 0 for top_n in range(config.min_top_n, maximum + 1)}
    client_count = 0
    for client_id, recommendations in iter_client_recommendations(frame):
        client_count += 1
        key_size = len(f"{key_prefix}:{snapshot_id}:user:{client_id}".encode())
        for top_n in raw_sizes:
            raw_sizes[top_n] += key_size + len(
                compact_payload(recommendations[:top_n]).encode()
            )
    return (
        {
            top_n: math.ceil(
                (
                    size
                    + len(compact_payload(fallback[:top_n]).encode())
                    + 4096
                )
                * config.memory_overhead_factor
            )
            for top_n, size in raw_sizes.items()
        },
        client_count,
    )


def _delete_namespace(redis_client: Any, pattern: str, batch_size: int) -> None:
    batch: list[Any] = []
    for key in redis_client.scan_iter(match=pattern, count=batch_size):
        batch.append(key)
        if len(batch) >= batch_size:
            redis_client.delete(*batch)
            batch = []
    if batch:
        redis_client.delete(*batch)


def _refresh_namespace_ttl(
    redis_client: Any, pattern: str, ttl_seconds: int, batch_size: int
) -> None:
    pipeline = redis_client.pipeline(transaction=False)
    pending = 0
    for key in redis_client.scan_iter(match=pattern, count=batch_size):
        pipeline.expire(key, ttl_seconds)
        pending += 1
        if pending >= batch_size:
            pipeline.execute()
            pipeline = redis_client.pipeline(transaction=False)
            pending = 0
    if pending:
        pipeline.execute()


def _validate_namespace(
    redis_client: Any,
    *,
    prefix: str,
    snapshot_id: str,
    expected_clients: int,
    expected_top_n: int,
) -> None:
    meta_key = f"{prefix}:{snapshot_id}:meta"
    fallback_key = f"{prefix}:{snapshot_id}:fallback"
    metadata = redis_client.hgetall(meta_key)
    decoded = {
        (key.decode() if isinstance(key, bytes) else str(key)): (
            value.decode() if isinstance(value, bytes) else str(value)
        )
        for key, value in metadata.items()
    }
    if int(decoded.get("client_count", -1)) != expected_clients:
        raise ValueError("Redis snapshot client count metadata mismatch")
    if int(decoded.get("published_top_n", -1)) != expected_top_n:
        raise ValueError("Redis snapshot published_top_n metadata mismatch")
    user_count = 0
    samples: list[Any] = []
    for key in redis_client.scan_iter(
        match=f"{prefix}:{snapshot_id}:user:*", count=1000
    ):
        user_count += 1
        if len(samples) < 3:
            samples.append(key)
    if user_count != expected_clients:
        raise ValueError(
            f"Redis snapshot user key mismatch: expected {expected_clients}, found {user_count}"
        )
    for key in samples:
        payload = redis_client.get(key)
        recommendations = RECOMMENDATION_LIST.validate_json(payload)
        if len(recommendations) > expected_top_n:
            raise ValueError("Redis user payload exceeds published top-N")
        if redis_client.ttl(key) <= 0:
            raise ValueError("Redis user key is missing snapshot TTL")
    fallback = RECOMMENDATION_LIST.validate_json(redis_client.get(fallback_key))
    if len(fallback) > expected_top_n:
        raise ValueError("Redis fallback payload exceeds published top-N")
    if redis_client.ttl(meta_key) <= 0 or redis_client.ttl(fallback_key) <= 0:
        raise ValueError("Redis snapshot metadata/fallback is missing TTL")


def publish_redis_snapshot(
    *,
    hdfs_base_uri: str,
    snapshot_date: date,
    redis_url: str,
    online_config: str,
    database_url: str,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from redis import Redis

    config = load_online_store_config(online_config)
    base = normalize_hdfs_base_uri(hdfs_base_uri)
    spark = (
        SparkSession.builder.appName(f"publish-redis-{snapshot_date.isoformat()}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    redis_client = Redis.from_url(
        redis_url,
        socket_connect_timeout=config.socket_timeout_seconds,
        socket_timeout=config.socket_timeout_seconds,
        decode_responses=False,
    )
    try:
        ranking_data_uri = gold_data_uri(base, "final_recommendations", snapshot_date)
        ranking_metadata_uri = (
            f"{gold_metadata_uri(base, 'final_recommendations', snapshot_date)}/_metadata.json"
        )
        checked_files = require_hdfs_replication(
            spark, ranking_data_uri, config.required_hdfs_replication
        )
        require_hdfs_replication(
            spark,
            gold_metadata_uri(base, "final_recommendations", snapshot_date),
            config.required_hdfs_replication,
        )
        metadata = _read_hdfs_json(spark, ranking_metadata_uri)
        snapshot_id = metadata.get("run_id")
        if not isinstance(snapshot_id, str) or RUN_ID_PATTERN.fullmatch(snapshot_id) is None:
            raise ValueError("ranking metadata run_id must be 32 lowercase hexadecimal characters")
        if metadata.get("snapshot_date") != snapshot_date.isoformat():
            raise ValueError("ranking metadata snapshot_date mismatch")
        prefix = config.key_prefix
        active_key = f"{prefix}:active_snapshot"
        active = redis_client.get(active_key)
        active_id = active.decode() if isinstance(active, bytes) else active
        if active_id == snapshot_id:
            existing_meta = redis_client.hgetall(f"{prefix}:{snapshot_id}:meta")
            if not existing_meta:
                raise ValueError("active Redis snapshot is missing metadata")
            decoded_meta = {
                (key.decode() if isinstance(key, bytes) else str(key)): (
                    value.decode() if isinstance(value, bytes) else str(value)
                )
                for key, value in existing_meta.items()
            }
            _validate_namespace(
                redis_client,
                prefix=prefix,
                snapshot_id=snapshot_id,
                expected_clients=int(decoded_meta["client_count"]),
                expected_top_n=int(decoded_meta["published_top_n"]),
            )
            _refresh_namespace_ttl(
                redis_client,
                f"{prefix}:{snapshot_id}:*",
                config.snapshot_ttl_seconds,
                config.pipeline_batch_size,
            )
            existing_fallback = RECOMMENDATION_LIST.validate_json(
                redis_client.get(f"{prefix}:{snapshot_id}:fallback")
            )
            publish_postgres_fallback(database_url, snapshot_id, existing_fallback)
            return {"snapshot_id": snapshot_id, "status": "already_active"}

        namespace_pattern = f"{prefix}:{snapshot_id}:*"
        _delete_namespace(redis_client, namespace_pattern, config.pipeline_batch_size)

        frame = spark.read.parquet(ranking_data_uri).cache()
        actual_run_ids = {
            row["ranking_run_id"]
            for row in frame.select("ranking_run_id").distinct().collect()
        }
        if actual_run_ids and actual_run_ids != {snapshot_id}:
            raise ValueError(
                f"ranking data run_id mismatch: expected {snapshot_id}, found {actual_run_ids}"
            )
        item_runs = (
            metadata.get("source_simulation_run_id"),
            metadata.get("source_optimizer_run_id"),
        )
        if any(not value for value in item_runs):
            raise ValueError("ranking metadata is missing source lineage")
        simulation_metadata = _read_hdfs_json(
            spark,
            f"{gold_metadata_uri(base, 'simulation_candidates', snapshot_date)}/_metadata.json",
        )
        if simulation_metadata.get("run_id") != metadata["source_simulation_run_id"]:
            raise ValueError("ranking references a stale simulation snapshot")
        source_gold_runs = simulation_metadata.get("source_gold_run_ids") or {}
        expected_item_runs = source_gold_runs.get("item_features")
        items_uri = gold_data_uri(base, "item_features", snapshot_date)
        require_hdfs_replication(spark, items_uri, config.required_hdfs_replication)
        items = spark.read.parquet(items_uri)
        actual_item_runs = sorted(
            str(row["feature_run_id"])
            for row in items.select("feature_run_id").distinct().collect()
        )
        if actual_item_runs != expected_item_runs:
            raise ValueError(
                f"fallback item lineage mismatch: expected {expected_item_runs}, "
                f"found {actual_item_runs}"
            )
        fallback = _fallback_recommendations(items, config.fallback_top_n)
        if not fallback:
            raise ValueError("cannot publish Redis snapshot without fallback items")
        source_top_n = int(metadata["ranking_policy"]["top_n"])
        estimated_sizes, client_count = _estimate_sizes(
            frame,
            config,
            key_prefix=prefix,
            snapshot_id=snapshot_id,
            fallback=fallback,
            source_top_n=source_top_n,
        )
        memory = redis_client.info("memory")
        maxmemory = int(memory.get("maxmemory") or 0)
        if maxmemory <= 0:
            raise ValueError("Redis maxmemory must be configured")
        available_bytes = max(
            0,
            maxmemory - int(memory.get("used_memory") or 0) - config.memory_reserve_bytes,
        )
        published_top_n = choose_published_top_n(
            estimated_sizes,
            max_snapshot_bytes=config.max_snapshot_bytes,
            available_bytes=available_bytes,
            min_top_n=config.min_top_n,
        )
        fallback_payload = compact_payload(fallback[:published_top_n])
        previous_db_snapshot = active_postgres_fallback(database_url)
        database_activated = False
        try:
            pipeline = redis_client.pipeline(transaction=False)
            pending = 0
            for client_id, recommendations in iter_client_recommendations(frame):
                pipeline.set(
                    f"{prefix}:{snapshot_id}:user:{client_id}",
                    compact_payload(recommendations[:published_top_n]),
                    ex=config.snapshot_ttl_seconds,
                )
                pending += 1
                if pending >= config.pipeline_batch_size:
                    pipeline.execute()
                    pipeline = redis_client.pipeline(transaction=False)
                    pending = 0
            if pending:
                pipeline.execute()
            meta_key = f"{prefix}:{snapshot_id}:meta"
            redis_client.hset(
                meta_key,
                mapping={
                    "cutoff": metadata["feature_cutoff"],
                    "created_at": metadata["created_at"],
                    "ranking_run_id": snapshot_id,
                    "ranking_policy_version": metadata["ranking_policy"]["version"],
                    "source_top_n": metadata["ranking_policy"]["top_n"],
                    "published_top_n": published_top_n,
                    "client_count": client_count,
                    "estimated_bytes": estimated_sizes[published_top_n],
                    "online_config_version": config.version,
                },
            )
            redis_client.expire(meta_key, config.snapshot_ttl_seconds)
            fallback_key = f"{prefix}:{snapshot_id}:fallback"
            redis_client.set(
                fallback_key, fallback_payload, ex=config.snapshot_ttl_seconds
            )
            _validate_namespace(
                redis_client,
                prefix=prefix,
                snapshot_id=snapshot_id,
                expected_clients=client_count,
                expected_top_n=published_top_n,
            )
            publish_postgres_fallback(
                database_url, snapshot_id, fallback[:published_top_n]
            )
            database_activated = True
            redis_client.set(active_key, snapshot_id)
        except Exception:
            if database_activated:
                restore_postgres_fallback(database_url, previous_db_snapshot)
            current = redis_client.get(active_key)
            current_id = current.decode() if isinstance(current, bytes) else current
            if current_id != snapshot_id:
                _delete_namespace(
                    redis_client, namespace_pattern, config.pipeline_batch_size
                )
            raise
        return {
            "snapshot_id": snapshot_id,
            "status": "published",
            "client_count": client_count,
            "source_top_n": source_top_n,
            "published_top_n": published_top_n,
            "estimated_bytes": estimated_sizes[published_top_n],
            "checked_hdfs_files": checked_files,
        }
    finally:
        redis_client.close()
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--snapshot-date", required=True, type=parse_iso_date)
    parser.add_argument("--redis-url", default="redis://redis:6379/0")
    parser.add_argument("--online-config", default="/workspace/configs/online_store.yaml")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.database_url:
        raise ValueError("--database-url or DATABASE_URL is required")
    result = publish_redis_snapshot(
        hdfs_base_uri=args.hdfs_base_uri,
        snapshot_date=args.snapshot_date,
        redis_url=args.redis_url,
        online_config=args.online_config,
        database_url=args.database_url,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
