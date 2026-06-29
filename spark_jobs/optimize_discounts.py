"""Select discounts under local constraints, user caps, and a global expected-cost budget."""

from __future__ import annotations

import argparse
import json
import math
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Protocol

from spark_jobs.bronze_common import (
    filesystem,
    hadoop_path,
    normalize_hdfs_base_uri,
    replace_hdfs_paths,
)
from spark_jobs.build_user_features import parse_feature_cutoff
from spark_jobs.gold_common import gold_data_uri, gold_metadata_uri
from spark_jobs.optimizer_config import OptimizerConfig, load_optimizer_config
from spark_jobs.silver_common import stage_parquet, write_json_metadata
from spark_jobs.time_compat import UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class AllocationRow(Protocol):
    client_id: str
    product_id: str
    expected_discount_cost: float


@dataclass(frozen=True)
class AllocationDecision:
    client_id: str
    product_id: str
    optimizer_decision: str
    allocation_rank: int
    budget_spent_after: float


def category_cap(level_2: str | None, config: OptimizerConfig) -> float:
    if level_2 is not None and level_2 in config.category_max_discount.overrides:
        return config.category_max_discount.overrides[level_2]
    return config.category_max_discount.default


def is_locally_eligible(
    *,
    discount: float,
    is_discount_eligible: bool,
    margin_rate: float,
    maximum_discount: float,
    promo_abuse_score: float,
    incremental_profit: float,
    roi: float,
    config: OptimizerConfig,
) -> bool:
    return (
        discount > 0
        and is_discount_eligible
        and margin_rate - discount > config.min_margin_rate
        and discount <= maximum_discount
        and promo_abuse_score <= config.promo_abuse_threshold
        and incremental_profit > 0
        and roi >= config.min_promo_roi
    )


def choose_tolerant_discount(
    options: Iterable[tuple[float, float]], tolerance: float
) -> float | None:
    values = list(options)
    if not values:
        return None
    best_profit = max(profit for _, profit in values)
    return min(
        discount for discount, profit in values if profit >= best_profit - tolerance
    )


def greedy_allocate(
    rows: Iterable[AllocationRow], *, global_budget: float, user_cap: int
) -> Iterator[AllocationDecision]:
    if global_budget <= 0:
        raise ValueError("global budget must be positive")
    if user_cap <= 0:
        raise ValueError("user cap must be positive")
    spent = 0.0
    accepted_by_user: dict[str, int] = {}
    for rank, row in enumerate(rows, start=1):
        cost = float(row.expected_discount_cost)
        if not math.isfinite(cost) or cost < 0:
            raise ValueError("expected discount cost must be finite and non-negative")
        accepted_count = accepted_by_user.get(row.client_id, 0)
        if accepted_count >= user_cap:
            decision = "USER_CAP_REJECTED"
        elif spent + cost <= global_budget + 1e-9:
            decision = "DISCOUNT_ACCEPTED"
            spent += cost
            accepted_by_user[row.client_id] = accepted_count + 1
        else:
            decision = "BUDGET_REJECTED"
        yield AllocationDecision(
            client_id=row.client_id,
            product_id=row.product_id,
            optimizer_decision=decision,
            allocation_rank=rank,
            budget_spent_after=spent,
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


def _require_simulation_contract(
    metadata: dict[str, Any], *, snapshot: date, feature_cutoff: datetime
) -> tuple[str, str]:
    expected = {
        "snapshot_date": snapshot.isoformat(),
        "feature_cutoff": feature_cutoff.isoformat(),
    }
    mismatched = {
        name: {"expected": value, "actual": metadata.get(name)}
        for name, value in expected.items()
        if metadata.get(name) != value
    }
    if mismatched:
        raise ValueError(f"simulation metadata mismatch: {mismatched}")
    run_id = metadata.get("run_id")
    simulation = metadata.get("simulation_config")
    version = simulation.get("version") if isinstance(simulation, dict) else None
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("simulation metadata is missing run_id")
    if not isinstance(version, str) or not version:
        raise ValueError("simulation metadata is missing simulation version")
    return run_id, version


def _category_cap_expression(config: OptimizerConfig) -> Any:
    from pyspark.sql import functions as functions

    entries: list[Any] = []
    for category, maximum in sorted(config.category_max_discount.overrides.items()):
        entries.extend((functions.lit(category), functions.lit(maximum)))
    if not entries:
        return functions.lit(config.category_max_discount.default)
    return functions.coalesce(
        functions.element_at(functions.create_map(*entries), functions.col("level_2")),
        functions.lit(config.category_max_discount.default),
    )


def _select_local_discounts(scenarios: DataFrame, config: OptimizerConfig) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    pair_window = Window.partitionBy("client_id", "product_id")
    eligible = (
        (functions.col("discount") > 0)
        & functions.col("is_discount_eligible")
        & (
            functions.col("margin_rate") - functions.col("discount")
            > functions.lit(config.min_margin_rate)
        )
        & (functions.col("discount") <= functions.col("category_max_discount"))
        & (functions.col("promo_abuse_score") <= functions.lit(config.promo_abuse_threshold))
        & (functions.col("incremental_profit") > 0)
        & (functions.col("roi") >= functions.lit(config.min_promo_roi))
    )
    annotated = (
        scenarios.withColumn("category_max_discount", _category_cap_expression(config))
        .withColumn("_is_locally_eligible", eligible)
        .withColumn(
            "_best_incremental_profit",
            functions.max(
                functions.when(
                    functions.col("_is_locally_eligible"), functions.col("incremental_profit")
                )
            ).over(pair_window),
        )
    )
    near_best = annotated.where(
        functions.col("_is_locally_eligible")
        & (
            functions.col("incremental_profit")
            >= functions.col("_best_incremental_profit")
            - functions.lit(config.incremental_profit_tolerance)
        )
    )
    tie_window = Window.partitionBy("client_id", "product_id").orderBy(
        functions.asc("discount")
    )
    return (
        near_best.withColumn("_local_rank", functions.row_number().over(tie_window))
        .where(functions.col("_local_rank") == 1)
        .drop("_is_locally_eligible", "_best_incremental_profit", "_local_rank")
    )


def _allocation_frame(local_discounts: DataFrame, config: OptimizerConfig) -> DataFrame:
    from pyspark.sql import Row
    from pyspark.sql import functions as functions
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    ordered = local_discounts.select(
        "client_id",
        "product_id",
        "expected_discount_cost",
        "roi",
        "incremental_profit",
    ).repartition(1).sortWithinPartitions(
        functions.desc("roi"),
        functions.desc("incremental_profit"),
        functions.asc("expected_discount_cost"),
        functions.asc("client_id"),
        functions.asc("product_id"),
    )

    def allocate_partition(rows: Iterator[AllocationRow]) -> Iterator[Any]:
        for decision in greedy_allocate(
            rows,
            global_budget=config.global_budget,
            user_cap=config.max_discounted_items_per_user,
        ):
            yield Row(**decision.__dict__)

    schema = StructType(
        [
            StructField("client_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("optimizer_decision", StringType(), False),
            StructField("allocation_rank", LongType(), False),
            StructField("budget_spent_after", DoubleType(), False),
        ]
    )
    return local_discounts.sparkSession.createDataFrame(
        ordered.rdd.mapPartitions(allocate_partition), schema
    )


def _finalize_offers(
    scenarios: DataFrame,
    local_discounts: DataFrame,
    allocations: DataFrame,
    *,
    run_id: str,
    created_at: datetime,
    policy_version: str,
    config: OptimizerConfig,
) -> DataFrame:
    from pyspark.sql import functions as functions

    allocation_columns = [
        "client_id",
        "product_id",
        "optimizer_decision",
        "allocation_rank",
        "budget_spent_after",
    ]
    accepted = local_discounts.join(
        allocations.select(*allocation_columns), ["client_id", "product_id"]
    ).where(functions.col("optimizer_decision") == "DISCOUNT_ACCEPTED")
    baselines = scenarios.where(functions.col("discount") == 0).withColumn(
        "category_max_discount", _category_cap_expression(config)
    ).join(
        allocations.select(*allocation_columns), ["client_id", "product_id"], "left"
    )
    baselines = baselines.where(
        functions.col("optimizer_decision").isNull()
        | (functions.col("optimizer_decision") != "DISCOUNT_ACCEPTED")
    ).withColumn(
        "optimizer_decision",
        functions.coalesce(
            "optimizer_decision",
            functions.when(
                ~functions.col("is_discount_eligible"), "MISSING_PRICE"
            ).otherwise("LOCAL_BASELINE"),
        ),
    )
    output = accepted.unionByName(baselines, allowMissingColumns=True)
    return output.withColumns(
        {
            "optimizer_run_id": functions.lit(run_id),
            "optimizer_policy_version": functions.lit(policy_version),
            "optimized_at": functions.lit(created_at).cast("timestamp"),
        }
    )


def optimize_discounts(
    *,
    hdfs_base_uri: str,
    feature_cutoff: datetime,
    optimizer_config: str = "/workspace/configs/optimizer.yaml",
) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as functions

    config = load_optimizer_config(optimizer_config)
    base = normalize_hdfs_base_uri(hdfs_base_uri)
    snapshot = feature_cutoff.date()
    run_id = uuid.uuid4().hex
    created_at = datetime.now(UTC).replace(tzinfo=None)
    staging = f"{base}/tmp/discount-optimizer/{run_id}"
    backup = f"{base}/tmp/discount-optimizer-backup/{run_id}"
    spark = (
        SparkSession.builder.appName(f"optimize-discounts-{run_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    hdfs = filesystem(spark, staging)
    try:
        simulation_uri = gold_data_uri(base, "simulation_candidates", snapshot)
        metadata_uri = (
            f"{gold_metadata_uri(base, 'simulation_candidates', snapshot)}/_metadata.json"
        )
        metadata = _read_hdfs_json(spark, metadata_uri)
        simulation_run_id, simulation_version = _require_simulation_contract(
            metadata, snapshot=snapshot, feature_cutoff=feature_cutoff
        )
        scenarios = spark.read.parquet(simulation_uri).cache()
        actual_run_ids = {
            row["simulation_run_id"]
            for row in scenarios.select("simulation_run_id").distinct().collect()
        }
        if actual_run_ids != {simulation_run_id}:
            raise ValueError(
                "simulation data run_id mismatch: "
                f"expected {simulation_run_id}, found {actual_run_ids}"
            )
        actual_versions = {
            row["simulation_version"]
            for row in scenarios.select("simulation_version").distinct().collect()
        }
        if actual_versions != {simulation_version}:
            raise ValueError(
                "simulation data version mismatch: "
                f"expected {simulation_version}, found {actual_versions}"
            )
        actual_cutoffs = {
            row["feature_cutoff"]
            for row in scenarios.select("feature_cutoff").distinct().collect()
        }
        if actual_cutoffs != {feature_cutoff}:
            raise ValueError(
                f"simulation feature_cutoff mismatch: expected {feature_cutoff}, "
                f"found {actual_cutoffs}"
            )
        if scenarios.groupBy("client_id", "product_id", "discount").count().where(
            "count != 1"
        ).count():
            raise ValueError("simulation contains duplicate scenario keys")
        candidate_count = scenarios.select("client_id", "product_id").distinct().count()
        baseline_count = scenarios.where("discount = 0").count()
        if baseline_count != candidate_count:
            raise ValueError("simulation must contain exactly one baseline per candidate pair")

        local_discounts = _select_local_discounts(scenarios, config).cache()
        allocations = _allocation_frame(local_discounts, config).cache()
        offers = _finalize_offers(
            scenarios,
            local_discounts,
            allocations,
            run_id=run_id,
            created_at=created_at,
            policy_version=config.version,
            config=config,
        ).cache()
        output_count = offers.count()
        if output_count != candidate_count:
            raise ValueError(
                f"optimizer coverage mismatch: expected {candidate_count}, produced {output_count}"
            )
        if offers.groupBy("client_id", "product_id").count().where("count != 1").count():
            raise ValueError("optimized offers contain duplicate candidate keys")
        discounted = offers.where("discount > 0").cache()
        oversubscribed_users = discounted.groupBy("client_id").count().where(
            functions.col("count") > config.max_discounted_items_per_user
        ).count()
        if oversubscribed_users:
            raise ValueError("optimized offers violate user discount cap")
        expected_spend = discounted.agg(
            functions.coalesce(functions.sum("expected_discount_cost"), functions.lit(0.0))
        ).first()[0]
        if expected_spend > config.global_budget + 1e-9:
            raise ValueError("optimized offers exceed global budget")

        decision_counts = {
            row["optimizer_decision"]: int(row["count"])
            for row in offers.groupBy("optimizer_decision").count().collect()
        }
        staged_data = f"{staging}/data"
        staged_metadata = f"{staging}/metadata"
        stage_parquet(offers, staged_data)
        result = {
            "job": "optimize_discounts",
            "run_id": run_id,
            "created_at": created_at.isoformat(),
            "snapshot_date": snapshot.isoformat(),
            "feature_cutoff": feature_cutoff.isoformat(),
            "source_simulation_run_id": simulation_run_id,
            "source_simulation_version": simulation_version,
            "optimizer_policy": config.model_dump(mode="json"),
            "metrics": {
                "candidate_pairs": candidate_count,
                "locally_discounted_pairs": local_discounts.count(),
                "accepted_discount_pairs": discounted.count(),
                "expected_discount_spend": expected_spend,
                "budget_utilization": expected_spend / config.global_budget,
                "decision_counts": decision_counts,
            },
        }
        write_json_metadata(spark, staged_metadata, result)
        replace_hdfs_paths(
            spark,
            [
                (staged_data, gold_data_uri(base, "optimized_offers", snapshot)),
                (staged_metadata, gold_metadata_uri(base, "optimized_offers", snapshot)),
            ],
            backup,
        )
        return result
    finally:
        hdfs.delete(hadoop_path(spark, staging), True)
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-base-uri", default="hdfs://namenode:9000/promo")
    parser.add_argument("--feature-cutoff", required=True, type=parse_feature_cutoff)
    parser.add_argument("--optimizer-config", default="/workspace/configs/optimizer.yaml")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = optimize_discounts(
        hdfs_base_uri=args.hdfs_base_uri,
        feature_cutoff=args.feature_cutoff,
        optimizer_config=args.optimizer_config,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
