"""
delta_streaming.py
------------------
Utilities for managing the Delta Lake tables written by the streaming job.

Covers:
  - Schema creation (run once before starting the pipeline)
  - Table maintenance (OPTIMIZE, VACUUM)
  - Time-travel queries (audit log, point-in-time reads)
  - Batch reads for downstream consumers (dashboards, ML feature store)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DELTA_BASE      = os.environ.get("DELTA_BASE_PATH", "/tmp/delta_lake")
DELTA_RELEASES  = f"{DELTA_BASE}/streaming/releases_enriched"
DELTA_ANOMALIES = f"{DELTA_BASE}/streaming/anomalies"


def build_spark(app_name: str = "EconDeltaAdmin") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ── Table management ───────────────────────────────────────────────────────────
def optimize_table(spark: SparkSession, path: str, z_order_cols: list[str] | None = None) -> None:
    """
    OPTIMIZE compacts small files (common in streaming — lots of small micro-batches).
    Z-ORDER sorts data so queries filtering on those columns skip most files.
    """
    dt = DeltaTable.forPath(spark, path)
    if z_order_cols:
        dt.optimize().executeZOrderBy(*z_order_cols)
        logger.info(f"OPTIMIZE + Z-ORDER({z_order_cols}) completed: {path}")
    else:
        dt.optimize().executeCompaction()
        logger.info(f"OPTIMIZE (compaction only) completed: {path}")


def vacuum_table(spark: SparkSession, path: str, retain_hours: int = 168) -> None:
    """
    VACUUM removes old data files no longer referenced by the transaction log.
    Default: retain 7 days (168 hours) of history for time travel.
    """
    dt = DeltaTable.forPath(spark, path)
    dt.vacuum(retentionHours=retain_hours)
    logger.info(f"VACUUM completed (retained {retain_hours}h): {path}")


def show_history(spark: SparkSession, path: str, n: int = 10) -> None:
    """Show the last n operations in the Delta transaction log."""
    dt = DeltaTable.forPath(spark, path)
    history = dt.history(n)
    history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)


# ── Time-travel queries ────────────────────────────────────────────────────────
def read_at_version(spark: SparkSession, path: str, version: int):
    """Read the Delta table as it existed at a specific version."""
    return spark.read.format("delta").option("versionAsOf", version).load(path)


def read_at_timestamp(spark: SparkSession, path: str, ts: str):
    """Read the Delta table as it existed at a specific timestamp. ts: '2024-04-10 08:30:00'"""
    return spark.read.format("delta").option("timestampAsOf", ts).load(path)


# ── Analytics queries ──────────────────────────────────────────────────────────
def latest_releases(spark: SparkSession, n: int = 20):
    """Most recent economic releases processed by the streaming pipeline."""
    df = spark.read.format("delta").load(DELTA_RELEASES)
    return (
        df.orderBy(F.col("processed_ts").desc())
          .select("release_name", "indicator", "period", "value",
                  "surprise_pct", "market_impact", "value_direction",
                  "is_anomaly", "processed_ts")
          .limit(n)
    )


def high_impact_events(spark: SparkSession, since_days: int = 90):
    """Filter to HIGH impact events in the last N days."""
    since = (datetime.now() - timedelta(days=since_days)).strftime("%Y-%m-%d")
    df = spark.read.format("delta").load(DELTA_RELEASES)
    return (
        df.filter(
            (F.col("market_impact") == "HIGH") &
            (F.col("processing_date") >= since)
        )
        .orderBy(F.col("processed_ts").desc())
    )


def surprise_analysis(spark: SparkSession):
    """
    Summary of surprise magnitude by release type.
    Useful for understanding which releases are most volatile / unpredictable.
    """
    df = spark.read.format("delta").load(DELTA_RELEASES)
    return (
        df.filter(F.col("surprise_pct").isNotNull())
          .groupBy("release_name", "source")
          .agg(
              F.count("*").alias("n_releases"),
              F.round(F.mean(F.abs(F.col("surprise_pct"))), 2).alias("mean_abs_surprise_pct"),
              F.round(F.max(F.abs(F.col("surprise_pct"))), 2).alias("max_abs_surprise_pct"),
              F.round(F.stddev(F.col("surprise_pct")), 2).alias("surprise_std"),
              F.sum(F.when(F.col("surprise_pct") > 0, 1).otherwise(0)).alias("n_beats"),
              F.sum(F.when(F.col("surprise_pct") < 0, 1).otherwise(0)).alias("n_misses"),
          )
          .orderBy("mean_abs_surprise_pct", ascending=False)
    )


def fomc_decision_history(spark: SparkSession):
    """All FOMC rate decisions with direction and surprise context."""
    df = spark.read.format("delta").load(DELTA_RELEASES)
    return (
        df.filter(F.col("release_name") == "FOMC_RATE_DECISION")
          .select("period", "value", "prior_value", "value_direction",
                  "surprise_pct", "is_anomaly", "processed_ts")
          .orderBy("period")
    )


def anomaly_summary(spark: SparkSession):
    """All flagged anomalies with reasons."""
    df = spark.read.format("delta").load(DELTA_RELEASES)
    return (
        df.filter(F.col("is_anomaly") == True)
          .select("release_name", "indicator", "period", "value",
                  "surprise_pct", "anomaly_reason", "market_impact", "processed_ts")
          .orderBy(F.col("processed_ts").desc())
    )


# ── CLI for maintenance tasks ──────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Delta Lake admin for streaming tables")
    parser.add_argument("--action", choices=[
        "optimize", "vacuum", "history", "latest", "anomalies", "surprise"
    ], default="latest")
    parser.add_argument("--path", default=DELTA_RELEASES)
    parser.add_argument("--n", type=int, default=20)
    parser.add_argument("--retain-hours", type=int, default=168)
    args = parser.parse_args()

    spark = build_spark()

    if args.action == "optimize":
        optimize_table(spark, args.path, z_order_cols=["release_name", "source"])
    elif args.action == "vacuum":
        vacuum_table(spark, args.path, args.retain_hours)
    elif args.action == "history":
        show_history(spark, args.path, args.n)
    elif args.action == "latest":
        latest_releases(spark, args.n).show(truncate=False)
    elif args.action == "anomalies":
        anomaly_summary(spark).show(truncate=False)
    elif args.action == "surprise":
        surprise_analysis(spark).show(truncate=False)
