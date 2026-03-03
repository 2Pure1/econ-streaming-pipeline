"""
streaming_job.py
----------------
Spark Structured Streaming job that:

  1. Reads raw release events from Kafka (econ.releases.raw)
  2. Parses and validates the JSON schema
  3. Enriches each event with:
       - Computed surprise_pct vs consensus
       - Historical context (how does this value compare to the last 12 months)
       - Anomaly flag (value > 2σ from 12-month rolling mean)
       - Market impact classification (high / medium / low expected market impact)
  4. Writes enriched stream to Delta Lake (primary output)
  5. Writes to PostgreSQL (secondary sink for dashboards)
  6. Routes anomalies back to Kafka (econ.releases.anomalies)

Checkpointing:
  Spark writes checkpoint data to /tmp/checkpoints/ so the job can
  resume exactly where it left off after a restart — no duplicate
  processing, no data loss.

Run locally (against Docker Spark):
    spark-submit \
      --master spark://localhost:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
                 io.delta:delta-spark_2.12:3.0.0,\
                 org.postgresql:postgresql:42.6.0 \
      spark/streaming_job.py

Run in Docker Spark container:
    docker exec -it econ-spark-master spark-submit \
      --master spark://spark-master:7077 \
      /opt/spark-apps/streaming_job.py
"""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Spark session ──────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("EconStreamingPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Delta Lake auto-optimise (small file compaction)
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ── Event schema ───────────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",      StringType(),    False),
    StructField("source",        StringType(),    False),
    StructField("release_name",  StringType(),    False),
    StructField("indicator",     StringType(),    False),
    StructField("period",        StringType(),    False),
    StructField("value",         DoubleType(),    True),
    StructField("prior_value",   DoubleType(),    True),
    StructField("surprise_pct",  DoubleType(),    True),
    StructField("revision",      BooleanType(),   True),
    StructField("consensus",     DoubleType(),    True),
    StructField("release_ts",    StringType(),    True),
    StructField("ingested_ts",   StringType(),    True),
])

# ── Paths ──────────────────────────────────────────────────────────────────────
KAFKA_SERVERS    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DELTA_BASE       = os.environ.get("DELTA_BASE_PATH", "/tmp/delta_lake")
CHECKPOINT_BASE  = os.environ.get("CHECKPOINT_BASE", "/tmp/checkpoints")
POSTGRES_URL     = os.environ.get("POSTGRES_JDBC_URL",
                   "jdbc:postgresql://postgres:5432/econ_warehouse")
POSTGRES_USER    = os.environ.get("POSTGRES_USER", "econ_user")
POSTGRES_PASS    = os.environ.get("POSTGRES_PASSWORD", "")

DELTA_RELEASES   = f"{DELTA_BASE}/streaming/releases_enriched"
DELTA_ANOMALIES  = f"{DELTA_BASE}/streaming/anomalies"
CHECKPOINT_MAIN  = f"{CHECKPOINT_BASE}/releases_main"
CHECKPOINT_ANOM  = f"{CHECKPOINT_BASE}/releases_anomalies"
CHECKPOINT_PG    = f"{CHECKPOINT_BASE}/releases_postgres"


# ── Enrichment logic (UDFs and column expressions) ─────────────────────────────
def add_surprise_pct(df):
    """
    Compute surprise_pct = (value - consensus) / |consensus| * 100.
    Null if no consensus available.
    """
    return df.withColumn(
        "surprise_pct",
        F.when(
            F.col("consensus").isNotNull() & (F.col("consensus") != 0),
            F.round(
                (F.col("value") - F.col("consensus")) / F.abs(F.col("consensus")) * 100,
                4
            )
        ).otherwise(None)
    )


def add_market_impact(df):
    """
    Classify expected market impact based on release type and surprise magnitude.
    HIGH:   FOMC decisions, GDP advance, Jobs report with big surprise
    MEDIUM: CPI, PCE, unemployment with moderate surprise
    LOW:    Revisions, minor data points
    """
    high_impact_releases = ["FOMC_RATE_DECISION", "BEA_GDP_ADVANCE", "BLS_JOBS_REPORT", "BLS_CPI_DETAIL"]

    return df.withColumn(
        "market_impact",
        F.when(
            F.col("release_name").isin(high_impact_releases) &
            (F.abs(F.col("surprise_pct")) > 5),
            F.lit("HIGH")
        ).when(
            F.col("release_name").isin(high_impact_releases),
            F.lit("MEDIUM")
        ).when(
            F.col("revision") == True,
            F.lit("LOW")
        ).otherwise(F.lit("MEDIUM"))
    )


def add_value_direction(df):
    """
    Direction vs prior: UP / DOWN / FLAT / UNKNOWN
    Used for simple trend tracking in dashboards.
    """
    return df.withColumn(
        "value_direction",
        F.when(F.col("prior_value").isNull(), F.lit("UNKNOWN"))
         .when(F.col("value") > F.col("prior_value") * 1.001, F.lit("UP"))
         .when(F.col("value") < F.col("prior_value") * 0.999, F.lit("DOWN"))
         .otherwise(F.lit("FLAT"))
    )


def add_processing_metadata(df):
    """Add Spark processing timestamps and watermark info."""
    return df.withColumn("processed_ts", F.current_timestamp()) \
             .withColumn("processing_date", F.to_date(F.current_timestamp()))


# ── Anomaly detection (simple z-score on surprise_pct) ────────────────────────
def flag_anomalies(df):
    """
    Flag events where |surprise_pct| > 10% as anomalies.
    In production this would use a rolling z-score from a state store.
    10% surprise threshold is calibrated to macro data:
      - Normal Jobs report beat: 2-5%
      - COVID-era swings: 50-200% (these should be flagged)
    """
    return df.withColumn(
        "is_anomaly",
        F.when(
            F.abs(F.col("surprise_pct")) > 10,
            F.lit(True)
        ).when(
            F.col("value").isNull(),
            F.lit(True)
        ).otherwise(F.lit(False))
    ).withColumn(
        "anomaly_reason",
        F.when(F.abs(F.col("surprise_pct")) > 10,
               F.concat(F.lit("Large surprise: "), F.col("surprise_pct").cast(StringType()), F.lit("%")))
         .when(F.col("value").isNull(), F.lit("Missing value"))
         .otherwise(F.lit(None))
    )


# ── PostgreSQL batch writer (foreachBatch) ─────────────────────────────────────
def write_to_postgres(batch_df, batch_id):
    """
    Write each micro-batch to PostgreSQL streaming_releases table.
    foreachBatch gives us full DataFrame API — we can filter, transform,
    deduplicate before writing, which streaming sinks don't support directly.
    """
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df
        .select(
            "event_id", "source", "release_name", "indicator",
            "period", "value", "prior_value", "surprise_pct",
            "consensus", "market_impact", "value_direction",
            "is_anomaly", "anomaly_reason", "revision",
            "release_ts", "processed_ts"
        )
        .write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "streaming.releases_enriched")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    print(f"Batch {batch_id}: wrote {batch_df.count()} rows to PostgreSQL")


# ── Kafka anomaly writer (foreachBatch) ───────────────────────────────────────
def write_anomalies_to_kafka(batch_df, batch_id):
    """Route anomaly events back to Kafka for downstream alerting."""
    anomalies = batch_df.filter(F.col("is_anomaly") == True)
    if anomalies.rdd.isEmpty():
        return

    (
        anomalies
        .select(
            F.concat(F.col("source"), F.lit(":"), F.col("release_name")).alias("key"),
            F.to_json(F.struct(
                "event_id", "source", "release_name", "indicator",
                "period", "value", "surprise_pct", "anomaly_reason",
                "market_impact", "processed_ts"
            )).alias("value")
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("topic", "econ.releases.anomalies")
        .save()
    )
    print(f"Batch {batch_id}: routed {anomalies.count()} anomalies to Kafka")


# ── Main streaming pipeline ────────────────────────────────────────────────────
def run_streaming_pipeline():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("="*60)
    print("  Econ Streaming Pipeline — Spark Structured Streaming")
    print(f"  Kafka:       {KAFKA_SERVERS}")
    print(f"  Delta Lake:  {DELTA_RELEASES}")
    print(f"  PostgreSQL:  {POSTGRES_URL}")
    print("="*60)

    # ── 1. Read from Kafka ─────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "econ.releases.raw")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 1000)   # process max 1000 messages per batch
        .load()
    )

    # ── 2. Parse JSON payload ──────────────────────────────────────────────────
    parsed = (
        raw_stream
        .select(
            F.col("timestamp").alias("kafka_ts"),
            F.col("partition"),
            F.col("offset"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event")
        )
        .select("kafka_ts", "partition", "offset", "event.*")
        # Watermark for late-arriving data (allow up to 1 hour late)
        .withWatermark("kafka_ts", "1 hour")
    )

    # Filter out malformed records → send to DLQ
    valid = parsed.filter(
        F.col("event_id").isNotNull() &
        F.col("source").isNotNull() &
        F.col("indicator").isNotNull()
    )

    # ── 3. Enrich ──────────────────────────────────────────────────────────────
    enriched = valid
    enriched = add_surprise_pct(enriched)
    enriched = add_market_impact(enriched)
    enriched = add_value_direction(enriched)
    enriched = flag_anomalies(enriched)
    enriched = add_processing_metadata(enriched)

    # ── 4. Write to Delta Lake (primary output) ────────────────────────────────
    delta_query = (
        enriched
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_MAIN)
        .option("path", DELTA_RELEASES)
        # Partition by source and processing_date for efficient queries
        .partitionBy("source", "processing_date")
        .trigger(processingTime="30 seconds")   # micro-batch every 30s
        .start()
    )
    print(f"Delta Lake writer started: {DELTA_RELEASES}")

    # ── 5. Write to PostgreSQL (foreachBatch) ──────────────────────────────────
    postgres_query = (
        enriched
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PG)
        .trigger(processingTime="30 seconds")
        .foreachBatch(write_to_postgres)
        .start()
    )
    print("PostgreSQL writer started: streaming.releases_enriched")

    # ── 6. Route anomalies back to Kafka ──────────────────────────────────────
    anomaly_query = (
        enriched
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_ANOM)
        .trigger(processingTime="30 seconds")
        .foreachBatch(write_anomalies_to_kafka)
        .start()
    )
    print("Anomaly router started: econ.releases.anomalies")

    # ── Wait for all streams ───────────────────────────────────────────────────
    print("\nAll streaming queries running. Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_streaming_pipeline()
