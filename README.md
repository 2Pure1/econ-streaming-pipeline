# ⚡ Econ Streaming Pipeline

> **Real-time economic data release processing** — BEA, FRED, and BLS release events flow through Kafka into Spark Structured Streaming, enriched with surprise scores and anomaly flags, written to Delta Lake and PostgreSQL.

![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-231F20?logo=apache-kafka)
![Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?logo=apache-spark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-00ADD8)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8-017CEE?logo=apache-airflow)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                           │
│                                                             │
│   BEA API           FRED API           BLS API             │
│  GDP · PCE        Fed Funds · CPI    Jobs · CPI            │
│  (quarterly)      (monthly/weekly)   (monthly)             │
└────────────┬───────────────┬──────────────┬────────────────┘
             └───────────────┼──────────────┘
                             │
             ┌───────────────▼──────────────────┐
             │       release_producer.py         │
             │                                  │
             │  --mode replay   (2024 calendar) │
             │  --mode live     (FRED polling)  │
             │  --mode test     (single event)  │
             └───────────────┬──────────────────┘
                             │  JSON events
                             ▼
             ┌───────────────────────────────────┐
             │         Apache Kafka               │
             │                                   │
             │  econ.releases.raw       (3 part) │
             │  econ.releases.enriched  (3 part) │
             │  econ.releases.anomalies (1 part) │
             │  econ.releases.dlq       (1 part) │
             └───────────────┬───────────────────┘
                             │
             ┌───────────────▼──────────────────────────────┐
             │        Spark Structured Streaming             │
             │        (streaming_job.py)                    │
             │                                              │
             │  Parse JSON → Validate schema                │
             │  Add surprise_pct = (actual-consensus)/|con| │
             │  Add market_impact: HIGH/MEDIUM/LOW          │
             │  Add value_direction: UP/DOWN/FLAT           │
             │  Flag anomalies: |surprise| > 10%            │
             │  Watermark: 1 hour for late data             │
             │  Micro-batch: every 30 seconds               │
             └──────────┬─────────────────┬─────────────────┘
                        │                 │
           ┌────────────▼────────┐  ┌────▼─────────────────┐
           │     Delta Lake      │  │     PostgreSQL        │
           │                     │  │                       │
           │  /streaming/        │  │  streaming.           │
           │  releases_enriched  │  │  releases_enriched    │
           │                     │  │                       │
           │  Partitioned by:    │  │  Views:               │
           │  source + date      │  │  latest_by_indicator  │
           │                     │  │  recent_high_impact   │
           │  OPTIMIZE + VACUUM  │  │  fomc_history         │
           │  via Airflow DAG    │  │  surprise_leaderboard │
           └─────────────────────┘  └──────────────────────┘
```

---

## 📨 Event Schema

Every economic release produces a JSON event on `econ.releases.raw`:

```json
{
  "event_id":      "550e8400-e29b-41d4-a716-446655440000",
  "source":        "BLS",
  "release_name":  "BLS_JOBS_REPORT",
  "indicator":     "nonfarm_payrolls_thousands",
  "period":        "2024-01",
  "value":         353.0,
  "prior_value":   216.0,
  "consensus":     180.0,
  "surprise_pct":  null,
  "revision":      false,
  "release_ts":    "2024-02-02T13:30:00+00:00",
  "ingested_ts":   "2024-02-02T13:30:01+00:00"
}
```

After Spark enrichment on `econ.releases.enriched`:

```json
{
  ...all above fields...,
  "surprise_pct":    96.11,
  "market_impact":   "HIGH",
  "value_direction": "UP",
  "is_anomaly":      true,
  "anomaly_reason":  "Large surprise: 96.11%",
  "processed_ts":    "2024-02-02T13:30:05+00:00"
}
```

---

## 📦 Kafka Topics

| Topic | Partitions | Retention | Purpose |
|-------|-----------|-----------|---------|
| `econ.releases.raw` | 3 | 7 days | Raw events from all producers |
| `econ.releases.enriched` | 3 | 7 days | Spark-enriched events |
| `econ.releases.anomalies` | 1 | 30 days | High-surprise events for alerting |
| `econ.releases.dlq` | 1 | 30 days | Failed/malformed events |

---

## 📁 Project Structure

```
econ-streaming-pipeline/
├── producers/
│   └── release_producer.py     # Kafka producer (replay / live / test modes)
│
├── spark/
│   └── streaming_job.py        # Spark Structured Streaming job
│
├── delta/
│   └── delta_streaming.py      # Delta Lake admin: OPTIMIZE, VACUUM, queries
│
├── config/
│   └── schemas.py              # Event schema, topic names, release catalog
│
├── scripts/
│   └── init_streaming_schema.sql  # PostgreSQL schema for streaming sink
│
├── airflow/
│   └── dags/
│       └── dag_streaming_pipeline.py  # Kafka health, Spark submit, maintenance
│
├── docker/
│   └── docker-compose.yml      # Zookeeper, Kafka, Schema Registry, Kafka UI, Spark
│
└── requirements.txt
```

---

## 🚀 Quick Start

### 1. Start the infrastructure

```bash
cd econ-streaming-pipeline
docker compose -f docker/docker-compose.yml up -d

# Wait ~60s for Kafka to be ready, then verify:
docker compose logs kafka-init | tail -20
# Should show 4 topics created
```

### 2. Access the UIs

| UI | URL | Purpose |
|----|-----|---------|
| Kafka UI | http://localhost:8090 | Browse topics, messages, consumer groups |
| Spark UI | http://localhost:8091 | Monitor running streaming jobs |

### 3. Initialise PostgreSQL sink

```bash
# From econ-data-pipeline root (where Docker stack runs)
docker exec -i econ-data-pipeline-postgres-1 \
  psql -U econ_user -d econ_warehouse \
  < scripts/init_streaming_schema.sql
```

### 4. Start the Spark streaming job

```bash
# Inside the Spark master container
docker exec -it econ-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages \
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
    io.delta:delta-spark_2.12:3.0.0,\
    org.postgresql:postgresql:42.6.0 \
  /opt/spark-apps/streaming_job.py
```

### 5. Produce events

```bash
pip install -r requirements.txt
set -a && source ../.env && set +a

# Replay 2024 economic calendar (fast — good for demos)
python producers/release_producer.py --mode replay --speed 20

# Send a single test event
python producers/release_producer.py --mode test --release BLS_JOBS_REPORT

# Live polling (runs indefinitely)
python producers/release_producer.py --mode live --poll-interval 300
```

### 6. Query results

**In Kafka UI** (http://localhost:8090):
- Topics → `econ.releases.raw` → Messages
- Topics → `econ.releases.anomalies` → see high-surprise events

**In pgAdmin** (http://localhost:5050):
```sql
-- Latest releases processed
SELECT * FROM streaming.latest_by_indicator ORDER BY processed_ts DESC;

-- FOMC rate decision history
SELECT * FROM streaming.fomc_history;

-- Which releases have the biggest surprises?
SELECT * FROM streaming.surprise_leaderboard;

-- Recent anomalies
SELECT release_name, period, value, surprise_pct, anomaly_reason
FROM streaming.releases_enriched
WHERE is_anomaly = TRUE
ORDER BY processed_ts DESC;
```

**In Spark (Delta Lake)**:
```bash
docker exec -it econ-spark-master python3 \
  /opt/spark-apps/../delta/delta_streaming.py --action latest

# Show surprise analysis
docker exec -it econ-spark-master python3 \
  /opt/spark-apps/../delta/delta_streaming.py --action surprise
```

---

## 🔬 Design Decisions

**Why Kafka over a simple database queue?**
Kafka decouples producers from consumers. The Spark job can restart, catch up from any offset, and replay events without the producer needing to resend. Multiple consumers can read the same topic independently — the forecast engine, the dashboard, and an alert system all read `econ.releases.enriched` without interfering with each other.

**Why Spark Structured Streaming over Faust or Kafka Streams?**
Spark lets us write the same DataFrame API for batch (dbt/DuckDB) and streaming — same SQL expressions, same Delta Lake sink. SHAP enrichment, ML scoring, and join-with-historical-context all work identically whether processing one event or one million. Faust is Python-native and simpler but doesn't have the same analytics ecosystem.

**Why watermarking at 1 hour?**
BEA, FRED, and BLS sometimes publish corrections or revised figures hours after the initial release. A 1-hour watermark lets Spark correctly aggregate events that arrive late without holding state indefinitely.

**Why foreachBatch for the PostgreSQL sink?**
Spark's native JDBC sink doesn't support upserts — every write is an append. `foreachBatch` gives us a full DataFrame inside each micro-batch, so we can deduplicate on `event_id` before writing, preventing duplicates if the Spark job restarts mid-batch.

**Why Delta Lake over Parquet directly?**
Streaming jobs produce thousands of small Parquet files (one per micro-batch per partition). Without Delta Lake, querying them requires listing all files — slow and brittle. Delta's transaction log makes the table queryable as a single logical table, and OPTIMIZE compacts the small files daily.

---

## 🔄 Airflow DAG

`dag_streaming_pipeline` runs daily:

```
health_check_kafka ─┐
                    ├─→ submit_spark_job → monitor_consumer_lag ─┬─→ delta_maintenance
health_check_spark ─┘                                            └─→ postgres_maintenance
```

---

## 📈 Upstream / Downstream

| Direction | Project | Relationship |
|-----------|---------|-------------|
| Upstream  | `econ-data-pipeline` | PostgreSQL for historical context; shares Docker network |
| Downstream | `econ-ml-platform` | Reads enriched releases for real-time feature updates |
| Downstream | `econ-forecaster-dashboard` | Reads PostgreSQL streaming views for live feed |

---

## 📄 License

MIT
