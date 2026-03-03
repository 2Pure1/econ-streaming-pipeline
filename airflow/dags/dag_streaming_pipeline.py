"""
dag_streaming_pipeline.py
--------------------------
Airflow DAG that manages the econ-streaming-pipeline lifecycle:

  1. health_check_kafka      — verify Kafka broker is reachable
  2. health_check_spark      — verify Spark master is reachable
  3. submit_spark_job        — submit streaming_job.py to Spark cluster
  4. monitor_lag             — watch consumer group lag every 5 minutes
  5. run_delta_maintenance   — daily OPTIMIZE + VACUUM on Delta tables
  6. run_postgres_maintenance — VACUUM ANALYZE on streaming.releases_enriched

The streaming job itself runs continuously (not managed by Airflow task lifecycle).
Airflow manages: startup, monitoring, and maintenance around it.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator

SPARK_MASTER      = "spark://spark-master:7077"
SPARK_APPS_DIR    = "/opt/spark-apps"
KAFKA_SERVERS     = "kafka:29092"
DELTA_BASE        = "/tmp/delta_lake"

default_args = {
    "owner":           "econ-team",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id="dag_streaming_pipeline",
    description="Manage econ-streaming-pipeline: health checks, job submission, maintenance",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["streaming", "kafka", "spark", "delta"],
) as dag:

    # ── 1. Kafka health check ──────────────────────────────────────────────────
    kafka_health = BashOperator(
        task_id="health_check_kafka",
        bash_command=f"""
            kafka-broker-api-versions \
              --bootstrap-server {KAFKA_SERVERS} \
              --command-config /dev/null 2>&1 | grep -q "kafka" && echo "Kafka OK" || exit 1
        """,
    )

    # ── 2. Spark health check ──────────────────────────────────────────────────
    spark_health = BashOperator(
        task_id="health_check_spark",
        bash_command="curl -sf http://spark-master:8080 | grep -q 'Spark Master' && echo 'Spark OK' || exit 1",
    )

    # ── 3. Submit Spark streaming job ─────────────────────────────────────────
    submit_spark = BashOperator(
        task_id="submit_spark_job",
        bash_command=f"""
            spark-submit \
              --master {SPARK_MASTER} \
              --name "EconStreamingPipeline" \
              --packages \
                org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
                io.delta:delta-spark_2.12:3.0.0,\
                org.postgresql:postgresql:42.6.0 \
              --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
              --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
              --conf spark.streaming.stopGracefullyOnShutdown=true \
              --conf spark.executor.memory=2g \
              --conf spark.driver.memory=1g \
              --deploy-mode client \
              {SPARK_APPS_DIR}/streaming_job.py \
              > /tmp/spark_streaming.log 2>&1 &

            echo "Spark streaming job submitted (PID: $!)"
            echo $! > /tmp/spark_streaming.pid
            sleep 30
            cat /tmp/spark_streaming.log | tail -20
        """,
        execution_timeout=timedelta(minutes=5),
    )

    # ── 4. Monitor consumer lag ────────────────────────────────────────────────
    def check_consumer_lag(**context):
        """
        Check Kafka consumer group lag for the Spark streaming job.
        High lag = Spark is falling behind the producer rate.
        Alert threshold: 10,000 messages.
        """
        import subprocess
        import re

        result = subprocess.run(
            [
                "kafka-consumer-groups",
                "--bootstrap-server", KAFKA_SERVERS,
                "--describe",
                "--group", "spark-streaming-consumer",
            ],
            capture_output=True, text=True, timeout=30
        )

        total_lag = 0
        for line in result.stdout.splitlines():
            match = re.search(r'\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)', line)
            if match:
                lag = int(match.group(4))
                total_lag += lag

        context["ti"].xcom_push(key="consumer_lag", value=total_lag)

        if total_lag > 10000:
            raise ValueError(f"Consumer lag too high: {total_lag} messages behind. "
                             f"Check Spark job status.")

        print(f"Consumer lag: {total_lag} messages (within threshold)")
        return total_lag

    monitor_lag = PythonOperator(
        task_id="monitor_consumer_lag",
        python_callable=check_consumer_lag,
    )

    # ── 5. Delta Lake maintenance ──────────────────────────────────────────────
    delta_maintenance = BashOperator(
        task_id="delta_maintenance",
        bash_command=f"""
            spark-submit \
              --master {SPARK_MASTER} \
              --packages io.delta:delta-spark_2.12:3.0.0 \
              --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
              {SPARK_APPS_DIR}/../delta/delta_streaming.py \
              --action optimize \
              --path {DELTA_BASE}/streaming/releases_enriched

            spark-submit \
              --master {SPARK_MASTER} \
              --packages io.delta:delta-spark_2.12:3.0.0 \
              {SPARK_APPS_DIR}/../delta/delta_streaming.py \
              --action vacuum \
              --path {DELTA_BASE}/streaming/releases_enriched \
              --retain-hours 168
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # ── 6. PostgreSQL maintenance ──────────────────────────────────────────────
    postgres_maintenance = BashOperator(
        task_id="postgres_maintenance",
        bash_command="""
            psql $POSTGRES_JDBC_URL \
              -c "VACUUM ANALYZE streaming.releases_enriched;" \
              -c "SELECT COUNT(*), MAX(processed_ts) FROM streaming.releases_enriched;"
        """,
    )

    # ── DAG wiring ─────────────────────────────────────────────────────────────
    [kafka_health, spark_health] >> submit_spark >> monitor_lag
    monitor_lag >> [delta_maintenance, postgres_maintenance]
