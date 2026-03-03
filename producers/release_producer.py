"""
release_producer.py
-------------------
Kafka producer that publishes economic data release events to econ.releases.raw.

Three production modes:
  1. --mode live     Poll BEA/FRED/BLS APIs on their release schedules
  2. --mode replay   Replay the 2024 release calendar (for testing / demo)
  3. --mode webhook  Listen for HTTP POST webhooks from data vendors

Each mode produces the same EconomicReleaseEvent JSON envelope so the
downstream Spark consumer doesn't care about the source mode.

Usage:
    # Replay 2024 releases (good for demos)
    python producers/release_producer.py --mode replay --speed 10

    # Live polling (runs continuously)
    python producers/release_producer.py --mode live

    # Send a single test event
    python producers/release_producer.py --mode test --release BLS_JOBS_REPORT
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from confluent_kafka import Producer
from loguru import logger

from config.schemas import (
    RELEASE_CATALOG,
    RELEASE_CALENDAR_2024,
    TOPICS,
    EconomicReleaseEvent,
)

# ── Kafka producer config ──────────────────────────────────────────────────────
def build_producer(bootstrap_servers: str = "localhost:9092") -> Producer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id":         "econ-release-producer",
        "acks":              "all",          # wait for all replicas
        "retries":           5,
        "retry.backoff.ms":  500,
        "compression.type":  "lz4",
        "batch.size":        16384,
        "linger.ms":         10,
    }
    return Producer(conf)


def delivery_callback(err, msg):
    if err:
        logger.error(f"Delivery failed: {err} | topic={msg.topic()} key={msg.key()}")
    else:
        logger.debug(f"Delivered: topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def publish_event(producer: Producer, event: EconomicReleaseEvent) -> None:
    """Publish a single release event to the raw topic."""
    producer.produce(
        topic=TOPICS["raw"],
        key=f"{event.source}:{event.release_name}:{event.period}",
        value=event.to_json().encode("utf-8"),
        headers={
            "source":       event.source.encode(),
            "release_name": event.release_name.encode(),
            "event_id":     event.event_id.encode(),
        },
        callback=delivery_callback,
    )
    producer.poll(0)   # trigger callbacks without blocking
    logger.info(f"Published: {event.release_name} | {event.indicator}={event.value} | period={event.period}")


# ── Mode 1: Replay 2024 calendar ──────────────────────────────────────────────
def replay_calendar(producer: Producer, speed: float = 1.0) -> None:
    """
    Replay the 2024 economic release calendar in order.
    speed: 1.0 = real time between releases, 10.0 = 10x faster (for demos)
    Events are spaced proportionally to their real-world time gaps.
    """
    logger.info(f"Replaying 2024 release calendar | speed={speed}x")
    sorted_calendar = sorted(RELEASE_CALENDAR_2024, key=lambda x: x[0])

    prev_date = None
    for release_date_str, release_name, expected_value in sorted_calendar:
        release_date = datetime.strptime(release_date_str, "%Y-%m-%d")

        # Sleep proportionally to the real gap between releases
        if prev_date is not None:
            gap_days = (release_date - prev_date).days
            sleep_sec = max(0.5, gap_days * 0.1 / speed)
            logger.info(f"Waiting {sleep_sec:.1f}s before next release...")
            time.sleep(sleep_sec)

        catalog = RELEASE_CATALOG.get(release_name, {})
        if not catalog:
            logger.warning(f"Unknown release: {release_name} — skipping")
            continue

        # Simulate actual vs consensus (add ±5% noise to expected)
        import random
        actual = expected_value or 0.0
        consensus = round(actual * (1 + random.uniform(-0.05, 0.05)), 2) if actual else None
        prior = round(actual * (1 + random.uniform(-0.08, 0.08)), 2) if actual else None

        event = EconomicReleaseEvent(
            source=catalog["source"],
            release_name=release_name,
            indicator=catalog["indicator"],
            period=release_date.strftime("%Y-%m"),
            value=actual,
            prior_value=prior,
            consensus=consensus,
            surprise_pct=None,   # computed downstream in Spark
            revision=False,
            release_ts=datetime(
                release_date.year, release_date.month, release_date.day,
                8, 30, 0, tzinfo=timezone.utc   # most US releases at 8:30 ET = 13:30 UTC
            ).isoformat(),
            ingested_ts=datetime.now(timezone.utc).isoformat(),
        )

        publish_event(producer, event)
        prev_date = release_date

    producer.flush()
    logger.success("Calendar replay complete")


# ── Mode 2: Live polling ───────────────────────────────────────────────────────
def live_poll(producer: Producer, poll_interval_seconds: int = 300) -> None:
    """
    Poll FRED API every poll_interval_seconds for new releases.
    In production this would be supplemented by webhook subscriptions.
    """
    fred_api_key = os.environ.get("FRED_API_KEY")
    if not fred_api_key:
        raise ValueError("FRED_API_KEY not set in environment")

    # FRED series to watch
    watched_series = {
        "FEDFUNDS": ("FOMC_RATE_DECISION", "fed_funds_rate"),
        "CPIAUCSL": ("FRED_CPI",           "cpi_yoy_pct"),
        "PCEPILFE": ("FRED_CORE_PCE",      "core_pce_yoy_pct"),
        "M2SL":     ("FRED_M2",            "m2_money_supply_billions"),
        "UNRATE":   ("BLS_UNEMPLOYMENT",   "unemployment_rate"),
        "PAYEMS":   ("BLS_JOBS_REPORT",    "nonfarm_payrolls_thousands"),
    }

    seen_observations = {}  # track last seen value per series

    logger.info(f"Starting live poll | interval={poll_interval_seconds}s")

    while True:
        for series_id, (release_name, indicator) in watched_series.items():
            try:
                resp = requests.get(
                    "https://api.stlouisfed.org/fred/series/observations",
                    params={
                        "series_id":     series_id,
                        "api_key":       fred_api_key,
                        "file_type":     "json",
                        "sort_order":    "desc",
                        "limit":         2,
                        "observation_start": "2020-01-01",
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                obs  = data.get("observations", [])
                if not obs:
                    continue

                latest = obs[0]
                period = latest["date"]
                value  = float(latest["value"]) if latest["value"] != "." else None
                if value is None:
                    continue

                # Only publish if this is a new observation we haven't seen
                cache_key = f"{series_id}:{period}"
                if cache_key not in seen_observations:
                    prior_value = float(obs[1]["value"]) if len(obs) > 1 and obs[1]["value"] != "." else None
                    event = EconomicReleaseEvent(
                        source="FRED",
                        release_name=release_name,
                        indicator=indicator,
                        period=period,
                        value=value,
                        prior_value=prior_value,
                        ingested_ts=datetime.now(timezone.utc).isoformat(),
                    )
                    publish_event(producer, event)
                    seen_observations[cache_key] = value

            except Exception as e:
                logger.warning(f"Poll failed for {series_id}: {e}")

        producer.flush()
        logger.debug(f"Poll cycle complete. Sleeping {poll_interval_seconds}s...")
        time.sleep(poll_interval_seconds)


# ── Mode 3: Single test event ──────────────────────────────────────────────────
def publish_test_event(producer: Producer, release_name: str) -> None:
    catalog = RELEASE_CATALOG.get(release_name)
    if not catalog:
        raise ValueError(f"Unknown release: {release_name}. Valid: {list(RELEASE_CATALOG)}")

    event = EconomicReleaseEvent(
        source=catalog["source"],
        release_name=release_name,
        indicator=catalog["indicator"],
        period=datetime.now().strftime("%Y-%m"),
        value=round(4.1 + (hash(release_name) % 100) / 100, 2),  # deterministic fake value
        prior_value=4.0,
        consensus=4.05,
        ingested_ts=datetime.now(timezone.utc).isoformat(),
    )
    publish_event(producer, event)
    producer.flush()
    logger.success(f"Test event published: {release_name}")


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Economic release Kafka producer")
    parser.add_argument("--mode", choices=["live", "replay", "test"], default="replay")
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--speed", type=float, default=5.0, help="Replay speed multiplier")
    parser.add_argument("--poll-interval", type=int, default=300, help="Live poll interval in seconds")
    parser.add_argument("--release", default="BLS_JOBS_REPORT", help="Release name for --mode test")
    args = parser.parse_args()

    producer = build_producer(args.bootstrap_servers)
    logger.info(f"Producer connected to {args.bootstrap_servers}")

    if args.mode == "replay":
        replay_calendar(producer, speed=args.speed)
    elif args.mode == "live":
        live_poll(producer, poll_interval_seconds=args.poll_interval)
    elif args.mode == "test":
        publish_test_event(producer, args.release)
