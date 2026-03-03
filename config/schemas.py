"""
schemas.py
----------
Defines the event schemas for economic data releases flowing through Kafka.

Three event types published to econ.releases.raw:
  1. BEA release  — GDP, PCE, income data (quarterly / monthly)
  2. FRED release — Fed Funds Rate, CPI, M2 updates
  3. BLS release  — Jobs report, CPI components (monthly, 1st Friday / mid-month)

Each event is a JSON message with a standardised envelope:
  {
    "event_id":      UUID,
    "source":        "BEA" | "FRED" | "BLS",
    "release_name":  "GDP_ADVANCE" | "FOMC_RATE_DECISION" | "JOBS_REPORT" | ...,
    "indicator":     "gdp_qoq_pct" | "fed_funds_rate" | "nonfarm_payrolls" | ...,
    "period":        "2024-Q4" | "2024-12" | "2024-01-01",
    "value":         float,
    "prior_value":   float | null,
    "surprise_pct":  float | null,   # (actual - consensus) / |consensus| * 100
    "revision":      bool,           # true if this is a revision to a prior release
    "release_ts":    ISO8601 UTC,    # when the agency published this
    "ingested_ts":   ISO8601 UTC,    # when our producer received it
  }
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

# ── Kafka topic names ─────────────────────────────────────────────────────────
TOPICS = {
    "raw":       "econ.releases.raw",        # raw release events from producers
    "enriched":  "econ.releases.enriched",   # after Spark enrichment + validation
    "anomalies": "econ.releases.anomalies",  # surprise > 2σ or data quality issues
    "dlq":       "econ.releases.dlq",        # dead-letter queue for failed events
}

# ── Release names → indicator mappings ───────────────────────────────────────
RELEASE_CATALOG = {
    # BEA releases
    "BEA_GDP_ADVANCE":     {"indicator": "gdp_qoq_pct",          "source": "BEA", "frequency": "quarterly"},
    "BEA_GDP_SECOND":      {"indicator": "gdp_qoq_pct",          "source": "BEA", "frequency": "quarterly"},
    "BEA_GDP_THIRD":       {"indicator": "gdp_qoq_pct",          "source": "BEA", "frequency": "quarterly"},
    "BEA_PCE":             {"indicator": "pce_mom_pct",           "source": "BEA", "frequency": "monthly"},
    "BEA_PERSONAL_INCOME": {"indicator": "personal_income_mom",   "source": "BEA", "frequency": "monthly"},

    # FRED / Federal Reserve releases
    "FOMC_RATE_DECISION":  {"indicator": "fed_funds_rate",        "source": "FRED", "frequency": "6-weekly"},
    "FRED_CPI":            {"indicator": "cpi_yoy_pct",           "source": "FRED", "frequency": "monthly"},
    "FRED_CORE_PCE":       {"indicator": "core_pce_yoy_pct",      "source": "FRED", "frequency": "monthly"},
    "FRED_M2":             {"indicator": "m2_money_supply_billions","source": "FRED", "frequency": "weekly"},

    # BLS releases
    "BLS_JOBS_REPORT":     {"indicator": "nonfarm_payrolls_thousands","source": "BLS","frequency": "monthly"},
    "BLS_UNEMPLOYMENT":    {"indicator": "unemployment_rate",     "source": "BLS", "frequency": "monthly"},
    "BLS_CPI_DETAIL":      {"indicator": "cpi_all_urban",         "source": "BLS", "frequency": "monthly"},
    "BLS_PPI":             {"indicator": "ppi_final_demand",      "source": "BLS", "frequency": "monthly"},
}

# ── 2024 Economic release calendar (for scheduler / testing) ─────────────────
# Format: (YYYY-MM-DD, release_name, expected_value_hint)
RELEASE_CALENDAR_2024 = [
    ("2024-01-05", "BLS_JOBS_REPORT",     216.0),   # Dec 2023 jobs
    ("2024-01-11", "BLS_CPI_DETAIL",      3.4),
    ("2024-01-26", "BEA_PCE",             None),
    ("2024-01-31", "FOMC_RATE_DECISION",  5.33),
    ("2024-02-02", "BLS_JOBS_REPORT",     353.0),   # Jan 2024 — huge beat
    ("2024-02-13", "BLS_CPI_DETAIL",      3.1),
    ("2024-02-28", "BEA_GDP_SECOND",      3.2),     # Q4 2023 2nd estimate
    ("2024-03-08", "BLS_JOBS_REPORT",     275.0),
    ("2024-03-12", "BLS_CPI_DETAIL",      3.2),
    ("2024-03-20", "FOMC_RATE_DECISION",  5.33),    # hold
    ("2024-03-28", "BEA_PCE",             None),
    ("2024-04-05", "BLS_JOBS_REPORT",     303.0),
    ("2024-04-10", "BLS_CPI_DETAIL",      3.5),     # hot CPI — market shock
    ("2024-04-25", "BEA_GDP_ADVANCE",     1.6),     # Q1 2024 advance — miss
    ("2024-05-03", "BLS_JOBS_REPORT",     175.0),
    ("2024-05-15", "BLS_CPI_DETAIL",      3.4),
    ("2024-06-07", "BLS_JOBS_REPORT",     272.0),
    ("2024-06-12", "FOMC_RATE_DECISION",  5.33),    # hold again
    ("2024-07-05", "BLS_JOBS_REPORT",     206.0),
    ("2024-07-11", "BLS_CPI_DETAIL",      3.0),     # cooling
    ("2024-07-26", "BEA_GDP_ADVANCE",     2.8),     # Q2 2024 — beat
    ("2024-09-18", "FOMC_RATE_DECISION",  5.0),     # FIRST CUT -25bps
    ("2024-11-07", "FOMC_RATE_DECISION",  4.75),    # second cut
    ("2024-12-18", "FOMC_RATE_DECISION",  4.5),     # third cut
]


# ── Event dataclass ────────────────────────────────────────────────────────────
@dataclass
class EconomicReleaseEvent:
    source:       str
    release_name: str
    indicator:    str
    period:       str
    value:        float
    prior_value:  Optional[float] = None
    surprise_pct: Optional[float] = None
    revision:     bool = False
    consensus:    Optional[float] = None
    event_id:     str = field(default_factory=lambda: str(uuid.uuid4()))
    release_ts:   str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ingested_ts:  str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "EconomicReleaseEvent":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    @classmethod
    def from_json(cls, s: str) -> "EconomicReleaseEvent":
        return cls.from_dict(json.loads(s))

    def compute_surprise(self) -> Optional[float]:
        """
        Market surprise = (actual - consensus) / |consensus| * 100.
        Positive = beat (stronger than expected).
        Negative = miss (weaker than expected).
        """
        if self.consensus is not None and self.consensus != 0:
            return round((self.value - self.consensus) / abs(self.consensus) * 100, 4)
        return None
