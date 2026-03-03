-- scripts/init_streaming_schema.sql
-- Run once to create the PostgreSQL sink tables for the streaming pipeline.
-- Execute in pgAdmin or: psql -U econ_user -d econ_warehouse -f scripts/init_streaming_schema.sql

-- ── Schema ─────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS streaming;

-- ── Main enriched releases table ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS streaming.releases_enriched (
    id               BIGSERIAL    PRIMARY KEY,
    event_id         UUID         NOT NULL,
    source           VARCHAR(20)  NOT NULL,          -- BEA / FRED / BLS
    release_name     VARCHAR(60)  NOT NULL,          -- BLS_JOBS_REPORT, etc.
    indicator        VARCHAR(60)  NOT NULL,          -- nonfarm_payrolls_thousands
    period           VARCHAR(20)  NOT NULL,          -- 2024-01
    value            DOUBLE PRECISION,
    prior_value      DOUBLE PRECISION,
    consensus        DOUBLE PRECISION,
    surprise_pct     DOUBLE PRECISION,               -- (actual - consensus) / |consensus| * 100
    market_impact    VARCHAR(10),                    -- HIGH / MEDIUM / LOW
    value_direction  VARCHAR(10),                    -- UP / DOWN / FLAT / UNKNOWN
    is_anomaly       BOOLEAN      DEFAULT FALSE,
    anomaly_reason   TEXT,
    revision         BOOLEAN      DEFAULT FALSE,
    release_ts       TIMESTAMPTZ,                    -- when agency published
    processed_ts     TIMESTAMPTZ  DEFAULT NOW(),     -- when Spark processed
    UNIQUE (event_id)
);

-- ── Indexes for common query patterns ─────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_releases_source
    ON streaming.releases_enriched (source);

CREATE INDEX IF NOT EXISTS idx_releases_release_name
    ON streaming.releases_enriched (release_name);

CREATE INDEX IF NOT EXISTS idx_releases_period
    ON streaming.releases_enriched (period DESC);

CREATE INDEX IF NOT EXISTS idx_releases_processed_ts
    ON streaming.releases_enriched (processed_ts DESC);

CREATE INDEX IF NOT EXISTS idx_releases_market_impact
    ON streaming.releases_enriched (market_impact)
    WHERE market_impact = 'HIGH';

CREATE INDEX IF NOT EXISTS idx_releases_anomaly
    ON streaming.releases_enriched (is_anomaly)
    WHERE is_anomaly = TRUE;

-- ── View: latest release per indicator ─────────────────────────────────────────
CREATE OR REPLACE VIEW streaming.latest_by_indicator AS
SELECT DISTINCT ON (indicator)
    indicator, release_name, source, period, value,
    prior_value, surprise_pct, market_impact,
    value_direction, is_anomaly, processed_ts
FROM streaming.releases_enriched
ORDER BY indicator, processed_ts DESC;

-- ── View: recent high-impact events ───────────────────────────────────────────
CREATE OR REPLACE VIEW streaming.recent_high_impact AS
SELECT
    release_name, source, indicator, period,
    value, prior_value, surprise_pct,
    value_direction, anomaly_reason, processed_ts
FROM streaming.releases_enriched
WHERE market_impact = 'HIGH'
  AND processed_ts > NOW() - INTERVAL '90 days'
ORDER BY processed_ts DESC;

-- ── View: FOMC rate decision history ──────────────────────────────────────────
CREATE OR REPLACE VIEW streaming.fomc_history AS
SELECT
    period, value AS fed_funds_rate,
    prior_value AS prior_rate,
    ROUND((value - COALESCE(prior_value, value))::NUMERIC, 2) AS rate_change_bps,
    value_direction AS direction,
    surprise_pct,
    processed_ts
FROM streaming.releases_enriched
WHERE release_name = 'FOMC_RATE_DECISION'
ORDER BY period DESC;

-- ── View: surprise leaderboard ────────────────────────────────────────────────
CREATE OR REPLACE VIEW streaming.surprise_leaderboard AS
SELECT
    release_name,
    source,
    COUNT(*) AS n_releases,
    ROUND(AVG(ABS(surprise_pct))::NUMERIC, 2) AS mean_abs_surprise,
    ROUND(MAX(ABS(surprise_pct))::NUMERIC, 2) AS max_abs_surprise,
    SUM(CASE WHEN surprise_pct > 0 THEN 1 ELSE 0 END) AS beats,
    SUM(CASE WHEN surprise_pct < 0 THEN 1 ELSE 0 END) AS misses,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies
FROM streaming.releases_enriched
WHERE surprise_pct IS NOT NULL
GROUP BY release_name, source
ORDER BY mean_abs_surprise DESC;

-- ── Grant access ───────────────────────────────────────────────────────────────
GRANT USAGE ON SCHEMA streaming TO econ_user;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA streaming TO econ_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA streaming TO econ_user;

-- ── Verify ─────────────────────────────────────────────────────────────────────
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'streaming'
ORDER BY table_type, table_name;
