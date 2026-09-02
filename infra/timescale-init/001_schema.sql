-- ============================================================================
-- Market Data Time-Series Schema (TimescaleDB)
--
-- Design notes / why it's shaped this way:
--
-- 1. Cardinality control: "exchange" and "symbol" are low-cardinality TEXT
--    columns (a handful of exchanges, a handful of tracked symbols) stored as
--    plain columns, NOT as a sprawling set of per-symbol tables or a JSONB
--    tag-bag. That's the #1 way time-series schemas blow up: treating every
--    symbol/side/exchange combo as its own series or index. Here there is
--    exactly ONE hypertable for trades and ONE for book events, partitioned
--    by time, with symbol/exchange as regular indexed columns. Cardinality
--    stays bounded by (exchanges x symbols), not by every tick.
--
-- 2. Raw ticks vs bars: raw trades are the source of truth. 1m/1h/1d bars are
--    materialized views (continuous aggregates) computed FROM raw trades,
--    refreshed incrementally by Timescale's background jobs -- we never
--    hand-roll downsample logic that can drift from the source.
--
-- 3. Order book: we do NOT persist full depth snapshots on every update
--    (that's an unbounded-cardinality trap: N price levels x every update).
--    Instead we persist the raw normalized diff/snapshot EVENTS (small,
--    append-only, needed for replay/backtesting/audit) and reconstruct the
--    live book in memory (see src/orderbook). Periodic compacted snapshots
--    are stored separately and pruned aggressively.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ----------------------------------------------------------------------------
-- Raw trades (tick data)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trades (
    ts          TIMESTAMPTZ       NOT NULL,
    exchange    TEXT              NOT NULL,
    symbol      TEXT              NOT NULL,
    trade_id    TEXT              NOT NULL,
    price       DOUBLE PRECISION  NOT NULL,
    size        DOUBLE PRECISION  NOT NULL,
    side        TEXT              NOT NULL CHECK (side IN ('buy', 'sell')),
    ingested_at TIMESTAMPTZ       NOT NULL DEFAULT now()
);

SELECT create_hypertable(
    'trades', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- One composite index covers the common query pattern (symbol + time range).
-- Deliberately not indexing every column -- extra indexes cost write
-- throughput on a high-frequency tick table.
CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts
    ON trades (exchange, symbol, ts DESC);

-- Dedup guard: exchanges occasionally redeliver the same trade_id on
-- reconnect. Enforced as a unique index rather than a constraint on the
-- hypertable's implicit primary key (hypertables can't have a PK that
-- excludes the partitioning column without including it).
CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_exchange_symbol_tradeid
    ON trades (exchange, symbol, trade_id, ts);

-- ----------------------------------------------------------------------------
-- Order book raw events (append-only log of snapshots + diffs as received).
-- This is what lets backtesting replay the book exactly as it evolved live,
-- and what correctness tests replay against a ground truth.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS book_events (
    ts            TIMESTAMPTZ       NOT NULL,
    exchange      TEXT              NOT NULL,
    symbol        TEXT              NOT NULL,
    event_type    TEXT              NOT NULL CHECK (event_type IN ('snapshot', 'diff')),
    side          TEXT              NOT NULL CHECK (side IN ('bid', 'ask')),
    price         DOUBLE PRECISION  NOT NULL,
    size          DOUBLE PRECISION  NOT NULL,  -- 0 means "remove this level"
    first_update_id BIGINT,
    last_update_id  BIGINT,
    ingested_at   TIMESTAMPTZ       NOT NULL DEFAULT now()
);

SELECT create_hypertable(
    'book_events', 'ts',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_book_events_symbol_ts
    ON book_events (exchange, symbol, ts DESC);

-- ----------------------------------------------------------------------------
-- Continuous aggregates: raw ticks -> 1m / 1h / 1d OHLCV bars.
-- These refresh incrementally in the background -- no manual downsample job.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', ts) AS bucket,
    exchange,
    symbol,
    first(price, ts)  AS open,
    max(price)        AS high,
    min(price)        AS low,
    last(price, ts)   AS close,
    sum(size)         AS volume,
    count(*)          AS trade_count
FROM trades
GROUP BY bucket, exchange, symbol
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    exchange,
    symbol,
    first(open, bucket)  AS open,
    max(high)            AS high,
    min(low)             AS low,
    last(close, bucket)  AS close,
    sum(volume)          AS volume,
    sum(trade_count)     AS trade_count
FROM bars_1m
GROUP BY time_bucket('1 hour', bucket), exchange, symbol
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS bars_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', bucket) AS bucket,
    exchange,
    symbol,
    first(open, bucket)  AS open,
    max(high)            AS high,
    min(low)             AS low,
    last(close, bucket)  AS close,
    sum(volume)          AS volume,
    sum(trade_count)     AS trade_count
FROM bars_1h
GROUP BY time_bucket('1 day', bucket), exchange, symbol
WITH NO DATA;

-- Refresh policies: keep bars near-real-time without manual intervention.
SELECT add_continuous_aggregate_policy('bars_1m',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

SELECT add_continuous_aggregate_policy('bars_1h',
    start_offset => INTERVAL '3 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE);

SELECT add_continuous_aggregate_policy('bars_1d',
    start_offset => INTERVAL '14 days',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '6 hours',
    if_not_exists => TRUE);

-- ----------------------------------------------------------------------------
-- Tiered retention: raw ticks are expensive to keep forever and are only
-- needed at full resolution for recent history + backtesting windows you've
-- already materialized into bars. Drop raw trade chunks after 30 days;
-- keep bars indefinitely (they're tiny by comparison).
-- Order book raw events are bulkier and less needed long-term -> shorter window.
-- ----------------------------------------------------------------------------
SELECT add_retention_policy('trades', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('book_events', INTERVAL '3 days', if_not_exists => TRUE);

-- Compression: compress trade chunks older than 2 days to cut storage
-- further before they age out entirely.
ALTER TABLE trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'exchange, symbol'
);
SELECT add_compression_policy('trades', INTERVAL '2 days', if_not_exists => TRUE);
