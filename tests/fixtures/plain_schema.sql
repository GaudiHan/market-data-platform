-- Test-only schema: same tables/columns/indexes as infra/timescale-init/001_schema.sql,
-- minus TimescaleDB-specific calls (create_hypertable, continuous aggregates,
-- compression, retention policies) which require the timescaledb extension
-- that isn't installable in a plain CI/sandbox Postgres. This lets the
-- storage-writer logic (inserts, ON CONFLICT dedup, batching) be verified
-- against a real running Postgres without needing the full Timescale stack.
-- The production schema (001_schema.sql) is the source of truth; keep the
-- two in sync if columns change.

DROP TABLE IF EXISTS trades;
CREATE TABLE trades (
    ts          TIMESTAMPTZ       NOT NULL,
    exchange    TEXT              NOT NULL,
    symbol      TEXT              NOT NULL,
    trade_id    TEXT              NOT NULL,
    price       DOUBLE PRECISION  NOT NULL,
    size        DOUBLE PRECISION  NOT NULL,
    side        TEXT              NOT NULL CHECK (side IN ('buy', 'sell')),
    ingested_at TIMESTAMPTZ       NOT NULL DEFAULT now()
);
CREATE INDEX idx_trades_symbol_ts ON trades (exchange, symbol, ts DESC);
CREATE UNIQUE INDEX uq_trades_exchange_symbol_tradeid ON trades (exchange, symbol, trade_id, ts);

DROP TABLE IF EXISTS book_events;
CREATE TABLE book_events (
    ts              TIMESTAMPTZ       NOT NULL,
    exchange        TEXT              NOT NULL,
    symbol          TEXT              NOT NULL,
    event_type      TEXT              NOT NULL CHECK (event_type IN ('snapshot', 'diff')),
    side            TEXT              NOT NULL CHECK (side IN ('bid', 'ask')),
    price           DOUBLE PRECISION  NOT NULL,
    size            DOUBLE PRECISION  NOT NULL,
    first_update_id BIGINT,
    last_update_id  BIGINT,
    ingested_at     TIMESTAMPTZ       NOT NULL DEFAULT now()
);
CREATE INDEX idx_book_events_symbol_ts ON book_events (exchange, symbol, ts DESC);
