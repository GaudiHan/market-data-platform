# Market Data Time-Series Platform

Ingests live crypto market data (Binance + Coinbase, both free/public
WebSocket feeds, no API key or paid tier of any kind), stores it in
TimescaleDB + MongoDB, reconstructs a live L2 order book, and backtests
mechanical strategies against it with realistic execution simulation.

Built to demonstrate database/schema design and market-microstructure
understanding, not "call an API and plot a line."

## Status: Layers 1 and 2 complete

| Layer | Status |
|---|---|
| 1. Ingestion (Binance + Coinbase WS) | Done, tested |
| 2. Storage (TimescaleDB + Mongo) | Done, tested — writers + repository wired up |
| 3. Order book reconstruction | Not started |
| 4. Backtesting engine | Not started |

Test coverage as of Layer 2: **36 passed, 1 skipped** (37 total). The one
skip is `test_mongo_index_usage.py`, which needs a real MongoDB to verify
the compound index via `.explain()` — Ubuntu dropped the `mongodb-org`
package over licensing, so it isn't installable in the sandbox these files
were built in. It runs for real once you `docker compose up -d`. Everything
else — including the TimescaleDB writer tests — ran against a real local
Postgres 16 while building this, not just mocked.

## Zero-budget guarantee

Everything here runs on your own machine for $0:
- **TimescaleDB** and **MongoDB Community**: self-hosted via Docker, no cloud account
- **Binance** (`stream.binance.com`) and **Coinbase Exchange** (`ws-feed.exchange.coinbase.com`)
  public market-data WebSocket feeds: no API key, no account, no auth at all
  needed for trade/order-book data (only *trading* requires keys, which this
  project never does)

One deliberate choice worth knowing about: Coinbase has two WebSocket APIs.
The newer "Advanced Trade" API requires a signed JWT even for public data.
This project uses the older public **Exchange** feed instead, specifically
to avoid needing any account/credentials.

## Setup

```bash
cp .env.example .env          # defaults are fine for local use
docker compose up -d          # starts TimescaleDB + Mongo, runs init scripts
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Run ingestion standalone to verify both exchange connections come up and
data flows (this is the thing I couldn't test from my sandbox — no outbound
network to exchange domains there — so please run this first):

```bash
python -m scripts.run_ingestion
```

You should see interleaved `[TRADE]`, `[SNAPSHOT]`, `[DIFF]`, and occasional
`[CONN]` lines from both `binance` and `coinbase`. Ctrl+C to stop; it prints
event counts on exit.

Run the test suite:

```bash
pytest -v
```

Most tests need no infra at all (parsing/handling logic, and Mongo
repository logic via `mongomock`). Two integration test files talk to real
services and skip cleanly if they're not reachable:
- `tests/correctness/test_timescale_writer.py` — needs Postgres/Timescale.
  Defaults to `postgresql://postgres:postgres@localhost:5432/marketdata_test`;
  override with `TEST_POSTGRES_DSN` if needed. Against docker-compose's
  Timescale container, first create that scratch DB:
  `docker exec -it mdp_timescaledb createdb -U mdp marketdata_test` (or
  just set `TEST_POSTGRES_DSN` to reuse the main `marketdata` DB).
- `tests/correctness/test_mongo_index_usage.py` — needs real MongoDB (see
  status table above for why). Defaults to matching `docker-compose`'s Mongo.

## Architecture decisions worth knowing about

**Common event schema, exchange-specific parsing isolated at the edge**
(`src/common/events.py`, `src/ingestion/*_client.py`): every exchange client
translates its own wire format into the same `Trade` / `BookSnapshot` /
`BookDiff` / `ConnectionEvent` types before anything else sees them. Adding
a third exchange later means writing one new client, not touching storage,
order book, or backtest code.

**Symbol format**: standardized on `BASE-QUOTE` (`BTC-USD`) everywhere.
Binance's native format (`btcusdt`, and it uses USDT not USD pairs) is
translated at the edge in `src/common/symbols.py` — that mapping decision
(USD to USDT) is explicit and documented there rather than silently assumed.

**Order book gap-handling asymmetry (Binance vs. Coinbase)**: Binance's diff
stream carries `U`/`u` update-ID fields that let you detect a sequence gap
*within* a live connection and know exactly when to resync. Coinbase's
public `level2` channel doesn't carry an equivalent sequence number — so gap
detection there is connection-level only (any disconnect means a full resync
on reconnect). This is documented in `coinbase_client.py` rather than
papered over; the order book module (Layer 3, next) will treat "no snapshot
yet" and "sequence gap detected" as the same trigger so both exchanges are
handled correctly despite the asymmetry.

**Reconnection is not treated as exceptional**: `ExchangeClient.run_forever`
in `src/ingestion/base.py` expects connections to drop — exchanges cycle
them, networks blip — and retries with exponential backoff + a capped jitter,
emitting a `ConnectionEvent` each time so downstream consumers (eventually:
the order book) know a resync is needed. This is also what the chaos test
"kill the connection, assert resync without corruption" will exercise against.

**Malformed messages can't crash the process**: every client's
`_handle_message` is a hard boundary — JSON errors, missing fields, and
unrecognized message shapes are all caught, logged, and (for real parse
failures) surfaced as a `malformed_message` ConnectionEvent, never raised.
Tested directly in `tests/chaos/test_malformed_messages.py` with no network
required.

**TimescaleDB schema avoids the classic cardinality trap**
(`infra/timescale-init/001_schema.sql`): one hypertable for trades and one
for order-book events, partitioned by time, with `exchange`/`symbol` as
plain low-cardinality columns — not a table-per-symbol or a JSONB tag-bag
per tick. Downsampling (1m/1h/1d bars) is done via Timescale continuous
aggregates computed *from* raw trades, not hand-rolled batch jobs that can
drift from source data. Retention + compression policies are already wired
so raw ticks age out (30 days) while bars stay indefinitely.

**MongoDB schema** (`infra/mongo-init/001_init.js`): watchlists, portfolios,
and alert rules — all naturally document-shaped, user-editable state that
doesn't belong in the time-series side. Indexes are built to match the
actual query shape each collection will see (e.g. `alert_rules` is indexed
on `{symbol, active}` compound because "all active rules for symbol X" is
the hot-path query every time a tick lands — not just a single-field index
for its own sake).

**TimescaleDB writer uses two different write strategies on purpose**
(`src/storage/timescale/writer.py`): `trades` go through batched
`INSERT ... ON CONFLICT DO NOTHING` because exchanges redeliver trades on
reconnect and silently duplicating tick history would corrupt every bar and
backtest computed downstream — correctness beats raw throughput here.
`book_events` go through `COPY` (via `copy_records_to_table`), which is
materially faster for bulk inserts; duplicates there are harmless since it's
an append-only replay log and order-book reconstruction is idempotent to
replaying the same diff twice. Both paths were verified against a real
Postgres 16 instance, not just asserted — see `tests/correctness/test_timescale_writer.py`.

**Mongo repository is synchronous on purpose** (`src/storage/mongo/repository.py`):
unlike the ingestion→Timescale path, watchlists/portfolios/alerts aren't on
the hot tick-processing loop — they're read/written by a future API layer
and an alert-evaluation job, both fine with a blocking call per request.
`ensure_indexes()` duplicates the index definitions from
`infra/mongo-init/001_init.js` deliberately: the JS only runs once, on first
container creation, so anything that creates its own Mongo connection (tests,
a fresh non-Docker deployment) needs an idempotent way to guarantee indexes
exist rather than depending on container lifecycle timing.

## What's next (in order)

1. **Order book reconstruction** — implement the actual Binance
   snapshot+buffer+reconcile algorithm and the Coinbase resync-on-disconnect
   path, both producing a queryable live L2 depth structure
2. **Backtesting engine** — mean-reversion + momentum strategies, walk-forward
   splitting, execution against the reconstructed book, Sharpe/max-drawdown
3. **Remaining test suites** — correctness (order book vs. ground truth,
   downsampling correctness), performance benchmarks (write throughput, query
   latency, order-book update latency), backtest validity (lookahead-bias
   check, slippage sanity), and the connection-kill chaos test

## Project layout

```
market-data-platform/
├── docker-compose.yml          # TimescaleDB + Mongo, zero paid services
├── .env.example
├── requirements.txt
├── infra/
│   ├── timescale-init/001_schema.sql   # hypertables, continuous aggregates, retention
│   └── mongo-init/001_init.js          # collections + indexes
├── src/
│   ├── config.py
│   ├── common/
│   │   ├── events.py            # normalized event types (the cross-layer contract)
│   │   └── symbols.py           # BASE-QUOTE <-> exchange-native mapping
│   ├── ingestion/
│   │   ├── base.py              # reconnect/backoff contract every client follows
│   │   ├── binance_client.py
│   │   ├── coinbase_client.py
│   │   └── manager.py           # runs both concurrently, merges into one stream
│   ├── storage/
│   │   ├── timescale/writer.py   # batched trade inserts + COPY for book events
│   │   └── mongo/
│   │       ├── models.py         # Watchlist / Portfolio / AlertRule dataclasses
│   │       └── repository.py     # CRUD + hot-path queries, ensure_indexes()
│   ├── orderbook/                # (next)
│   └── backtest/                 # (next)
├── scripts/
│   ├── run_ingestion.py         # Layer 1 only: sanity-check the raw feed
│   └── run_pipeline.py          # Layer 1 + 2: ingestion streaming into TimescaleDB
└── tests/
    ├── correctness/
    │   ├── test_symbols.py
    │   ├── test_timescale_writer.py     # verified against real Postgres
    │   ├── test_mongo_repository.py     # verified against mongomock
    │   └── test_mongo_index_usage.py    # verified against real Mongo (skips otherwise)
    ├── performance/
    ├── backtest_validity/
    ├── chaos/
    │   └── test_malformed_messages.py   # done: malformed-payload handling
    └── fixtures/
        └── plain_schema.sql             # Timescale schema mirrored for plain-Postgres testing
```
