# Market Data Time-Series Platform

Ingests live crypto market data (Binance + Coinbase, both free/public
WebSocket feeds, no API key or paid tier of any kind), stores it in
TimescaleDB + MongoDB, reconstructs a live L2 order book, and backtests
mechanical strategies against it with realistic execution simulation.

Built to demonstrate database/schema design and market-microstructure
understanding, not "call an API and plot a line."

## Status: Complete -- all four layers, performance benchmarks, chaos test, real order-book execution wired into the backtest

| Layer | Status |
|---|---|
| 1. Ingestion (Binance + Coinbase WS) | Done, tested |
| 2. Storage (TimescaleDB + Mongo) | Done, tested — writers + repository wired up |
| 3. Order book reconstruction | Done, tested — reconciliation + gap/resync handling |
| 4. Backtesting engine | Done, tested — walk-forward, real order-book execution, risk-adjusted metrics |
| Performance benchmarks | Done — write throughput, query latency, order-book update latency |
| Chaos test: connection kill mid-stream | Done — real local socket, both exchanges |

Test coverage: **113 passed, 1 skipped** (114 total). The one skip needs
live Postgres/Mongo not installable in the sandbox these files were built
in (see below); it runs for real once you `docker compose up -d`.

## Zero-budget guarantee

Everything here runs on your own machine for $0:
- **TimescaleDB** and **MongoDB Community**: self-hosted via Docker, no cloud account
- **Binance** (`stream.binance.com`) and **Coinbase Exchange** (`ws-feed.exchange.coinbase.com`)
  public market-data WebSocket feeds: no API key, no account, no auth at all
  needed for trade/order-book data (only *trading* requires keys, which this
  project never does)

One correction worth knowing about, **caught via a web search, not live
testing** (this sandbox can't reach exchange domains, so I fact-checked
instead): Coinbase's plain `level2` channel has required a signed API key
since August 2023 — my original Layer 1 assumption that it was public was
stale/wrong. The fix isn't to drop Coinbase order-book support, though:
Coinbase documents `level2_batch` as an explicitly unauthenticated channel
with identical `snapshot`/`l2update` message shapes (just batched every
50ms server-side), so it's a one-line channel-name change with no parsing
differences. See the docstring in `coinbase_client.py` for the full story.

## Setup

```bash
cp .env.example .env          # defaults are fine for local use
docker compose up -d          # starts TimescaleDB + Mongo, runs init scripts
python -m venv .venv && source .venv/Scripts/activate
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

To see the actual reconstructed order books rather than raw events:

```bash
python -m scripts.run_orderbook
```

Prints top-of-book (best bid/ask, spread, sync status) for both exchanges
every 3 seconds. Watch the `[SYNCED]`/`[resyncing...]` tag — it should read
`resyncing...` only briefly, right after startup, before settling to
`SYNCED`.

To run a backtest (works immediately with zero setup via a free historical
data pull, or synthetic data as a last resort):

```bash
python -m scripts.backfill_binance_klines --symbol BTC-USD --interval 1h
python -m scripts.run_backtest --symbol BTC-USD --interval 1h
```

Prints walk-forward fold-by-fold results for both strategies: return,
Sharpe, max drawdown, and a buy-and-hold benchmark for comparison. It also
prints which execution mode it's using: real order-book replay if
`scripts/run_pipeline.py` has accumulated `book_events` history for that
exchange/symbol, or the documented flat-slippage fallback if not. Pass
`--no-order-book` to force the fallback regardless.

Run the test suite:

```bash
pytest -v
```

Most tests need no infra at all (parsing/handling logic, and Mongo
repository logic via `mongomock`). A few talk to real services and skip
cleanly if they're not reachable:
- `tests/correctness/test_timescale_writer.py`,
  `tests/correctness/test_orderbook_replay.py`, `tests/performance/test_write_throughput.py`,
  `tests/performance/test_query_latency.py` — need a scratch Postgres/Timescale
  database. Default DSN (`postgresql://mdp:mdp_local_pw@localhost:5432/marketdata_test`)
  matches this project's own `docker-compose.yml` credentials, so against
  the containers here you only need to create the scratch database once —
  it's a different database from your live `marketdata` one on purpose, so
  running tests never truncates real collected data:
  ```bash
  docker exec -it mdp_timescaledb createdb -U mdp marketdata_test
  ```
  Override with `TEST_POSTGRES_DSN` if you're running Postgres elsewhere or
  under different credentials.
- `tests/correctness/test_mongo_index_usage.py` — needs real MongoDB (see
  status table above for why). Defaults to matching `docker-compose`'s Mongo
  credentials directly — no setup step needed, it uses your live `marketdata`
  Mongo database's `alert_rules` collection but only reads from it (and
  writes to a separate `marketdata_index_test` DB it creates and drops
  itself, never touching your real data).

To see the actual benchmark numbers rather than just pass/fail (the perf
tests print, but pytest swallows stdout by default):

```bash
pytest tests/performance -v -s
```

## Architecture decisions worth knowing about

**Test defaults should match this project's own docker-compose, not a
generic assumption** — found via your test run, not mine: all four
Postgres-dependent test files defaulted `TEST_POSTGRES_DSN` to
`postgres:postgres`, a generic superuser guess that doesn't match this
project's actual `docker-compose.yml` credentials (`mdp`/`mdp_local_pw`).
Against your containers, every one of those tests silently skipped instead
of failing loudly — technically correct behavior (skip-if-unreachable is
the intended design so the suite doesn't require infra), but a wrong
default that produces 12 silent skips defeats the point of having the
tests. Fixed by changing the default to match this project's own compose
file exactly, so `docker compose up -d` plus one `createdb` call is enough
for the full suite to run for real with zero configuration.

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
`level2_batch` channel doesn't carry an equivalent sequence number — so gap
detection there is connection-level only (any disconnect, or an explicit
resync request, triggers a full resync). This is documented in
`coinbase_client.py` rather than papered over; `src/orderbook` treats "no
snapshot yet" and "sequence gap detected" as the same trigger so both
exchanges are handled correctly despite the asymmetry.

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

**Order book reconstruction is split into two layers on purpose**
(`src/orderbook/book.py` vs. `src/orderbook/reconciler.py`): `OrderBook`
is a dumb, exchange-agnostic price-level store (SortedDict per side,
O(log n) updates) that just applies levels — it has no idea what a
sequence gap is. All the "is this diff safe to apply, and what do we do if
it isn't" logic lives in the reconciler, one layer up, because the two
exchanges give genuinely different guarantees and that logic needs to
differ per exchange without the book itself caring:
- **Binance** diffs carry `U`/`u` (first/last update id), so
  `BinanceReconciler` implements the documented procedure exactly: buffer
  diffs while waiting for a REST snapshot, drop any diff already covered by
  the snapshot, require the first applied diff to bridge
  `U <= snapshot.lastUpdateId + 1 <= u`, and treat any later break in the
  `U == previous.u + 1` chain as a gap that clears the book and triggers a
  resync.
- **Coinbase**'s `level2_batch` channel carries no sequence number at all
  (see `coinbase_client.py`), so `CoinbaseReconciler` can't detect a gap
  from the data itself — correctness there means "apply the snapshot, then
  apply diffs in arrival order," leaning on connection-level resync (any
  disconnect, or an explicit request) rather than sequence-level detection.

**Resync doesn't require a full reconnect.** A Binance sequence gap can
happen on an otherwise-healthy WebSocket connection, so the order book
layer needed a way to ask ingestion for a fresh snapshot without tearing
the connection down. `OrderBookRegistry` calls a `resync_cb` that's wired
to `IngestionManager.request_resync()`, which dispatches to
`ExchangeClient.trigger_resync()` — a new method added to the Layer 1
`ExchangeClient` base class in this layer. Binance's implementation just
re-runs the same REST snapshot fetch used on initial connect; Coinbase's
re-sends a `level2_batch` subscribe message for the affected symbol, which
makes the server push a fresh snapshot.

One correction from Layer 1 surfaced while building this: the original
`coinbase_client.py` docstring claimed the plain `level2` channel was
public. That's been wrong since August 2023 — Coinbase now requires a
signed API key for it. A web search (this sandbox can't reach exchange
domains to test live) confirmed the fix: `level2_batch` is explicitly
documented as unauthenticated and sends identical `snapshot`/`l2update`
message shapes, so it was a one-line channel-name change, not a redesign.

**Snapshot-fetch ordering bug, found via your live test, not mine**: the
first version of `binance_client.py` fetched the REST snapshot *before*
opening the WebSocket depth stream. That's backwards from Binance's
documented procedure — you're supposed to start receiving diffs first, so
the ones buffered during the REST round-trip span across the snapshot's
`lastUpdateId` and can bridge into it. Fetching the snapshot first means
every diff received afterward has an update-id permanently ahead of
`lastUpdateId + 1`, so the bridge condition in `BinanceReconciler` can never
be satisfied — the book sits in "resyncing" forever, which is exactly what
running `scripts/run_orderbook.py` surfaced. Fixed by firing the snapshot
fetch as a background task *after* entering the WebSocket message loop
instead of before it. This is the kind of timing bug that's basically
invisible to unit tests (the reconciler logic itself was already correct
and already tested) and only shows up against a real, live feed — which is
exactly why I asked you to run it rather than trusting the test suite alone.

**Coinbase disconnect loop from oversized snapshot messages, also found via
your live test**: `websockets` closes the connection (code 1009) rather
than truncate when an incoming message exceeds its default 1MB limit. A
`level2_batch` snapshot for a liquid pair like BTC-USD carries thousands of
price levels in one JSON message and routinely exceeds that — so the
client was stuck looping connect → oversized snapshot → disconnect →
reconnect, forever. Fixed by raising `max_size` to 20MB on both exchange
clients' `websockets.connect()` calls (generous headroom, not unbounded).

**Lookahead bias is prevented by construction, not by discipline**
(`src/backtest/engine.py`, `strategies/base.py`): `BacktestEngine.run` always
calls `strategy.generate_signal(bars.iloc[:i+1])` — a slice ending at the
current bar, never the full frame. A strategy implementation literally
cannot read a future bar because it's never in the object it's holding.
`tests/backtest_validity/test_lookahead.py` verifies this the way that
actually matters: it corrupts every bar after a fixed decision point with
wildly different prices and confirms the signal at that point doesn't
change. That test would fail immediately if anyone ever passed the strategy
the whole `bars` frame instead of a truncated slice.

**Execution is simulated by walking the reconstructed order book, not a
flat slippage assumption** (`src/backtest/execution.py`): a market order
consumes resting size level by level from the best price outward, exactly
like a real exchange fills it — so slippage is a genuine output of book
depth, not an input the backtest assumes. `OrderBookReplayer`
(`src/orderbook/replay.py`) rebuilds historical book state from the
persisted `book_events` log up to a cutoff timestamp, deliberately never
touching the live reconciler (replaying already-validated history has no
sequencing uncertainty to resolve — that's what the reconciler exists for
on the live path). `scripts/run_backtest.py` auto-detects whether
`book_events` history exists for the chosen exchange/symbol and wires this
in automatically when it does, falling back to a documented flat-slippage
model per bar (see `FALLBACK_SLIPPAGE_BPS` in `engine.py`) when it
doesn't, rather than silently pretending the execution is realistic
when it isn't.

**Walk-forward splitting exists even though the strategies don't fit
parameters** (`src/backtest/walkforward.py`): the strategies here are
mechanical (fixed lookback/thresholds), so there's nothing to "train" in
the traditional sense. The splitter still matters because it's the
mechanism that enforces sequential, non-overlapping evaluation windows
rather than scoring a strategy against one big shuffled blob of history —
and it's ready to support real parameter search later (the interface
doesn't change) without needing a redesign.

**Fees are a fixed rate; slippage is not** (`src/backtest/costs.py` vs.
`execution.py`): a taker fee really is a fixed percentage of notional, so
that's modeled as a constant. Slippage is never modeled as a flat
percentage — it's whatever the order-book walk actually produces, because a
backtest that assumes a slippage number is just moving the same
optimism-bias problem one level down.

**Zero-cost historical data, so the backtest is runnable immediately**
(`scripts/backfill_binance_klines.py`): pulls OHLCV bars straight from
Binance's public REST klines endpoint — no key, no account, same
constraint as everything else in this project — into a local CSV that
`CsvBarsSource` reads. Without this, testing the backtest engine for real
(not synthetic data) would mean waiting days or weeks for live ingestion to
accumulate enough history.

**Performance benchmarks caught a real ~40x regression, not a hypothetical
one** (`src/orderbook/book.py`'s `depth()` and `total_size_within()`):
both called `list(sorted_dict.items())` before slicing/filtering, which
materializes the *entire* book before touching the part you actually
wanted — O(total levels) instead of O(log n + levels requested).
`SortedItemsView` supports direct slicing and `SortedDict.irange()` exists
specifically for this. `tests/performance/test_orderbook_latency.py`
measured `depth()` at ~200µs before the fix and ~5µs after, against a book
with 1000 levels/side — the kind of thing that's invisible in a
correctness test (both versions return the same answer) and only shows up
when you actually measure. The benchmark's ceiling was tightened
afterward specifically so the slow version would fail it if reintroduced.

**The connection-kill chaos test runs against a real local socket, not
mocked objects** (`tests/chaos/test_connection_kill.py`,
`test_connection_kill_coinbase.py`): a real `websockets.serve()` server and
a real `aiohttp` HTTP server stand in for the exchange, `BinanceClient`/
`CoinbaseClient` connect to them for real over localhost, and the fake
server abruptly kills the connection with a policy-violation close code
(1011) mid-stream — the closest a portable automated test gets to "yanked
the network cable." The test then proves the specific claim the assignment
asks for, not just "it reconnected": a large, unmistakable price jump is
baked into the *second* snapshot fetch (simulating the market having moved
during the outage), and the test asserts the book afterward contains
**only** that new price regime, with the pre-kill price level completely
absent — not overwritten, not lingering alongside the new state, gone.
That's the concrete meaning of "without corrupting downstream data" here.

Building this test surfaced a real bug in `src/ingestion/base.py`, found
before any live network was involved: `websockets`' async iterator returns
*normally* (no exception) on a clean WebSocket close (code 1000/1001) —
only abnormal closes raise. `run_forever` only emitted its `disconnected`
ConnectionEvent from the exception branch, so a graceful server-side close
would leave the order book layer believing nothing happened, silently
serving stale state as "synced" until a snapshot happened to arrive.
Fixed by emitting the event on both exit paths — caught by a fast,
network-free unit test (`tests/chaos/test_reconnect_base.py`) before the
slower real-socket test ever ran, which is exactly the point of having
both: the unit tests pin down `run_forever`'s own logic precisely and
cheaply, the real-socket tests prove the whole stack recovers end-to-end.

**Position sizing defaults to a fraction of current equity, not a fixed
unit count** (`src/backtest/engine.py`): the original default (`fixed_units`,
1.0 unit) meant a $10,000 account trading BTC at ~$100,000/unit was
implicitly ~10x leveraged from the first trade — which is exactly what
produced the unrealistic ±16% swings and 40% drawdowns seen in an early
live run. The default `sizing_mode="equity_fraction"` instead targets
`current_equity * position_fraction` notional exposure, so risk scales with
account size and instrument price rather than being an arbitrary constant
that happens to work at some price scales and not others.
`sizing_mode="fixed_units"` is still available for cases where identical
notional exposure across runs is genuinely what you want to compare — it's
demoted from silent default to an explicit opt-in, not removed.

**Order-book execution in the backtest is now real, auto-detected, and
gracefully degrading** (`scripts/run_backtest.py`, `src/orderbook/replay.py`):
the script checks whether TimescaleDB has any `book_events` rows for the
chosen exchange/symbol; if so, it wires `OrderBookReplayer.reconstruct` in
as `BacktestEngine`'s `order_book_provider` and trades execute against real
historical depth. If not (a fresh setup, or `--no-order-book`), it falls
back to the documented flat-slippage model with no crash and no silent
wrong-mode surprise — the script prints which mode it's using either way.
This required making `BacktestEngine.run` itself `async` (previously
synchronous), since `OrderBookReplayer.reconstruct` is a real Postgres
query via `asyncpg` — there's no clean way to call that from inside a
sync function without either a second, redundant sync DB driver or a
nested-event-loop hack, and this codebase is async-first everywhere else
already. The change is backward compatible with synchronous providers too
(`inspect.isawaitable` gates whether the result gets `await`ed), so
`None` (no provider) and any future non-DB-backed provider still work
unmodified.

## What's next (optional polish)

Everything from the original scope, performance benchmarks, both chaos-test
categories, real order-book backtest execution, and the position-sizing fix
are complete. What's left is genuinely optional:

1. **A thin API/CLI layer** over the Mongo repository (watchlists/alerts)
   and the backtest engine, if you want this to be demoable end-to-end
   rather than script-by-script.
2. **More strategies / multi-exchange signals** (e.g. a cross-exchange
   arbitrage detector comparing Binance vs. Coinbase top-of-book, which the
   existing `OrderBookRegistry` already tracks both exchanges for) if you
   want to extend the market-microstructure story further.

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
│   │   ├── base.py              # reconnect/backoff contract + trigger_resync() hook
│   │   ├── binance_client.py
│   │   ├── coinbase_client.py   # uses level2_batch (unauthenticated) for L2 depth
│   │   └── manager.py           # runs both concurrently, merges into one stream, request_resync()
│   ├── storage/
│   │   ├── timescale/writer.py   # batched trade inserts + COPY for book events
│   │   └── mongo/
│   │       ├── models.py         # Watchlist / Portfolio / AlertRule dataclasses
│   │       └── repository.py     # CRUD + hot-path queries, ensure_indexes()
│   ├── orderbook/
│   │   ├── book.py               # exchange-agnostic L2 price-level store
│   │   ├── reconciler.py         # per-exchange gap/out-of-order handling + resync trigger
│   │   ├── manager.py            # routes ingestion events to per (exchange,symbol) reconcilers
│   │   └── replay.py             # rebuilds historical book state from book_events for backtesting
│   └── backtest/
│       ├── strategies/
│       │   ├── base.py           # Strategy ABC -- lookahead prevented by construction
│       │   ├── mean_reversion.py
│       │   └── momentum.py
│       ├── data.py               # TimescaleBarsSource / CsvBarsSource / synthetic generator
│       ├── costs.py              # fixed-rate taker fee model
│       ├── execution.py          # fills market orders by walking a reconstructed order book
│       ├── walkforward.py        # sequential, non-overlapping train/test fold splitting
│       ├── metrics.py            # Sharpe, max drawdown, buy-and-hold benchmark
│       └── engine.py             # orchestrates signal -> execution -> fees -> metrics
├── scripts/
│   ├── run_ingestion.py            # Layer 1 only: sanity-check the raw feed
│   ├── run_pipeline.py             # Layer 1 + 2: ingestion streaming into TimescaleDB
│   ├── run_orderbook.py            # Layer 1 + 3: live top-of-book printer
│   ├── backfill_binance_klines.py  # free historical OHLCV, no key/account needed
│   └── run_backtest.py             # Layer 4: walk-forward backtest over both strategies
└── tests/
    ├── correctness/
    │   ├── test_symbols.py
    │   ├── test_timescale_writer.py         # verified against real Postgres
    │   ├── test_mongo_repository.py         # verified against mongomock
    │   ├── test_mongo_index_usage.py        # verified against real Mongo (skips otherwise)
    │   ├── test_orderbook.py                # order book vs. ground truth
    │   ├── test_orderbook_reconciliation.py # out-of-order/gap handling, both exchanges
    │   ├── test_orderbook_replay.py         # historical replay vs. ground truth (real Postgres)
    │   └── test_strategies.py               # strategy signal logic
    ├── performance/
    │   ├── _latency.py                    # shared measure()/measure_async() helper
    │   ├── test_orderbook_latency.py      # apply_level, reconciled diff, depth() latency
    │   ├── test_write_throughput.py       # trade (ON CONFLICT) vs. book_event (COPY) writes
    │   └── test_query_latency.py          # indexed vs. unindexed query, EXPLAIN-verified
    ├── backtest_validity/
    │   ├── test_lookahead.py              # corrupts future bars, confirms signal doesn't change
    │   ├── test_position_sizing.py        # equity_fraction default, fixed_units opt-in, leverage regression
    │   ├── test_walkforward_split.py      # fold sequencing/non-overlap validation
    │   ├── test_execution_realism.py      # order-book-walk fills, partial fills, shortfalls
    │   ├── test_slippage_fees.py          # fee proportionality, slippage vs. book depth
    │   └── test_benchmark_comparison.py   # buy-and-hold baseline + Sharpe/drawdown correctness
    ├── chaos/
    │   ├── test_malformed_messages.py       # malformed-payload handling (Layer 1)
    │   ├── test_reconnect_base.py           # fast, network-free: run_forever's reconnect/backoff logic
    │   ├── test_connection_kill.py          # real local socket: Binance kill + resync end-to-end
    │   └── test_connection_kill_coinbase.py # same, for Coinbase's no-sequence-number resync path
    └── fixtures/
        └── plain_schema.sql             # Timescale schema mirrored for plain-Postgres testing
```
