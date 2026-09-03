"""
Query latency: the point of the (exchange, symbol, ts DESC) index on
`trades` (see infra/timescale-init/001_schema.sql) is that "get recent
trades for a symbol" stays fast as the table grows. This seeds a realistic
number of rows across multiple symbols and benchmarks exactly the query
shape that index was built for -- plus, as a negative control, a query
shape the index does NOT cover well, to make the difference concrete rather
than just asserted in a docstring.
"""
import datetime
import json
import os
import random

import asyncpg
import pytest

from tests.performance._latency import measure_async

TEST_DSN = os.getenv(
    "TEST_POSTGRES_DSN", "postgresql://mdp:mdp_local_pw@localhost:5432/marketdata_test"
)
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", "plain_schema.sql")

MAX_ACCEPTABLE_P95_MS = 50.0  # generous ceiling, see module docstring in _latency.py
N_ROWS_PER_SYMBOL = 5_000
SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD"]


async def _postgres_available() -> bool:
    try:
        conn = await asyncpg.connect(TEST_DSN)
        await conn.close()
        return True
    except Exception:
        return False


@pytest.fixture
async def seeded_pool():
    if not await _postgres_available():
        pytest.skip(f"no Postgres reachable at {TEST_DSN}")

    conn = await asyncpg.connect(TEST_DSN)
    with open(SCHEMA_PATH) as f:
        await conn.execute(f.read())

    rng = random.Random(42)
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    records = []
    i = 0
    for symbol in SYMBOLS:
        for j in range(N_ROWS_PER_SYMBOL):
            records.append((
                base + datetime.timedelta(seconds=i), "binance", symbol, f"{symbol}-{j}",
                50000.0 + rng.uniform(-100, 100), rng.uniform(0.001, 1.0),
                "buy" if j % 2 == 0 else "sell",
            ))
            i += 1

    await conn.executemany(
        "INSERT INTO trades (ts, exchange, symbol, trade_id, price, size, side) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7)",
        records,
    )
    await conn.close()

    pool = await asyncpg.create_pool(TEST_DSN, min_size=1, max_size=5)
    yield pool
    await pool.close()

    conn = await asyncpg.connect(TEST_DSN)
    await conn.execute("TRUNCATE trades, book_events")
    await conn.close()


@pytest.mark.asyncio
async def test_recent_trades_for_symbol_query_latency(seeded_pool):
    """The query the (exchange, symbol, ts DESC) index exists for."""
    async def query():
        async with seeded_pool.acquire() as conn:
            await conn.fetch(
                "SELECT * FROM trades WHERE exchange = $1 AND symbol = $2 "
                "ORDER BY ts DESC LIMIT 100",
                "binance", "ETH-USD",
            )

    stats = await measure_async(query, n=200)
    print(f"\n[timescale] recent-trades-for-symbol query (indexed): {stats}")
    assert stats.p95_us / 1000 < MAX_ACCEPTABLE_P95_MS


@pytest.mark.asyncio
async def test_price_range_scan_query_latency_unindexed_column(seeded_pool):
    """Negative control: filtering on `price`, which has no index. Prints
    for comparison -- it's expected (and fine) for this to be slower than
    the indexed query above; this test documents that gap rather than
    hiding it, and only fails if it's absurdly slow."""
    async def query():
        async with seeded_pool.acquire() as conn:
            await conn.fetch(
                "SELECT * FROM trades WHERE price > $1 AND price < $2 LIMIT 100",
                50050.0, 50060.0,
            )

    stats = await measure_async(query, n=100)
    print(f"[timescale] price-range query (NOT indexed, for comparison): {stats}")
    assert stats.p95_us / 1000 < MAX_ACCEPTABLE_P95_MS * 4  # looser ceiling -- this one's *expected* to be slower


@pytest.mark.asyncio
async def test_recent_trades_query_uses_the_index(seeded_pool):
    """Don't just assert it's fast -- confirm via EXPLAIN that the indexed
    query path is actually hitting the index, the same pattern used for the
    Mongo compound-index check in Layer 2."""
    async with seeded_pool.acquire() as conn:
        plan = await conn.fetchval(
            "EXPLAIN (FORMAT JSON) SELECT * FROM trades WHERE exchange = $1 AND symbol = $2 "
            "ORDER BY ts DESC LIMIT 100",
            "binance", "BTC-USD",
        )
    plan_json = json.loads(plan)
    plan_text = json.dumps(plan_json)
    assert "Index" in plan_text, f"expected an index scan in the plan, got: {plan_text}"
