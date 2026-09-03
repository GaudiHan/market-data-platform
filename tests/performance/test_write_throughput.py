"""
Write throughput: how many trades/book-event rows per second can
TimescaleWriter sustain against a real database? This directly measures the
two write strategies documented in src/storage/timescale/writer.py --
batched INSERT..ON CONFLICT for trades (dedup costs something) vs. COPY for
book_events (no dedup, should be visibly faster) -- so the throughput
numbers here are also a check that the trade-off described in that
docstring is real and not just asserted.

Skips cleanly if no local Postgres is reachable, same pattern as
tests/correctness/test_timescale_writer.py.
"""
import os

import asyncpg
import pytest

from src.common.events import BookDiff, BookLevel, BookSide, Side, Trade
from src.storage.timescale.writer import TimescaleWriter
from tests.performance._latency import measure_async

TEST_DSN = os.getenv(
    "TEST_POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/marketdata_test"
)
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", "plain_schema.sql")

# Generous floor, not a target -- this sandbox, your laptop, and a CI runner
# will all report very different absolute numbers. This just catches "the
# writer got dramatically slower," e.g. from an accidentally per-row commit.
MIN_ACCEPTABLE_THROUGHPUT_PER_SEC = 200.0


async def _postgres_available() -> bool:
    try:
        conn = await asyncpg.connect(TEST_DSN)
        await conn.close()
        return True
    except Exception:
        return False


@pytest.fixture
async def pg_pool():
    if not await _postgres_available():
        pytest.skip(f"no Postgres reachable at {TEST_DSN}")

    conn = await asyncpg.connect(TEST_DSN)
    with open(SCHEMA_PATH) as f:
        await conn.execute(f.read())
    await conn.close()

    yield

    conn = await asyncpg.connect(TEST_DSN)
    await conn.execute("TRUNCATE trades, book_events")
    await conn.close()


@pytest.mark.asyncio
async def test_trade_write_throughput_on_conflict_dedup_path(pg_pool):
    writer = TimescaleWriter(TEST_DSN, batch_size=500, flush_interval_s=999)
    await writer.connect()
    counter = [0]

    async def write_one_trade():
        counter[0] += 1
        i = counter[0]
        await writer.handle_event(Trade(
            ts_ns=1_700_000_000_000_000_000 + i, exchange="binance", symbol="BTC-USD",
            trade_id=str(i), price=50000.0 + (i % 100), size=0.01, side=Side.BUY,
        ))

    stats = await measure_async(write_one_trade, n=5_000)
    await writer.flush()
    await writer.close()

    print(f"\n[timescale] trade writes (batched INSERT ON CONFLICT): {stats}")
    assert stats.throughput_per_sec > MIN_ACCEPTABLE_THROUGHPUT_PER_SEC
    assert writer.stats["trades_written"] == 5_000


@pytest.mark.asyncio
async def test_book_event_write_throughput_copy_path(pg_pool):
    writer = TimescaleWriter(TEST_DSN, batch_size=500, flush_interval_s=999)
    await writer.connect()
    counter = [0]

    async def write_one_diff():
        counter[0] += 1
        i = counter[0]
        await writer.handle_event(BookDiff(
            ts_ns=1_700_000_000_000_000_000 + i, exchange="binance", symbol="BTC-USD",
            levels=(BookLevel(price=50000.0 + (i % 100) * 0.01, size=1.0, side=BookSide.BID),),
            first_update_id=i, last_update_id=i,
        ))

    stats = await measure_async(write_one_diff, n=5_000)
    await writer.flush()
    await writer.close()

    print(f"[timescale] book event writes (COPY): {stats}")
    assert stats.throughput_per_sec > MIN_ACCEPTABLE_THROUGHPUT_PER_SEC
    assert writer.stats["book_rows_written"] == 5_000
