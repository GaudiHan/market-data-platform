"""
Integration test for TimescaleWriter against a real running Postgres
(schema mirrored without the timescaledb extension -- see
tests/fixtures/plain_schema.sql). Skips cleanly if no local Postgres is
reachable, so the rest of the suite stays runnable without infra.

To run this locally: `docker compose up -d` (or any local Postgres) then
set TEST_POSTGRES_DSN, or rely on the default which matches docker-compose's
defaults from .env.example.
"""
import os

import asyncpg
import pytest

from src.common.events import BookDiff, BookLevel, BookSide, BookSnapshot, Side, Trade
from src.storage.timescale.writer import TimescaleWriter

TEST_DSN = os.getenv(
    "TEST_POSTGRES_DSN", "postgresql://mdp:mdp_local_pw@localhost:5432/marketdata_test"
)
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", "plain_schema.sql")


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
async def test_trade_write_and_dedup(pg_pool):
    writer = TimescaleWriter(TEST_DSN, batch_size=100, flush_interval_s=999)
    await writer.connect()

    trade = Trade(
        ts_ns=1_700_000_000_000_000_000,
        exchange="binance", symbol="BTC-USD", trade_id="abc123",
        price=50000.0, size=0.01, side=Side.BUY,
    )
    await writer.handle_event(trade)
    # Same trade redelivered (e.g. after a reconnect) -- must not duplicate.
    await writer.handle_event(trade)
    await writer.flush()

    conn = await asyncpg.connect(TEST_DSN)
    rows = await conn.fetch("SELECT * FROM trades")
    await conn.close()
    await writer.close()

    assert len(rows) == 1, "duplicate trade_id must be deduplicated via ON CONFLICT"
    assert rows[0]["symbol"] == "BTC-USD"
    assert rows[0]["price"] == 50000.0


@pytest.mark.asyncio
async def test_trade_batch_flush_triggers_on_size(pg_pool):
    writer = TimescaleWriter(TEST_DSN, batch_size=5, flush_interval_s=999)
    await writer.connect()

    for i in range(5):
        trade = Trade(
            ts_ns=1_700_000_000_000_000_000 + i,
            exchange="binance", symbol="ETH-USD", trade_id=str(i),
            price=3000.0 + i, size=1.0, side=Side.SELL,
        )
        await writer.handle_event(trade)

    # No explicit flush() call -- reaching batch_size should have flushed already.
    conn = await asyncpg.connect(TEST_DSN)
    rows = await conn.fetch("SELECT * FROM trades WHERE symbol = 'ETH-USD'")
    await conn.close()
    await writer.close()

    assert len(rows) == 5
    assert writer.stats["trades_written"] == 5


@pytest.mark.asyncio
async def test_book_snapshot_writes_one_row_per_level(pg_pool):
    writer = TimescaleWriter(TEST_DSN, batch_size=100, flush_interval_s=999)
    await writer.connect()

    snapshot = BookSnapshot(
        ts_ns=1_700_000_000_000_000_000,
        exchange="binance", symbol="BTC-USD",
        bids=(
            BookLevel(price=49999.0, size=1.0, side=BookSide.BID),
            BookLevel(price=49998.0, size=2.0, side=BookSide.BID),
        ),
        asks=(
            BookLevel(price=50001.0, size=1.5, side=BookSide.ASK),
        ),
        last_update_id=42,
    )
    await writer.handle_event(snapshot)
    await writer.flush()

    conn = await asyncpg.connect(TEST_DSN)
    rows = await conn.fetch(
        "SELECT * FROM book_events WHERE event_type = 'snapshot' ORDER BY price"
    )
    await conn.close()
    await writer.close()

    assert len(rows) == 3
    assert all(r["first_update_id"] == 42 and r["last_update_id"] == 42 for r in rows)
    bid_rows = [r for r in rows if r["side"] == "bid"]
    ask_rows = [r for r in rows if r["side"] == "ask"]
    assert len(bid_rows) == 2
    assert len(ask_rows) == 1


@pytest.mark.asyncio
async def test_book_diff_preserves_update_id_range(pg_pool):
    writer = TimescaleWriter(TEST_DSN, batch_size=100, flush_interval_s=999)
    await writer.connect()

    diff = BookDiff(
        ts_ns=1_700_000_000_000_000_000,
        exchange="binance", symbol="BTC-USD",
        levels=(BookLevel(price=50000.0, size=0.0, side=BookSide.BID),),  # size 0 = remove level
        first_update_id=100,
        last_update_id=105,
    )
    await writer.handle_event(diff)
    await writer.flush()

    conn = await asyncpg.connect(TEST_DSN)
    row = await conn.fetchrow("SELECT * FROM book_events WHERE event_type = 'diff'")
    await conn.close()
    await writer.close()

    assert row["first_update_id"] == 100
    assert row["last_update_id"] == 105
    assert row["size"] == 0.0
