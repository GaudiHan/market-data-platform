"""
Integration test for OrderBookReplayer against a real Postgres (same
scratch DB and schema-mirror pattern as test_timescale_writer.py). Verifies
both correctness (reconstructed state matches ground truth) and the
no-lookahead property that matters for backtest execution: events after the
cutoff timestamp must never be applied.
"""
import os
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from src.common.events import BookDiff, BookLevel, BookSide, BookSnapshot
from src.orderbook.replay import OrderBookReplayer
from src.storage.timescale.writer import TimescaleWriter

TEST_DSN = os.getenv(
    "TEST_POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/marketdata_test"
)
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", "plain_schema.sql")

T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)


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


def _ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000_000)


async def _seed(writer: TimescaleWriter):
    """Snapshot at T0, then three diffs at T0+1s, T0+2s, T0+3s."""
    await writer.handle_event(BookSnapshot(
        ts_ns=_ns(T0), exchange="binance", symbol="BTC-USD",
        bids=(BookLevel(price=100.0, size=1.0, side=BookSide.BID),),
        asks=(BookLevel(price=101.0, size=1.0, side=BookSide.ASK),),
        last_update_id=10,
    ))
    await writer.handle_event(BookDiff(
        ts_ns=_ns(T0 + timedelta(seconds=1)), exchange="binance", symbol="BTC-USD",
        levels=(BookLevel(price=100.0, size=2.0, side=BookSide.BID),),  # size update
        first_update_id=11, last_update_id=11,
    ))
    await writer.handle_event(BookDiff(
        ts_ns=_ns(T0 + timedelta(seconds=2)), exchange="binance", symbol="BTC-USD",
        levels=(BookLevel(price=99.0, size=5.0, side=BookSide.BID),),  # new level added
        first_update_id=12, last_update_id=12,
    ))
    await writer.handle_event(BookDiff(
        ts_ns=_ns(T0 + timedelta(seconds=3)), exchange="binance", symbol="BTC-USD",
        levels=(BookLevel(price=100.0, size=0.0, side=BookSide.BID),),  # level removed
        first_update_id=13, last_update_id=13,
    ))
    await writer.flush()


@pytest.mark.asyncio
async def test_replay_reconstructs_state_at_each_point_correctly(pg_pool):
    writer = TimescaleWriter(TEST_DSN, batch_size=100, flush_interval_s=999)
    await writer.connect()
    await _seed(writer)
    await writer.close()

    pool = await asyncpg.create_pool(TEST_DSN)
    replayer = OrderBookReplayer(pool)

    # As of T0 exactly: only the snapshot applies.
    book_t0 = await replayer.reconstruct("binance", "BTC-USD", T0)
    assert book_t0.bids[100.0] == 1.0
    assert 99.0 not in book_t0.bids

    # As of T0+1s: the size-update diff has applied.
    book_t1 = await replayer.reconstruct("binance", "BTC-USD", T0 + timedelta(seconds=1))
    assert book_t1.bids[100.0] == 2.0

    # As of T0+2s: the new level has been added, on top of the size update.
    book_t2 = await replayer.reconstruct("binance", "BTC-USD", T0 + timedelta(seconds=2))
    assert book_t2.bids[100.0] == 2.0
    assert book_t2.bids[99.0] == 5.0

    # As of T0+3s: the 100.0 level has been removed entirely.
    book_t3 = await replayer.reconstruct("binance", "BTC-USD", T0 + timedelta(seconds=3))
    assert 100.0 not in book_t3.bids
    assert book_t3.bids[99.0] == 5.0

    await pool.close()


@pytest.mark.asyncio
async def test_replay_never_applies_events_after_cutoff(pg_pool):
    """The no-lookahead property that matters for backtest execution: asking
    for the book as of T0+1.5s must reflect ONLY the T0 snapshot and the
    T0+1s diff -- never the T0+2s or T0+3s diffs, even though they're sitting
    right there in the table."""
    writer = TimescaleWriter(TEST_DSN, batch_size=100, flush_interval_s=999)
    await writer.connect()
    await _seed(writer)
    await writer.close()

    pool = await asyncpg.create_pool(TEST_DSN)
    replayer = OrderBookReplayer(pool)

    book = await replayer.reconstruct("binance", "BTC-USD", T0 + timedelta(seconds=1, milliseconds=500))

    assert book.bids[100.0] == 2.0    # T0+1s diff applied
    assert 99.0 not in book.bids       # T0+2s diff must NOT be applied
    assert book.last_update_id == 11   # not 12 or 13

    await pool.close()


@pytest.mark.asyncio
async def test_replay_with_no_data_before_cutoff_returns_empty_book(pg_pool):
    writer = TimescaleWriter(TEST_DSN, batch_size=100, flush_interval_s=999)
    await writer.connect()
    await _seed(writer)
    await writer.close()

    pool = await asyncpg.create_pool(TEST_DSN)
    replayer = OrderBookReplayer(pool)

    book = await replayer.reconstruct("binance", "BTC-USD", T0 - timedelta(seconds=1))

    assert book.best_bid() is None
    assert book.best_ask() is None

    await pool.close()
