"""
Consumes normalized events from the ingestion queue and writes them to
TimescaleDB. Two deliberately different write strategies for two different
tables, because they have different correctness requirements:

- `trades`: written via batched INSERT ... ON CONFLICT (...) DO NOTHING.
  Exchanges redeliver trades on reconnect, so writes must be idempotent --
  the unique index on (exchange, symbol, trade_id, ts) is what makes that
  safe. This costs some throughput vs. a raw COPY, which is the right
  trade-off here: silently duplicating trade history would corrupt every
  downstream bar/backtest computed from it.

- `book_events`: written via COPY (asyncpg's copy_records_to_table), which
  is materially faster for bulk inserts. This table is an append-only replay
  log with no uniqueness requirement -- an occasional duplicate diff row on
  reconnect is harmless (the order-book reconstruction layer is idempotent
  to replaying the same diff twice), so there's no reason to pay the
  ON CONFLICT cost here.

Batching: events are buffered in memory and flushed when either the batch
size or a time interval is reached, whichever comes first -- standard
throughput/latency trade-off knob, exposed as constructor args so the
performance-benchmark suite can sweep it later.
"""
from __future__ import annotations

import asyncio
import logging
import time

import asyncpg

from src.common.events import BookDiff, BookSnapshot, NormalizedEvent, Trade

logger = logging.getLogger(__name__)

_TRADE_INSERT_SQL = """
    INSERT INTO trades (ts, exchange, symbol, trade_id, price, size, side)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (exchange, symbol, trade_id, ts) DO NOTHING
"""

_BOOK_EVENT_COLUMNS = (
    "ts", "exchange", "symbol", "event_type", "side",
    "price", "size", "first_update_id", "last_update_id",
)


class TimescaleWriter:
    def __init__(
        self,
        dsn: str,
        batch_size: int = 500,
        flush_interval_s: float = 2.0,
    ):
        self.dsn = dsn
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s

        self.pool: asyncpg.Pool | None = None
        self._trade_buffer: list[tuple] = []
        self._book_event_buffer: list[tuple] = []
        self._last_flush = time.monotonic()

        # Simple counters -- the performance-benchmark suite reads these
        # rather than needing to instrument the DB separately.
        self.stats = {"trades_written": 0, "book_rows_written": 0, "flushes": 0}

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)

    async def close(self) -> None:
        await self.flush()
        if self.pool is not None:
            await self.pool.close()

    async def handle_event(self, event: NormalizedEvent) -> None:
        """Route one normalized event into the right buffer. ConnectionEvents
        are intentionally not persisted here -- they're operational signal
        for the ingestion/order-book layers, not market data."""
        if isinstance(event, Trade):
            self._trade_buffer.append(
                (_ns_to_datetime(event.ts_ns), event.exchange, event.symbol,
                 event.trade_id, event.price, event.size, event.side.value)
            )
        elif isinstance(event, BookSnapshot):
            self._buffer_book_levels(event, event_type="snapshot")
        elif isinstance(event, BookDiff):
            self._buffer_book_levels(event, event_type="diff")

        await self._maybe_flush()

    def _buffer_book_levels(self, event: BookSnapshot | BookDiff, event_type: str) -> None:
        ts = _ns_to_datetime(event.ts_ns)
        if isinstance(event, BookSnapshot):
            # A snapshot IS the state as of a single update id -- first and
            # last are the same value (there's no "range" for a snapshot).
            first_uid = event.last_update_id
            last_uid = event.last_update_id
            levels = (*event.bids, *event.asks)
        else:
            first_uid = event.first_update_id
            last_uid = event.last_update_id
            levels = event.levels
        for level in levels:
            self._book_event_buffer.append((
                ts, event.exchange, event.symbol, event_type, level.side.value,
                level.price, level.size, first_uid, last_uid,
            ))

    async def _maybe_flush(self) -> None:
        should_flush = (
            len(self._trade_buffer) >= self.batch_size
            or len(self._book_event_buffer) >= self.batch_size
            or (time.monotonic() - self._last_flush) >= self.flush_interval_s
        )
        if should_flush:
            await self.flush()

    async def flush(self) -> None:
        if not self._trade_buffer and not self._book_event_buffer:
            self._last_flush = time.monotonic()
            return

        if self.pool is None:
            raise RuntimeError("TimescaleWriter.connect() must be called before flush()")

        async with self.pool.acquire() as conn:
            if self._trade_buffer:
                await conn.executemany(_TRADE_INSERT_SQL, self._trade_buffer)
                self.stats["trades_written"] += len(self._trade_buffer)
                self._trade_buffer = []

            if self._book_event_buffer:
                await conn.copy_records_to_table(
                    "book_events",
                    records=self._book_event_buffer,
                    columns=_BOOK_EVENT_COLUMNS,
                )
                self.stats["book_rows_written"] += len(self._book_event_buffer)
                self._book_event_buffer = []

        self.stats["flushes"] += 1
        self._last_flush = time.monotonic()

    async def run(self, event_source) -> None:
        """Consume an async iterable of NormalizedEvent (e.g.
        IngestionManager.events()) until cancelled, flushing periodically
        even if no event arrives to trigger a size-based flush."""
        async def periodic_flush():
            while True:
                await asyncio.sleep(self.flush_interval_s)
                await self.flush()

        flush_task = asyncio.create_task(periodic_flush())
        try:
            async for event in event_source:
                await self.handle_event(event)
        finally:
            flush_task.cancel()
            await asyncio.gather(flush_task, return_exceptions=True)
            await self.flush()


def _ns_to_datetime(ts_ns: int):
    import datetime
    return datetime.datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=datetime.timezone.utc)
