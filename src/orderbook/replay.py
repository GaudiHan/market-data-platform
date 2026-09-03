"""
Reconstructs an OrderBook as it looked at a specific point in the past, by
replaying the persisted book_events log (written by
src/storage/timescale/writer.py) up to a cutoff timestamp. This is what
lets Layer 4's execution simulator fill backtest trades against real
historical depth instead of a made-up slippage percentage.

Deliberately reuses OrderBook directly rather than going through the live
reconciler (src/orderbook/reconciler.py): reconciliation exists to handle
*uncertainty* about whether a live diff is safe to apply (still buffering,
mid-gap, etc.). Replaying already-persisted history has no such
uncertainty -- what's in the table is what was validated live, in order --
so replay is a straight sequential apply. The one thing replay must get
right that a live book doesn't have to worry about is the same principle
Layer 4's lookahead tests hammer on elsewhere: never apply an event whose
timestamp is after the requested cutoff.
"""
from __future__ import annotations

from datetime import datetime
from itertools import groupby

import asyncpg

from src.common.events import BookLevel, BookSide
from src.orderbook.book import OrderBook

_QUERY = """
    SELECT ts, event_type, side, price, size, first_update_id, last_update_id
    FROM book_events
    WHERE exchange = $1 AND symbol = $2 AND ts <= $3
    ORDER BY ts ASC
"""


class OrderBookReplayer:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def reconstruct(self, exchange: str, symbol: str, as_of: datetime) -> OrderBook:
        """Rebuild the book exactly as it stood at `as_of`. Events sharing
        the same (ts, event_type) came from a single snapshot/diff and are
        applied together -- see the writer's _buffer_book_levels, which
        stamps every level row from one event with the same ts."""
        book = OrderBook(exchange, symbol)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(_QUERY, exchange, symbol, as_of)

        for (_ts, event_type), group_iter in groupby(rows, key=lambda r: (r["ts"], r["event_type"])):
            group = list(group_iter)
            if event_type == "snapshot":
                book.clear()
                for row in group:
                    if row["size"] > 0:
                        side_map = book.bids if row["side"] == "bid" else book.asks
                        side_map[row["price"]] = row["size"]
                book.last_update_id = group[0]["last_update_id"]
            else:  # "diff"
                levels = tuple(
                    BookLevel(
                        price=row["price"], size=row["size"],
                        side=BookSide.BID if row["side"] == "bid" else BookSide.ASK,
                    )
                    for row in group
                )
                book.apply_levels(levels, update_id=group[-1]["last_update_id"])

        return book
