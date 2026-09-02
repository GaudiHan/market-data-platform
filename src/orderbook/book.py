"""
The order book itself, deliberately exchange-agnostic: it knows nothing
about Binance's update-id sequencing or Coinbase's lack thereof. It just
holds price -> size per side and applies levels. All the "is this diff safe
to apply / did we miss one" logic lives in reconciler.py, one layer up --
that separation is what makes both exchanges' very different guarantees
plug into the same book implementation.

Uses SortedDict (price -> size) per side rather than plain dicts so best
bid/ask and top-N depth are O(log n) to update and O(k) to read, not an
O(n log n) sort on every access -- this matters once you're applying
hundreds of diffs per second.
"""
from __future__ import annotations

from sortedcontainers import SortedDict

from src.common.events import BookLevel, BookSide, BookSnapshot


class OrderBook:
    def __init__(self, exchange: str, symbol: str):
        self.exchange = exchange
        self.symbol = symbol
        # Both stored ascending by price. Best bid = highest price = last key.
        # Best ask = lowest price = first key. SortedDict keeps this cheap.
        self.bids: SortedDict[float, float] = SortedDict()
        self.asks: SortedDict[float, float] = SortedDict()
        self.last_update_id: int | None = None

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None

    def apply_snapshot(self, snapshot: BookSnapshot) -> None:
        self.clear()
        for level in snapshot.bids:
            if level.size > 0:
                self.bids[level.price] = level.size
        for level in snapshot.asks:
            if level.size > 0:
                self.asks[level.price] = level.size
        self.last_update_id = snapshot.last_update_id

    def apply_level(self, level: BookLevel) -> None:
        """A size of exactly 0 means "remove this price level" -- that's the
        wire convention both exchanges use for diffs, so it's the convention
        here too rather than translating it away earlier."""
        side_map = self.bids if level.side == BookSide.BID else self.asks
        if level.size == 0:
            side_map.pop(level.price, None)
        else:
            side_map[level.price] = level.size

    def apply_levels(self, levels: tuple[BookLevel, ...], update_id: int | None = None) -> None:
        for level in levels:
            self.apply_level(level)
        if update_id is not None:
            self.last_update_id = update_id

    def best_bid(self) -> tuple[float, float] | None:
        return self.bids.peekitem(-1) if self.bids else None

    def best_ask(self) -> tuple[float, float] | None:
        return self.asks.peekitem(0) if self.asks else None

    def spread(self) -> float | None:
        bb, ba = self.best_bid(), self.best_ask()
        return (ba[0] - bb[0]) if (bb and ba) else None

    def mid_price(self) -> float | None:
        bb, ba = self.best_bid(), self.best_ask()
        return (ba[0] + bb[0]) / 2 if (bb and ba) else None

    def depth(self, n: int = 10) -> dict:
        """Top n levels each side, best price first."""
        bid_items = list(self.bids.items())[-n:][::-1]  # highest prices first
        ask_items = list(self.asks.items())[:n]           # lowest prices first
        return {"bids": bid_items, "asks": ask_items}

    def total_size_within(self, side: BookSide, price_from: float, price_to: float) -> float:
        """Sum of resting size between two prices inclusive -- used by the
        backtest execution simulator (Layer 4) to estimate fillable liquidity
        for a given order size without walking the whole book by hand."""
        book = self.bids if side == BookSide.BID else self.asks
        lo, hi = min(price_from, price_to), max(price_from, price_to)
        return sum(size for price, size in book.items() if lo <= price <= hi)

    def __repr__(self) -> str:
        return (
            f"OrderBook({self.exchange}:{self.symbol}, "
            f"best_bid={self.best_bid()}, best_ask={self.best_ask()}, "
            f"levels={len(self.bids)}b/{len(self.asks)}a)"
        )
