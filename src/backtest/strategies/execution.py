"""
This is the "execution simulated against your reconstructed order book"
requirement. A naive backtest fills every trade at the bar's close price
plus a made-up slippage percentage; that's not realistic and it isn't
using the order book Layer 3 built at all. Here, filling a market order
means actually walking price levels outward from the best price, consuming
resting size level by level, until the requested quantity is filled or the
book runs out of liquidity -- exactly what happens on a real exchange.

This operates on any OrderBook instance -- a live one from Layer 3, or one
reconstructed from historical book_events by src/orderbook/replay.py for
backtesting. The simulator itself doesn't know or care which.
"""
from __future__ import annotations

from dataclasses import dataclass

from src.common.events import Side
from src.orderbook.book import OrderBook


@dataclass(frozen=True)
class Fill:
    order_side: Side
    requested_qty: float
    filled_qty: float
    avg_price: float | None  # None if nothing could be filled at all (empty book)
    best_price_at_decision: float | None  # best bid/ask before this order touched the book

    @property
    def is_full_fill(self) -> bool:
        return abs(self.filled_qty - self.requested_qty) < 1e-12

    @property
    def shortfall(self) -> float:
        """Quantity that could not be filled -- available liquidity ran out
        before the full requested size was reached. A backtest that
        silently ignores this is claiming a fill the real market couldn't
        have given you."""
        return max(0.0, self.requested_qty - self.filled_qty)

    def slippage_bps(self) -> float | None:
        """How much worse the average fill price was than the best price
        available before the order touched the book, in basis points.
        Positive always means "worse for the trader," regardless of side."""
        if self.avg_price is None or not self.best_price_at_decision:
            return None
        raw = (self.avg_price - self.best_price_at_decision) / self.best_price_at_decision
        # BUY: paying more than best ask is bad -> raw already positive when bad.
        # SELL: receiving less than best bid is bad -> raw is negative when bad, so flip.
        sign = 1.0 if self.order_side == Side.BUY else -1.0
        return raw * sign * 10_000


class ExecutionSimulator:
    def simulate_market_order(self, book: OrderBook, order_side: Side, qty: float) -> Fill:
        if qty <= 0:
            raise ValueError("qty must be positive; use order_side to indicate direction")

        if order_side == Side.BUY:
            levels = list(book.asks.items())            # best (lowest) ask first
            best_price = book.best_ask()[0] if book.best_ask() else None
        else:
            levels = list(reversed(book.bids.items()))  # best (highest) bid first
            best_price = book.best_bid()[0] if book.best_bid() else None

        remaining = qty
        filled_qty = 0.0
        filled_notional = 0.0

        for price, size_available in levels:
            if remaining <= 0:
                break
            take = min(remaining, size_available)
            filled_qty += take
            filled_notional += take * price
            remaining -= take

        avg_price = (filled_notional / filled_qty) if filled_qty > 0 else None
        return Fill(
            order_side=order_side, requested_qty=qty, filled_qty=filled_qty,
            avg_price=avg_price, best_price_at_decision=best_price,
        )
