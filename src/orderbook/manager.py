"""
Owns one ReconcilingBookManager per (exchange, symbol), fed by the same
merged event stream the storage writer consumes. This is the thing that
turns "a pile of normalized events" into "queryable live order books."
"""
from __future__ import annotations

import asyncio
import logging

from src.common.events import BookDiff, BookSnapshot, ConnectionEvent
from src.orderbook.book import OrderBook
from src.orderbook.reconciler import BinanceReconciler, CoinbaseReconciler, ReconcilingBookManager

logger = logging.getLogger(__name__)

_RECONCILERS = {
    "binance": BinanceReconciler,
    "coinbase": CoinbaseReconciler,
}


class OrderBookRegistry:
    def __init__(self, ingestion_manager):
        """`ingestion_manager` is a src.ingestion.manager.IngestionManager --
        typed loosely here to avoid a hard import-cycle risk between
        ingestion and orderbook packages."""
        self.ingestion_manager = ingestion_manager
        self._books: dict[tuple[str, str], ReconcilingBookManager] = {}

    def _get_or_create(self, exchange: str, symbol: str) -> ReconcilingBookManager:
        key = (exchange, symbol)
        if key not in self._books:
            reconciler_cls = _RECONCILERS.get(exchange)
            if reconciler_cls is None:
                raise ValueError(f"no reconciler registered for exchange '{exchange}'")
            self._books[key] = reconciler_cls(exchange, symbol, resync_cb=self._request_resync)
        return self._books[key]

    def _request_resync(self, exchange: str, symbol: str) -> None:
        # Reconcilers call this synchronously from inside event handling;
        # schedule the actual (async) resync request rather than awaiting
        # it here, so book-updating stays a fast, synchronous hot path.
        asyncio.create_task(self.ingestion_manager.request_resync(exchange, symbol))

    async def run(self) -> None:
        async for event in self.ingestion_manager.events():
            if isinstance(event, BookSnapshot):
                self._get_or_create(event.exchange, event.symbol).handle_snapshot(event)
            elif isinstance(event, BookDiff):
                self._get_or_create(event.exchange, event.symbol).handle_diff(event)
            elif isinstance(event, ConnectionEvent):
                self._broadcast_connection_event(event)
            # Trade events aren't book-relevant; the storage writer handles those.

    def _broadcast_connection_event(self, event: ConnectionEvent) -> None:
        for (exchange, _symbol), reconciler in self._books.items():
            if exchange == event.exchange:
                reconciler.handle_connection_event(event)

    def get_book(self, exchange: str, symbol: str) -> OrderBook | None:
        reconciler = self._books.get((exchange, symbol))
        return reconciler.book if reconciler else None

    def is_synced(self, exchange: str, symbol: str) -> bool:
        reconciler = self._books.get((exchange, symbol))
        return reconciler.is_synced if reconciler else False

    def top_of_book(self, exchange: str, symbol: str) -> dict | None:
        book = self.get_book(exchange, symbol)
        if book is None:
            return None
        return {
            "best_bid": book.best_bid(),
            "best_ask": book.best_ask(),
            "spread": book.spread(),
            "mid": book.mid_price(),
            "synced": self.is_synced(exchange, symbol),
        }
