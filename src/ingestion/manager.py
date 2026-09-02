"""
Runs all configured exchange clients concurrently, fanning their normalized
events into a single asyncio.Queue that downstream consumers (storage
writers, order book builders) read from. This is the "resilience" layer:
Binance dropping doesn't affect Coinbase, and either reconnecting is
invisible to consumers except for the ConnectionEvent markers they can react
to.
"""
from __future__ import annotations

import asyncio
import logging

from src.common.events import NormalizedEvent
from src.ingestion.binance_client import BinanceClient
from src.ingestion.coinbase_client import CoinbaseClient

logger = logging.getLogger(__name__)


class IngestionManager:
    def __init__(self, symbols: list[str], queue_maxsize: int = 10_000):
        self.symbols = symbols
        self.queue: "asyncio.Queue[NormalizedEvent]" = asyncio.Queue(maxsize=queue_maxsize)
        self.clients = [
            BinanceClient(symbols, self.queue),
            CoinbaseClient(symbols, self.queue),
        ]
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        logger.info(
            "starting ingestion for symbols=%s across exchanges=%s",
            self.symbols, [c.name for c in self.clients],
        )
        self._tasks = [asyncio.create_task(c.run_forever()) for c in self.clients]

    async def stop(self) -> None:
        for c in self.clients:
            c.stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def events(self):
        """Async generator over the merged event stream. A consumer just
        does `async for event in manager.events(): ...` without caring which
        exchange produced it."""
        while True:
            event = await self.queue.get()
            yield event
