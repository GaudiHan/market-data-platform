"""
Every exchange client implements the same contract: connect, stream
normalized events onto an asyncio.Queue, reconnect with backoff on failure,
and never let a malformed message crash the process.

This is deliberately an async generator-style "run forever, push to queue"
design rather than "fetch and return" -- it's a live feed, not a batch call.
"""
from __future__ import annotations

import abc
import asyncio
import logging

from src.common.events import NormalizedEvent

logger = logging.getLogger(__name__)


class ExchangeClient(abc.ABC):
    """Base class for a single exchange's WebSocket ingestion client."""

    name: str  # e.g. "binance", "coinbase"

    def __init__(self, symbols: list[str], out_queue: "asyncio.Queue[NormalizedEvent]"):
        self.symbols = symbols
        self.out_queue = out_queue
        self._stop = asyncio.Event()
        self._max_backoff_s = 30.0
        self._base_backoff_s = 1.0
        self._ws = None  # live connection handle, set by _connect_and_stream; used by trigger_resync

    async def run_forever(self) -> None:
        """Connect, stream, and reconnect with exponential backoff + jitter
        on any failure, until stop() is called. A dropped connection is
        expected/normal operating behavior here, not an exceptional case --
        exchanges cycle connections, networks blip. We log it, emit a
        ConnectionEvent so downstream consumers know to resync, and retry."""
        backoff = self._base_backoff_s
        while not self._stop.is_set():
            try:
                await self._connect_and_stream()
                backoff = self._base_backoff_s  # reset after a clean run
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 -- intentionally broad at this boundary
                logger.warning(
                    "%s: connection lost (%s), retrying in %.1fs",
                    self.name, exc, backoff,
                )
                await self._emit_connection_event("disconnected", str(exc))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._max_backoff_s)

    def stop(self) -> None:
        self._stop.set()

    @abc.abstractmethod
    async def _connect_and_stream(self) -> None:
        """Open the WebSocket, subscribe, and push normalized events onto
        out_queue until the connection drops or an error is raised. Must
        raise (not swallow) on connection failure so run_forever's retry
        loop can handle it centrally."""
        raise NotImplementedError

    async def trigger_resync(self, symbol: str) -> None:
        """Ask this client to obtain a fresh snapshot for `symbol` WITHOUT
        necessarily tearing down the whole connection -- this is what lets
        the order book layer recover from a sequence gap (Binance) while the
        websocket itself is still healthy. Default: unsupported; subclasses
        override where the exchange makes this possible."""
        logger.warning("%s: trigger_resync not supported for this client", self.name)

    async def _emit_connection_event(self, kind: str, detail: str = "", symbol: str = "*") -> None:
        from src.common.events import ConnectionEvent  # local import avoids cycle risk
        await self.out_queue.put(
            ConnectionEvent(exchange=self.name, symbol=symbol, kind=kind, detail=detail)  # type: ignore[arg-type]
        )
