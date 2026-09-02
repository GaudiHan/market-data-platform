"""
Binance ingestion client.

Streams: combined trade + diff-depth streams over one WebSocket connection
(cheaper than one connection per stream, and Binance explicitly supports it).

Order book note: Binance's own documented procedure for building a correct
local book is:
  1. Start buffering diff events from <symbol>@depth
  2. Fetch a REST snapshot (GET /api/v3/depth) with its own lastUpdateId
  3. Discard any buffered diff where diff.u <= snapshot.lastUpdateId
  4. The first diff you apply must satisfy U <= lastUpdateId+1 <= u
  5. Every next diff's U must equal the previous diff's u + 1, or you have a
     gap and must resync from a fresh snapshot.
This client's job is only to produce the raw, faithfully-normalized events
(snapshot once at start, diffs continuously) with U/u intact. The gap
detection and reconciliation algorithm itself lives in src/orderbook -- that's
where "handling out-of-order messages" is actually demonstrated, not buried
in a network client.
"""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp
import orjson
import websockets

from src.common.events import BookDiff, BookLevel, BookSide, BookSnapshot, Side, Trade
from src.common.symbols import from_binance, to_binance
from src.ingestion.base import ExchangeClient

logger = logging.getLogger(__name__)

WS_BASE = "wss://stream.binance.com:9443/stream"
REST_DEPTH_URL = "https://api.binance.com/api/v3/depth"


class BinanceClient(ExchangeClient):
    name = "binance"

    async def _connect_and_stream(self) -> None:
        native_symbols = [to_binance(s) for s in self.symbols]
        streams = []
        for ns in native_symbols:
            streams.append(f"{ns}@trade")
            streams.append(f"{ns}@depth@100ms")
        url = f"{WS_BASE}?streams={'/'.join(streams)}"

        # Fetch a fresh REST snapshot for every symbol before/around the
        # streaming connection so the order book layer always has a known
        # starting point to reconcile diffs against -- both on first start
        # and after any reconnect.
        for common_symbol in self.symbols:
            await self._fetch_and_emit_snapshot(common_symbol)

        logger.info("binance: connecting to %s", url)
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            await self._emit_connection_event("connected")
            async for raw in ws:
                self._handle_message(raw)

    async def _fetch_and_emit_snapshot(self, common_symbol: str) -> None:
        native_symbol = to_binance(common_symbol).upper()
        async with aiohttp.ClientSession() as session:
            async with session.get(
                REST_DEPTH_URL, params={"symbol": native_symbol, "limit": 1000}
            ) as resp:
                data = await resp.json()

        bids = tuple(
            BookLevel(price=float(p), size=float(q), side=BookSide.BID)
            for p, q in data["bids"]
        )
        asks = tuple(
            BookLevel(price=float(p), size=float(q), side=BookSide.ASK)
            for p, q in data["asks"]
        )
        snapshot = BookSnapshot(
            ts_ns=time.time_ns(),
            exchange=self.name,
            symbol=common_symbol,
            bids=bids,
            asks=asks,
            last_update_id=data["lastUpdateId"],
        )
        await self.out_queue.put(snapshot)

    def _handle_message(self, raw: str | bytes) -> None:
        """Parse one combined-stream message. Any failure here -- bad JSON,
        missing fields, an unexpected shape -- is caught and turned into a
        malformed_message ConnectionEvent rather than propagating and
        killing the connection loop. This is the behavior the chaos test
        (malformed payload -> graceful handling) exercises directly."""
        try:
            outer = orjson.loads(raw)
            stream = outer["stream"]
            payload = outer["data"]
            event_type = payload.get("e")

            if event_type == "trade":
                self._handle_trade(payload)
            elif event_type == "depthUpdate":
                self._handle_depth_update(payload)
            else:
                logger.debug("binance: unrecognized event type on %s: %s", stream, event_type)
        except Exception as exc:  # noqa: BLE001 -- boundary must never raise
            logger.warning("binance: malformed message, skipping (%s)", exc)
            # Fire-and-forget: we're in a sync callback, schedule the coroutine.
            asyncio.create_task(
                self._emit_connection_event("malformed_message", detail=str(exc))
            )

    def _handle_trade(self, payload: dict) -> None:
        common_symbol = from_binance(payload["s"])
        is_buyer_maker = payload["m"]
        # If the buyer was the maker, the trade was initiated by a sell order
        # (the taker sold into a resting buy) -- so the taker side is SELL.
        taker_side = Side.SELL if is_buyer_maker else Side.BUY
        trade = Trade(
            ts_ns=int(payload["T"]) * 1_000_000,
            exchange=self.name,
            symbol=common_symbol,
            trade_id=str(payload["t"]),
            price=float(payload["p"]),
            size=float(payload["q"]),
            side=taker_side,
        )
        self.out_queue.put_nowait(trade)

    def _handle_depth_update(self, payload: dict) -> None:
        common_symbol = from_binance(payload["s"])
        levels = tuple(
            BookLevel(price=float(p), size=float(q), side=BookSide.BID)
            for p, q in payload["b"]
        ) + tuple(
            BookLevel(price=float(p), size=float(q), side=BookSide.ASK)
            for p, q in payload["a"]
        )
        diff = BookDiff(
            ts_ns=int(payload["E"]) * 1_000_000,
            exchange=self.name,
            symbol=common_symbol,
            levels=levels,
            first_update_id=int(payload["U"]),
            last_update_id=int(payload["u"]),
        )
        self.out_queue.put_nowait(diff)
