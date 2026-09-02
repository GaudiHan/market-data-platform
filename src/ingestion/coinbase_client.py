"""
Coinbase ingestion client, using the public "Exchange" WebSocket feed
(wss://ws-feed.exchange.coinbase.com). Deliberately NOT the newer "Advanced
Trade" WS API -- that one requires a signed JWT even for public market data,
which doesn't fit the zero-budget/no-account constraint. The Exchange feed's
`matches` and `level2` channels are public and anonymous.

Known asymmetry vs. Binance (documented here rather than hidden): the
`level2` channel does not carry a per-message sequence/update-id the way
Binance's depth diffs carry U/u. That means Coinbase-side gap *detection*
mid-stream isn't possible from this channel alone -- the practical resync
strategy is connection-level: any disconnect/error triggers a fresh
subscribe + snapshot, discarding prior book state. Binance additionally
supports true sequence-gap detection within a live connection. Both paths
converge in src/orderbook, which treats "no snapshot seen yet" and "sequence
gap detected" as the same trigger: throw away local state, wait for next
snapshot.
"""
from __future__ import annotations

import asyncio
import logging
import time

import orjson
import websockets

from src.common.events import BookDiff, BookLevel, BookSide, BookSnapshot, Side, Trade
from src.common.symbols import from_coinbase, to_coinbase
from src.ingestion.base import ExchangeClient

logger = logging.getLogger(__name__)

WS_URL = "wss://ws-feed.exchange.coinbase.com"


class CoinbaseClient(ExchangeClient):
    name = "coinbase"

    async def _connect_and_stream(self) -> None:
        native_symbols = [to_coinbase(s) for s in self.symbols]
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": native_symbols,
            "channels": ["matches", "level2"],
        }

        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(orjson.dumps(subscribe_msg).decode())
            await self._emit_connection_event("connected")
            async for raw in ws:
                self._handle_message(raw)

    def _handle_message(self, raw: str | bytes) -> None:
        try:
            payload = orjson.loads(raw)
            msg_type = payload.get("type")

            if msg_type == "match":
                self._handle_match(payload)
            elif msg_type == "snapshot":
                self._handle_snapshot(payload)
            elif msg_type == "l2update":
                self._handle_l2update(payload)
            elif msg_type in ("subscriptions", "heartbeat"):
                pass  # expected control messages, not data
            elif msg_type == "error":
                logger.warning("coinbase: error message from feed: %s", payload)
            else:
                logger.debug("coinbase: unrecognized message type: %s", msg_type)
        except Exception as exc:  # noqa: BLE001 -- boundary must never raise
            logger.warning("coinbase: malformed message, skipping (%s)", exc)
            asyncio.create_task(
                self._emit_connection_event("malformed_message", detail=str(exc))
            )

    def _handle_match(self, payload: dict) -> None:
        common_symbol = from_coinbase(payload["product_id"])
        # Coinbase's `side` on a match is the MAKER's side (the resting order
        # that got hit) -- so the taker (the aggressor, which is what "trade
        # side" conventionally means elsewhere in this project) is the
        # opposite. A maker "sell" means a buy order came in and took it.
        maker_side = payload["side"]
        taker_side = Side.BUY if maker_side == "sell" else Side.SELL
        trade = Trade(
            ts_ns=self._parse_iso_ns(payload["time"]),
            exchange=self.name,
            symbol=common_symbol,
            trade_id=str(payload["trade_id"]),
            price=float(payload["price"]),
            size=float(payload["size"]),
            side=taker_side,
        )
        self.out_queue.put_nowait(trade)

    def _handle_snapshot(self, payload: dict) -> None:
        common_symbol = from_coinbase(payload["product_id"])
        bids = tuple(
            BookLevel(price=float(p), size=float(q), side=BookSide.BID)
            for p, q in payload["bids"]
        )
        asks = tuple(
            BookLevel(price=float(p), size=float(q), side=BookSide.ASK)
            for p, q in payload["asks"]
        )
        snapshot = BookSnapshot(
            ts_ns=time.time_ns(),
            exchange=self.name,
            symbol=common_symbol,
            bids=bids,
            asks=asks,
            last_update_id=None,  # Coinbase level2 carries no update id -- see module docstring
        )
        self.out_queue.put_nowait(snapshot)

    def _handle_l2update(self, payload: dict) -> None:
        common_symbol = from_coinbase(payload["product_id"])
        levels = tuple(
            BookLevel(
                price=float(price),
                size=float(size),
                side=BookSide.BID if side == "buy" else BookSide.ASK,
            )
            for side, price, size in payload["changes"]
        )
        diff = BookDiff(
            ts_ns=self._parse_iso_ns(payload["time"]),
            exchange=self.name,
            symbol=common_symbol,
            levels=levels,
            first_update_id=None,
            last_update_id=None,
        )
        self.out_queue.put_nowait(diff)

    @staticmethod
    def _parse_iso_ns(iso_ts: str) -> int:
        from datetime import datetime, timezone
        dt = datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
