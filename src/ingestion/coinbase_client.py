"""
Coinbase ingestion client, using the public "Exchange" WebSocket feed
(wss://ws-feed.exchange.coinbase.com).

Correction from what Layer 1 originally assumed: Coinbase's plain `level2`
channel has required authentication (a signed API key) since August 1,
2023 -- confirmed against current Coinbase docs
(https://docs.cdp.coinbase.com/exchange/websocket-feed/authentication).
The original Layer 1 docstring claiming `level2` was public was wrong;
that's stale information, not something the sandbox's network restrictions
could have caught directly (no outbound to exchange domains here), but
verifiable -- and verified -- via a web search instead.

The fix is NOT to drop order-book reconstruction for Coinbase, which would
have been a bigger change than necessary: Coinbase also documents
`level2_batch` (the current name for what used to be called `level2_50`),
which is explicitly called out as NOT requiring authentication and sends
the exact same message shapes (`snapshot` then `l2update`), just batched
every 50ms server-side rather than pushed per individual change. That's a
one-line channel-name change with no parsing differences at all, so the
order-book reconciliation logic built in Layer 3 applies unmodified.

Known asymmetry vs. Binance (unchanged from the original design): neither
`level2` nor `level2_batch` carries a per-message sequence/update-id the
way Binance's depth diffs carry U/u. That means Coinbase-side gap
*detection* mid-stream isn't possible from this channel alone -- the
practical resync strategy is connection-level: any disconnect/error, or an
explicit resync request, triggers a fresh subscribe + snapshot, discarding
prior book state.
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

# `websockets`' default max message size is 1MB, and it closes the
# connection (code 1009) rather than truncate when a message exceeds it.
# A level2_batch snapshot for a liquid pair like BTC-USD can carry
# thousands of price levels in one JSON message and blow past 1MB easily --
# without raising this, the client would loop forever: connect, receive the
# oversized snapshot, get disconnected, reconnect, repeat. 20MB is generous
# headroom without being unbounded (avoids one pathological message being
# able to exhaust memory).
MAX_WS_MESSAGE_BYTES = 20 * 1024 * 1024

# Public, anonymous channels only. Plain "level2" (and "level3"/"full")
# require a signed API key as of Aug 2023 -- see module docstring.
# "level2_batch" gives identical message shapes without authentication.
PUBLIC_CHANNELS = ["matches", "level2_batch"]


class CoinbaseClient(ExchangeClient):
    name = "coinbase"

    async def _connect_and_stream(self) -> None:
        native_symbols = [to_coinbase(s) for s in self.symbols]
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": native_symbols,
            "channels": PUBLIC_CHANNELS,
        }

        async with websockets.connect(
            WS_URL, ping_interval=20, ping_timeout=20, max_size=MAX_WS_MESSAGE_BYTES
        ) as ws:
            self._ws = ws
            await ws.send(orjson.dumps(subscribe_msg).decode())
            await self._emit_connection_event("connected")
            async for raw in ws:
                self._handle_message(raw)

    async def trigger_resync(self, symbol: str) -> None:
        """Coinbase's level2_batch channel has no sequence number to detect
        a gap with (see module docstring) -- but re-sending a `subscribe`
        for this product's level2_batch channel makes the server push a
        brand-new snapshot message, which is enough to recover from "we're
        not sure our local state is right" without a full reconnect.
        Requires the connection to still be open; if it isn't, the normal
        reconnect-and-resubscribe path in _connect_and_stream will get a
        fresh snapshot anyway."""
        if self._ws is None:
            logger.warning("coinbase: trigger_resync called with no live connection, symbol=%s", symbol)
            return
        native_symbol = to_coinbase(symbol)
        msg = {"type": "subscribe", "product_ids": [native_symbol], "channels": ["level2_batch"]}
        logger.info("coinbase: resync requested for %s, re-subscribing to level2_batch", symbol)
        await self._ws.send(orjson.dumps(msg).decode())

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
            last_update_id=None,  # level2_batch carries no update id -- see module docstring
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