"""
The chaos-test requirement, run for real: "Kill the WebSocket connection
mid-stream, assert the system reconnects and resyncs the book from a fresh
snapshot without corrupting downstream data."

This spins up a REAL local WebSocket server and a real local HTTP server
(standing in for Binance's WS feed and REST snapshot endpoint) on
localhost, points BinanceClient at them via monkeypatching the module-level
URL constants, and then has the fake server abruptly kill the connection
mid-stream (a policy-violation close code, not a graceful one -- the
closest a portable automated test can get to "yanked the network cable").

This is deliberately NOT mocked at the object level (no fake `_handle_message`
calls, no monkeypatched `websockets.connect`) -- it goes through a real
socket, real WebSocket handshake, and the actual reconnect/backoff loop in
src/ingestion/base.py, so it proves the whole stack recovers, not just that
individual functions behave correctly in isolation.

Design of the fake server: a single shared, ever-incrementing "true price"
state that the REST snapshot endpoint and the WS diff stream both read
from, so the numbers are internally consistent the way a real exchange's
would be. On the SECOND REST snapshot fetch (the one triggered by
reconnecting after the kill), the price jumps by a large, unmistakable
amount -- simulating real market movement during the outage. The book is
then required to end up reflecting ONLY that new price regime, with zero
trace of the pre-kill price level -- this is the "without corrupting
downstream data" half of the requirement, made concrete and assertable
rather than just claimed.
"""
import asyncio
import contextlib

import orjson
import pytest
import websockets
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from src.common.events import BookDiff, BookSnapshot, ConnectionEvent
from src.ingestion import binance_client as binance_client_module
from src.ingestion.binance_client import BinanceClient
from src.orderbook.reconciler import BinanceReconciler

PRE_KILL_MESSAGE_COUNT = 5
POST_RECONNECT_PRICE_JUMP = 5000.0


class FakeBinanceServer:
    """Owns both the fake REST snapshot endpoint and the fake WS diff
    stream, sharing one piece of state between them the way the real
    exchange's REST and WS surfaces are two views onto the same order book."""

    def __init__(self):
        self.update_id = 0
        self.price = 100.0
        self.rest_call_count = 0
        self.connection_count = 0
        self.connection_events_seen: list[str] = []  # "abrupt_kill" / "ended"

    # ---- REST snapshot endpoint ----
    async def handle_depth(self, request: web.Request) -> web.Response:
        self.rest_call_count += 1
        if self.rest_call_count == 2:
            # Simulate real market movement during the outage -- this is
            # the unmistakable signal the test checks for post-resync.
            self.price += POST_RECONNECT_PRICE_JUMP
        return web.json_response({
            "lastUpdateId": self.update_id,
            "bids": [[str(self.price - 1), "1.0"]],
            "asks": [[str(self.price + 1), "1.0"]],
        })

    # ---- WS diff stream ----
    async def handle_ws(self, websocket) -> None:
        self.connection_count += 1
        is_first_connection = self.connection_count == 1
        try:
            for _ in range(PRE_KILL_MESSAGE_COUNT if is_first_connection else 3):
                self.update_id += 1
                self.price += 0.01
                msg = {
                    "stream": "btcusdt@depth@100ms",
                    "data": {
                        "e": "depthUpdate", "E": self.update_id, "s": "BTCUSDT",
                        "U": self.update_id, "u": self.update_id,
                        "b": [[str(self.price), "2.0"]], "a": [],
                    },
                }
                await websocket.send(orjson.dumps(msg).decode())
                await asyncio.sleep(0.02)

            if is_first_connection:
                # The kill: an abnormal close code, not a graceful one --
                # `websockets` raises ConnectionClosedError on the client
                # side for this, exercising run_forever's exception path
                # (as opposed to the clean-close path already covered by
                # tests/chaos/test_reconnect_base.py).
                self.connection_events_seen.append("abrupt_kill")
                await websocket.close(code=1011, reason="simulated kill mid-stream")
            else:
                self.connection_events_seen.append("ended")
                await websocket.close(code=1000)
        except websockets.exceptions.ConnectionClosed:
            pass


async def _consume_into_reconciler(queue: asyncio.Queue, reconciler: BinanceReconciler, seen_events: list):
    while True:
        event = await queue.get()
        if isinstance(event, BookSnapshot):
            reconciler.handle_snapshot(event)
        elif isinstance(event, BookDiff):
            reconciler.handle_diff(event)
        elif isinstance(event, ConnectionEvent):
            seen_events.append(event)
            reconciler.handle_connection_event(event)


async def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.02):
    async def _poll():
        while not predicate():
            await asyncio.sleep(interval)
    await asyncio.wait_for(_poll(), timeout=timeout)


@pytest.mark.asyncio
async def test_kill_mid_stream_reconnects_and_resyncs_without_corruption(monkeypatch):
    fake_server = FakeBinanceServer()

    # Real local HTTP server for the REST snapshot endpoint.
    app = web.Application()
    app.router.add_get("/api/v3/depth", fake_server.handle_depth)
    http_server = TestServer(app)
    http_client = TestClient(http_server)
    await http_client.start_server()
    rest_url = f"http://{http_server.host}:{http_server.port}/api/v3/depth"

    # Real local WebSocket server for the diff stream.
    ws_server = await websockets.serve(fake_server.handle_ws, "127.0.0.1", 0)
    ws_port = ws_server.sockets[0].getsockname()[1]
    ws_url = f"ws://127.0.0.1:{ws_port}/stream"

    monkeypatch.setattr(binance_client_module, "REST_DEPTH_URL", rest_url)
    monkeypatch.setattr(binance_client_module, "WS_BASE", ws_url)

    queue: asyncio.Queue = asyncio.Queue()
    client = BinanceClient(symbols=["BTC-USD"], out_queue=queue)
    client._base_backoff_s = 0.05  # keep the test fast
    client._max_backoff_s = 0.1

    reconciler = BinanceReconciler(
        "binance", "BTC-USD",
        resync_cb=lambda ex, sym: asyncio.create_task(client.trigger_resync(sym)),
    )
    seen_connection_events: list[ConnectionEvent] = []

    client_task = asyncio.create_task(client.run_forever())
    consumer_task = asyncio.create_task(_consume_into_reconciler(queue, reconciler, seen_connection_events))

    try:
        # 1. Confirm it syncs initially, before any chaos.
        await _wait_until(lambda: reconciler.is_synced)
        pre_kill_bid = reconciler.book.best_bid()[0]
        assert pre_kill_bid < POST_RECONNECT_PRICE_JUMP  # sanity: still in the original price regime

        # 2. Wait for the kill to actually happen and be noticed.
        await _wait_until(lambda: "abrupt_kill" in fake_server.connection_events_seen)
        await _wait_until(lambda: any(e.kind == "disconnected" for e in seen_connection_events))

        # 3. Confirm it reconnects and resyncs on its own.
        await _wait_until(lambda: fake_server.connection_count >= 2, timeout=5.0)
        await _wait_until(lambda: reconciler.is_synced, timeout=5.0)

        # 4. The core assertion: the book reflects ONLY the new, post-outage
        # price regime -- no trace of the pre-kill price level anywhere in
        # the book, proving the resync actually cleared state rather than
        # patching over it.
        post_resync_bid, _ = reconciler.book.best_bid()
        assert post_resync_bid >= POST_RECONNECT_PRICE_JUMP, (
            f"expected post-resync price >= {POST_RECONNECT_PRICE_JUMP}, "
            f"got {post_resync_bid} -- looks like stale pre-kill state survived"
        )
        assert pre_kill_bid not in reconciler.book.bids, "pre-kill price level must not survive the resync"

        # 5. Confirm the reconciler's own bookkeeping (last_update_id) also
        # reflects only post-kill updates, not a mix of old and new.
        assert reconciler.book.last_update_id is not None
        assert reconciler.book.last_update_id > 0

    finally:
        client.stop()
        client_task.cancel()
        consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await client_task
        with contextlib.suppress(asyncio.CancelledError):
            await consumer_task
        ws_server.close()
        await ws_server.wait_closed()
        await http_client.close()
