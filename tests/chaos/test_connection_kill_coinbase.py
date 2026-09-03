"""
Same chaos requirement as test_connection_kill.py, but for Coinbase --
worth doing separately because Coinbase's resync mechanism is genuinely
different, not just a copy-paste of Binance's. Coinbase's level2_batch
channel carries no sequence number (see coinbase_client.py's module
docstring), so there's no "does this diff bridge the gap" check possible --
correctness here means "trust whatever arrives after the next snapshot,"
and the snapshot itself only ever arrives by (re)subscribing. This test
proves that path specifically: kill the connection, confirm the client
reconnects and resubscribes on its own, and confirm the book ends up
reflecting only the fresh snapshot's state.
"""
import asyncio
import contextlib

import orjson
import pytest
import websockets

from src.common.events import BookDiff, BookSnapshot, ConnectionEvent
from src.ingestion import coinbase_client as coinbase_client_module
from src.ingestion.coinbase_client import CoinbaseClient
from src.orderbook.reconciler import CoinbaseReconciler

PRE_KILL_MESSAGE_COUNT = 5
POST_RECONNECT_PRICE_JUMP = 5000.0


class FakeCoinbaseServer:
    def __init__(self):
        self.price = 100.0
        self.subscription_count = 0
        self.connection_count = 0

    async def handle_ws(self, websocket) -> None:
        self.connection_count += 1
        is_first_connection = self.connection_count == 1
        try:
            # Coinbase pushes a subscribe ack then a snapshot as soon as it
            # receives the subscribe message -- wait for it, ignore content.
            await websocket.recv()
            self.subscription_count += 1
            if self.subscription_count == 2:
                self.price += POST_RECONNECT_PRICE_JUMP  # market moved during the outage

            await websocket.send(orjson.dumps({
                "type": "snapshot", "product_id": "BTC-USD",
                "bids": [[str(self.price - 1), "1.0"]],
                "asks": [[str(self.price + 1), "1.0"]],
            }).decode())

            for _ in range(PRE_KILL_MESSAGE_COUNT if is_first_connection else 3):
                self.price += 0.01
                await websocket.send(orjson.dumps({
                    "type": "l2update", "product_id": "BTC-USD",
                    "time": "2024-01-01T00:00:00.000000Z",
                    "changes": [["buy", str(self.price), "2.0"]],
                }).decode())
                await asyncio.sleep(0.02)

            if is_first_connection:
                await websocket.close(code=1011, reason="simulated kill mid-stream")
            else:
                await websocket.close(code=1000)
        except websockets.exceptions.ConnectionClosed:
            pass


async def _consume_into_reconciler(queue: asyncio.Queue, reconciler: CoinbaseReconciler, seen_events: list):
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
async def test_coinbase_kill_mid_stream_reconnects_and_resyncs(monkeypatch):
    fake_server = FakeCoinbaseServer()

    ws_server = await websockets.serve(fake_server.handle_ws, "127.0.0.1", 0)
    ws_port = ws_server.sockets[0].getsockname()[1]
    ws_url = f"ws://127.0.0.1:{ws_port}"

    monkeypatch.setattr(coinbase_client_module, "WS_URL", ws_url)

    queue: asyncio.Queue = asyncio.Queue()
    client = CoinbaseClient(symbols=["BTC-USD"], out_queue=queue)
    client._base_backoff_s = 0.05
    client._max_backoff_s = 0.1

    reconciler = CoinbaseReconciler("coinbase", "BTC-USD")
    seen_connection_events: list[ConnectionEvent] = []

    client_task = asyncio.create_task(client.run_forever())
    consumer_task = asyncio.create_task(_consume_into_reconciler(queue, reconciler, seen_connection_events))

    try:
        await _wait_until(lambda: reconciler.is_synced)
        pre_kill_bid = reconciler.book.best_bid()[0]
        assert pre_kill_bid < POST_RECONNECT_PRICE_JUMP

        await _wait_until(lambda: any(e.kind == "disconnected" for e in seen_connection_events))
        await _wait_until(lambda: fake_server.connection_count >= 2, timeout=5.0)
        await _wait_until(lambda: reconciler.is_synced, timeout=5.0)

        post_resync_bid, _ = reconciler.book.best_bid()
        assert post_resync_bid >= POST_RECONNECT_PRICE_JUMP, (
            f"expected post-resync price >= {POST_RECONNECT_PRICE_JUMP}, "
            f"got {post_resync_bid} -- looks like stale pre-kill state survived"
        )
        assert pre_kill_bid not in reconciler.book.bids, "pre-kill price level must not survive the resync"

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
