"""
Fast, network-free tests for the reconnect/backoff/event-emission contract
every exchange client shares (src/ingestion/base.py). Uses a minimal fake
ExchangeClient whose _connect_and_stream() is scripted to disconnect in
specific ways, so this exercises run_forever's control flow directly and
quickly, without needing a real socket. The full local-WebSocket-server
tests in test_connection_kill.py cover the real thing end-to-end; this file
exists to pin down run_forever's own logic precisely and catch regressions
in it fast.
"""
import asyncio

import pytest

from src.common.events import ConnectionEvent
from src.ingestion.base import ExchangeClient


class ScriptedClient(ExchangeClient):
    """A fake exchange client whose _connect_and_stream() plays back a
    scripted sequence of behaviors: 'clean' returns normally (simulating a
    graceful close), an exception instance raises it (simulating an
    abnormal close/network error), and 'cancel' raises CancelledError --
    matching how a real shutdown actually happens (IngestionManager.stop()
    cancels the running task; it doesn't make _connect_and_stream return
    normally), so run_forever's CancelledError-reraise path is exercised
    the way it really gets hit, not simulated via a clean return."""

    name = "scripted"

    def __init__(self, symbols, out_queue, script: list):
        super().__init__(symbols, out_queue)
        self._script = list(script)
        self.connect_attempts = 0

    async def _connect_and_stream(self) -> None:
        self.connect_attempts += 1
        action = self._script.pop(0)
        if callable(action):
            action()  # lets a test call client.stop() synchronously mid-"connection", deterministically
            return
        if action == "clean":
            return
        if action == "cancel":
            raise asyncio.CancelledError()
        if isinstance(action, BaseException):
            raise action
        raise ValueError(f"unknown script action: {action!r}")


async def _drain_connection_events(queue: asyncio.Queue) -> list[ConnectionEvent]:
    events = []
    while not queue.empty():
        events.append(queue.get_nowait())
    return events


@pytest.mark.asyncio
async def test_clean_close_still_emits_disconnected_event():
    """This is the bug the chaos test was built to catch: `websockets`'
    async iterator returns normally (no exception) on a clean close, so
    without this behavior, a graceful server-side disconnect would leave
    the order book layer believing nothing happened."""
    queue: asyncio.Queue = asyncio.Queue()
    client = ScriptedClient(["BTC-USD"], queue, script=["clean", "cancel"])

    with pytest.raises(asyncio.CancelledError):
        await client.run_forever()

    events = await _drain_connection_events(queue)
    assert len(events) == 1
    assert events[0].kind == "disconnected"
    assert client.connect_attempts == 2  # reconnected once after the clean close, then cancelled


@pytest.mark.asyncio
async def test_abnormal_close_emits_disconnected_event_with_backoff():
    queue: asyncio.Queue = asyncio.Queue()
    client = ScriptedClient(["BTC-USD"], queue, script=[ConnectionError("boom"), "cancel"])
    client._base_backoff_s = 0.01
    client._max_backoff_s = 0.01

    with pytest.raises(asyncio.CancelledError):
        await client.run_forever()

    events = await _drain_connection_events(queue)
    assert len(events) == 1
    assert events[0].kind == "disconnected"
    assert "boom" in events[0].detail
    assert client.connect_attempts == 2


@pytest.mark.asyncio
async def test_reconnects_after_every_disconnect_until_stopped():
    """Multiple disconnects in a row -- mix of clean and abnormal -- must
    each trigger a reconnect attempt and each emit their own event. This is
    the "assert the system reconnects" half of the chaos requirement,
    proven for an arbitrary number of drops, not just one."""
    queue: asyncio.Queue = asyncio.Queue()
    script = ["clean", ConnectionError("drop 1"), "clean", ConnectionError("drop 2"), "cancel"]
    client = ScriptedClient(["BTC-USD"], queue, script=script)
    client._base_backoff_s = 0.01
    client._max_backoff_s = 0.01

    with pytest.raises(asyncio.CancelledError):
        await client.run_forever()

    events = await _drain_connection_events(queue)
    assert len(events) == 4  # one per disconnect; the final cancel emits nothing
    assert [e.kind for e in events] == ["disconnected"] * 4
    assert client.connect_attempts == 5


@pytest.mark.asyncio
async def test_backoff_resets_after_a_clean_close_following_a_failure():
    """A successful (clean) connection after a run of failures should reset
    the backoff clock -- otherwise a single blip early on would leave every
    future reconnect artificially slow forever."""
    queue: asyncio.Queue = asyncio.Queue()
    script = [ConnectionError("fail 1"), ConnectionError("fail 2"), "clean", "cancel"]
    client = ScriptedClient(["BTC-USD"], queue, script=script)
    client._base_backoff_s = 0.01
    client._max_backoff_s = 0.05

    sleep_calls = []
    real_sleep = asyncio.sleep

    async def recording_sleep(delay):
        sleep_calls.append(delay)
        await real_sleep(0)  # don't actually wait in the test

    import unittest.mock
    with unittest.mock.patch("asyncio.sleep", side_effect=recording_sleep):
        with pytest.raises(asyncio.CancelledError):
            await client.run_forever()

    # Two backoff sleeps for the two failures (increasing), none for the
    # clean close.
    assert len(sleep_calls) == 2
    assert sleep_calls[0] < sleep_calls[1]


@pytest.mark.asyncio
async def test_stop_called_before_run_forever_prevents_any_connect_attempt():
    """stop() is how a real shutdown is signalled externally (e.g.
    IngestionManager.stop()) -- calling it before run_forever() ever starts
    should mean _connect_and_stream is never invoked at all."""
    queue: asyncio.Queue = asyncio.Queue()
    client = ScriptedClient(["BTC-USD"], queue, script=[])

    client.stop()
    await client.run_forever()

    assert client.connect_attempts == 0
    assert client._stop.is_set()


@pytest.mark.asyncio
async def test_stop_called_mid_stream_prevents_further_reconnects():
    """The realistic shutdown path: stop() is called while a connection is
    active (e.g. from IngestionManager.stop() running concurrently).
    Scripted deterministically -- stop() is invoked synchronously as part
    of the single connection attempt, rather than raced against it from a
    separate task -- so this doesn't depend on asyncio scheduling order."""
    queue: asyncio.Queue = asyncio.Queue()
    client = ScriptedClient(["BTC-USD"], queue, script=[])
    client._script = [client.stop]  # call stop() during the attempt, then it returns cleanly

    await client.run_forever()

    assert client.connect_attempts == 1
    assert client._stop.is_set()
