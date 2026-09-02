"""
Covers the chaos-test requirement: "Feed malformed/unexpected message
payloads, assert graceful handling rather than a crash."

Two distinct categories of "bad" input, handled by two different code paths
in each client -- both count as graceful, but they don't look the same:

1. TRULY_MALFORMED: not valid JSON, or a recognized event type missing
   fields it needs. This hits the except-and-log path and emits a
   `malformed_message` ConnectionEvent.
2. UNRECOGNIZED_BUT_VALID: well-formed JSON with no recognized event type
   (e.g. a control message we don't handle, or an empty object). This is
   logged at debug and silently dropped -- correctly graceful, but it does
   NOT emit a ConnectionEvent, so tests for it only assert "no exception."

These tests call each client's private _handle_message directly (no real
network needed) and also verify one bad message doesn't poison subsequent
valid parsing.
"""
import asyncio

import pytest

from src.ingestion.binance_client import BinanceClient
from src.ingestion.coinbase_client import CoinbaseClient

TRULY_MALFORMED_BINANCE = [
    b"not json at all",
    b"",
    b"null",
    b"[1, 2, 3]",  # valid JSON, not a dict -> outer["stream"] raises TypeError
    b'{"stream": "btcusdt@trade"}',  # missing "data" envelope
    b'{"stream": "x", "data": {"e": "trade"}}',  # recognized type, missing required trade fields
    b'{"stream": "x", "data": {"e": "depthUpdate", "s": "BTCUSDT"}}',  # missing b/a/U/u
]

TRULY_MALFORMED_COINBASE = [
    b"not json at all",
    b"",
    b"null",
    b"[1, 2, 3]",
    b'{"type": "match"}',  # recognized type, missing required match fields
    b'{"type": "snapshot", "product_id": "BTC-USD"}',  # missing bids/asks
    b'{"type": "l2update", "product_id": "BTC-USD"}',  # missing changes/time
]

UNRECOGNIZED_BUT_VALID_BINANCE = [
    b'{"stream": "btcusdt@trade", "data": {"e": "someNewEventType"}}',
    b'{"stream": "btcusdt@trade", "data": {}}',  # no "e" at all -> event_type is None
]

UNRECOGNIZED_BUT_VALID_COINBASE = [
    b"{}",  # no "type" key -> falls through to the unrecognized branch
    b'{"type": "some_future_message_type"}',
]


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", TRULY_MALFORMED_BINANCE)
async def test_binance_malformed_message_emits_event_not_exception(payload):
    queue: asyncio.Queue = asyncio.Queue()
    client = BinanceClient(symbols=["BTC-USD"], out_queue=queue)

    client._handle_message(payload)  # must not raise
    await asyncio.sleep(0)  # let the scheduled ConnectionEvent task run

    event = await asyncio.wait_for(queue.get(), timeout=1)
    assert event.kind == "malformed_message"


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", TRULY_MALFORMED_COINBASE)
async def test_coinbase_malformed_message_emits_event_not_exception(payload):
    queue: asyncio.Queue = asyncio.Queue()
    client = CoinbaseClient(symbols=["BTC-USD"], out_queue=queue)

    client._handle_message(payload)  # must not raise
    await asyncio.sleep(0)

    event = await asyncio.wait_for(queue.get(), timeout=1)
    assert event.kind == "malformed_message"


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", UNRECOGNIZED_BUT_VALID_BINANCE)
async def test_binance_unrecognized_but_valid_is_silently_ignored(payload):
    """Well-formed JSON with a proper envelope but no recognized event type
    should never raise, and shouldn't be reported as an error either -- it's
    not corrupt, we just don't act on it."""
    queue: asyncio.Queue = asyncio.Queue()
    client = BinanceClient(symbols=["BTC-USD"], out_queue=queue)
    client._handle_message(payload)  # must not raise

    await asyncio.sleep(0)
    assert queue.empty()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", UNRECOGNIZED_BUT_VALID_COINBASE)
async def test_coinbase_unrecognized_but_valid_is_silently_ignored(payload):
    queue: asyncio.Queue = asyncio.Queue()
    client = CoinbaseClient(symbols=["BTC-USD"], out_queue=queue)
    client._handle_message(payload)  # must not raise

    await asyncio.sleep(0)
    assert queue.empty()


@pytest.mark.asyncio
async def test_binance_recovers_after_malformed_message():
    """One garbage message must not poison subsequent valid parsing."""
    queue: asyncio.Queue = asyncio.Queue()
    client = BinanceClient(symbols=["BTC-USD"], out_queue=queue)

    client._handle_message(b"garbage")
    await asyncio.sleep(0)
    await queue.get()  # drain the malformed_message ConnectionEvent

    valid = (
        b'{"stream":"btcusdt@trade","data":{"e":"trade","E":1700000000000,'
        b'"T":1700000000000,"s":"BTCUSDT","t":1,"p":"50000.00","q":"0.01","m":false}}'
    )
    client._handle_message(valid)
    trade = await asyncio.wait_for(queue.get(), timeout=1)
    assert trade.symbol == "BTC-USD"
    assert trade.price == 50000.00


@pytest.mark.asyncio
async def test_coinbase_recovers_after_malformed_message():
    queue: asyncio.Queue = asyncio.Queue()
    client = CoinbaseClient(symbols=["BTC-USD"], out_queue=queue)

    client._handle_message(b"garbage")
    await asyncio.sleep(0)
    await queue.get()

    valid = (
        b'{"type":"match","trade_id":1,"product_id":"BTC-USD",'
        b'"time":"2024-01-01T00:00:00.000000Z","size":"0.01","price":"50000.00","side":"sell"}'
    )
    client._handle_message(valid)
    trade = await asyncio.wait_for(queue.get(), timeout=1)
    assert trade.symbol == "BTC-USD"
    assert trade.price == 50000.00
