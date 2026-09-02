"""
Normalized event types. Every exchange client, however different its wire
format, must translate into one of these before anything downstream (storage,
order book, backtest) ever sees it. This is the seam that makes "add a third
exchange later" cheap and keeps exchange-specific parsing out of every other
layer.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class BookSide(str, Enum):
    BID = "bid"
    ASK = "ask"


@dataclass(frozen=True, slots=True)
class Trade:
    ts_ns: int          # exchange-reported trade time, nanoseconds since epoch
    exchange: str
    symbol: str          # common format, e.g. "BTC-USD"
    trade_id: str
    price: float
    size: float
    side: Side           # taker side


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: float
    size: float           # 0 means "remove this price level"
    side: BookSide


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    ts_ns: int
    exchange: str
    symbol: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    last_update_id: int | None = None


@dataclass(frozen=True, slots=True)
class BookDiff:
    ts_ns: int
    exchange: str
    symbol: str
    levels: tuple[BookLevel, ...]
    first_update_id: int | None = None
    last_update_id: int | None = None


@dataclass(frozen=True, slots=True)
class ConnectionEvent:
    """Not market data -- a signal from the ingestion layer that something
    happened to the connection itself. The order book layer and chaos tests
    both key off this to know when a resync is required."""
    exchange: str
    symbol: str
    kind: Literal["connected", "disconnected", "resync_required", "malformed_message"]
    detail: str = ""


NormalizedEvent = Trade | BookSnapshot | BookDiff | ConnectionEvent
