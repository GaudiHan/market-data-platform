"""
This is where "handling out-of-order messages correctly" actually lives.
The order book (book.py) doesn't know or care about sequencing -- it just
applies levels. Everything about *when it's safe* to apply a diff, and what
to do when it isn't, is here, because the two exchanges give genuinely
different guarantees:

Binance: every depth-diff carries (U, u) = (firstUpdateId, lastUpdateId).
Official reconciliation procedure (implemented in BinanceReconciler):
  1. Buffer diffs as they arrive.
  2. When a REST snapshot arrives (with its own lastUpdateId), drop any
     buffered diff whose u <= snapshot.lastUpdateId (it's already reflected).
  3. The first diff you apply on top of the snapshot must satisfy
     U <= snapshot.lastUpdateId + 1 <= u.
  4. Every diff after that must have U == previous diff's u + 1. If not,
     you've missed an update -- the local book is no longer trustworthy and
     must be rebuilt from a fresh snapshot.

Coinbase: the public level2 channel carries no per-message sequence number
at all (documented in coinbase_client.py). There is no way to detect a
missed update mid-stream from the data itself. The only correctness
guarantee available is: apply the snapshot, then apply diffs in the order
received, and treat "connection dropped" as "state is stale, wait for the
next snapshot" -- so CoinbaseReconciler is intentionally simpler and relies
on connection-level resync rather than sequence-level resync.

Both reconcilers share one behavior via ReconcilingBookManager: a
`resync_cb(exchange, symbol)` callback they invoke when they need a fresh
snapshot without necessarily waiting for a full reconnect (Binance can hit a
sequence gap on an otherwise-healthy connection; that callback is wired to
IngestionManager.request_resync, which re-fetches a REST snapshot for
Binance or re-subscribes for Coinbase -- see src/ingestion/manager.py).

Note on "out of order" scope: within a single WebSocket connection, TCP
guarantees in-order delivery, so a later update_id never arrives before an
earlier one from the same stream. "Out of order" in practice means diffs
racing the REST snapshot fetch (handled by buffering below) and missed
updates after a gap (handled by resync below) -- a reordering buffer for
genuinely out-of-sequence delivery within one connection isn't needed and
isn't implemented.
"""
from __future__ import annotations

import abc
import logging
from collections import deque
from typing import Callable

from src.common.events import BookDiff, BookSnapshot, ConnectionEvent
from src.orderbook.book import OrderBook

logger = logging.getLogger(__name__)

ResyncCallback = Callable[[str, str], None]


class ReconcilingBookManager(abc.ABC):
    def __init__(self, exchange: str, symbol: str, resync_cb: ResyncCallback | None = None):
        self.exchange = exchange
        self.symbol = symbol
        self.book = OrderBook(exchange, symbol)
        self._resync_cb = resync_cb
        self._pending_diffs: deque[BookDiff] = deque()
        self._max_pending = 5000  # safety valve so a stuck resync can't leak memory
        self._synced = False

    @property
    def is_synced(self) -> bool:
        return self._synced

    @abc.abstractmethod
    def handle_snapshot(self, snapshot: BookSnapshot) -> None: ...

    @abc.abstractmethod
    def handle_diff(self, diff: BookDiff) -> None: ...

    def handle_connection_event(self, event: ConnectionEvent) -> None:
        """A dropped connection means the local book can no longer be
        trusted -- discard it and wait for the next snapshot rather than
        silently continuing to serve stale state."""
        if event.kind in ("disconnected", "resync_required"):
            logger.info("%s %s: connection event '%s' -- clearing book, awaiting resync",
                        self.exchange, self.symbol, event.kind)
            self._synced = False
            self.book.clear()
            self._pending_diffs.clear()

    def _bound_buffer(self) -> None:
        while len(self._pending_diffs) > self._max_pending:
            self._pending_diffs.popleft()

    def _request_resync(self) -> None:
        if self._resync_cb is not None:
            self._resync_cb(self.exchange, self.symbol)


class BinanceReconciler(ReconcilingBookManager):
    def __init__(self, exchange: str, symbol: str, resync_cb: ResyncCallback | None = None):
        super().__init__(exchange, symbol, resync_cb)
        self._awaiting_first_diff = False  # snapshot applied, haven't attached a diff to it yet

    def handle_snapshot(self, snapshot: BookSnapshot) -> None:
        self.book.apply_snapshot(snapshot)
        self._synced = False
        self._awaiting_first_diff = True
        self._drain_buffer()

    def handle_diff(self, diff: BookDiff) -> None:
        if not self._synced:
            self._pending_diffs.append(diff)
            self._bound_buffer()
            if self.book.last_update_id is not None:
                self._drain_buffer()
            return

        if diff.first_update_id == self.book.last_update_id + 1:
            self.book.apply_levels(diff.levels, update_id=diff.last_update_id)
        else:
            logger.warning(
                "binance %s: sequence gap (expected U=%s, got U=%s) -- resyncing",
                self.symbol, self.book.last_update_id + 1, diff.first_update_id,
            )
            self._synced = False
            self._awaiting_first_diff = True
            self.book.clear()
            self._pending_diffs.clear()
            self._request_resync()

    def _drain_buffer(self) -> None:
        """Apply as many buffered diffs as are currently valid, in order,
        stopping cleanly at the first one that isn't safe to apply yet
        (rather than guessing) or that reveals a gap."""
        while self._pending_diffs:
            diff = self._pending_diffs[0]

            if diff.last_update_id <= self.book.last_update_id:
                self._pending_diffs.popleft()  # already reflected in the snapshot
                continue

            if self._awaiting_first_diff:
                if diff.first_update_id <= self.book.last_update_id + 1 <= diff.last_update_id:
                    self.book.apply_levels(diff.levels, update_id=diff.last_update_id)
                    self._pending_diffs.popleft()
                    self._awaiting_first_diff = False
                    self._synced = True
                    continue
                # Buffered diffs don't yet bridge the snapshot -- wait for a
                # later diff to arrive; don't guess.
                break

            if diff.first_update_id == self.book.last_update_id + 1:
                self.book.apply_levels(diff.levels, update_id=diff.last_update_id)
                self._pending_diffs.popleft()
                continue

            logger.warning(
                "binance %s: sequence gap found while draining buffer -- resyncing", self.symbol
            )
            self._synced = False
            self._awaiting_first_diff = True
            self.book.clear()
            self._pending_diffs.clear()
            self._request_resync()
            break


class CoinbaseReconciler(ReconcilingBookManager):
    """No sequence numbers available (see module docstring) -- correctness
    here means "apply the snapshot, then apply diffs in arrival order," and
    leaning on connection-level resync for the failure case rather than
    sequence-gap detection, which the feed doesn't give us the data for."""

    def handle_snapshot(self, snapshot: BookSnapshot) -> None:
        self.book.apply_snapshot(snapshot)
        self._synced = True
        # Any diffs that arrived while we were waiting for this snapshot are
        # still valid -- Coinbase gives no sequence info to check them
        # against, so "correct" means apply them in the order they arrived.
        while self._pending_diffs:
            diff = self._pending_diffs.popleft()
            self.book.apply_levels(diff.levels)

    def handle_diff(self, diff: BookDiff) -> None:
        if not self._synced:
            self._pending_diffs.append(diff)
            self._bound_buffer()
            return
        self.book.apply_levels(diff.levels)
