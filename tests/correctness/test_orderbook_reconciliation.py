"""
Correctness test: out-of-order / gap handling. This is the marquee
market-microstructure test -- it exercises the actual reconciliation
algorithm in src/orderbook/reconciler.py, not just the book data structure.
"""
from src.common.events import BookDiff, BookLevel, BookSide, BookSnapshot, ConnectionEvent
from src.orderbook.reconciler import BinanceReconciler, CoinbaseReconciler


def _snapshot(exchange, symbol, bids, asks, last_update_id):
    return BookSnapshot(
        ts_ns=0, exchange=exchange, symbol=symbol,
        bids=tuple(BookLevel(price=p, size=s, side=BookSide.BID) for p, s in bids),
        asks=tuple(BookLevel(price=p, size=s, side=BookSide.ASK) for p, s in asks),
        last_update_id=last_update_id,
    )


def _diff(exchange, symbol, levels, first_uid=None, last_uid=None):
    return BookDiff(
        ts_ns=0, exchange=exchange, symbol=symbol,
        levels=tuple(BookLevel(price=p, size=s, side=side) for p, s, side in levels),
        first_update_id=first_uid, last_update_id=last_uid,
    )


# ---------------------------------------------------------------------------
# Binance: sequence-numbered gap detection
# ---------------------------------------------------------------------------

def test_binance_diffs_arriving_before_snapshot_are_buffered_then_applied():
    """Exactly the documented Binance procedure: diffs can arrive while the
    REST snapshot request is still in flight. They must be buffered, not
    dropped, and applied once the snapshot lands."""
    r = BinanceReconciler("binance", "BTC-USD")

    # Diffs arrive first (snapshot request is still in flight)
    r.handle_diff(_diff("binance", "BTC-USD", [(100.0, 1.0, BookSide.BID)], first_uid=5, last_uid=5))
    r.handle_diff(_diff("binance", "BTC-USD", [(100.0, 2.0, BookSide.BID)], first_uid=6, last_uid=6))
    assert not r.is_synced
    assert r.book.best_bid() is None  # nothing applied yet, no snapshot to anchor to

    # Now the snapshot lands, with lastUpdateId=4 -- both buffered diffs (5,5) and (6,6)
    # are newer and should bridge cleanly: first qualifying diff has U=5 <= 4+1=5 <= u=5.
    r.handle_snapshot(_snapshot("binance", "BTC-USD", bids=[(99.0, 1.0)], asks=[(101.0, 1.0)], last_update_id=4))

    assert r.is_synced
    assert r.book.best_bid() == (100.0, 2.0)  # diff 6 applied after diff 5, size replaced
    assert r.book.last_update_id == 6


def test_binance_stale_buffered_diffs_are_dropped_not_misapplied():
    """A diff fully covered by the snapshot's lastUpdateId must be dropped,
    not re-applied -- re-applying it would be harmless here but the
    real-world equivalent (out-of-order re-delivery) must not corrupt state."""
    r = BinanceReconciler("binance", "BTC-USD")

    r.handle_diff(_diff("binance", "BTC-USD", [(999.0, 1.0, BookSide.BID)], first_uid=1, last_uid=1))
    r.handle_snapshot(_snapshot("binance", "BTC-USD", bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], last_update_id=10))

    # The stale diff (u=1 <= snapshot lastUpdateId=10) must be dropped, not
    # applied -- price 999 should never appear in the book.
    assert 999.0 not in r.book.bids
    assert not r.is_synced  # still waiting for a diff that actually bridges update_id 10->11


def test_binance_sequence_gap_triggers_resync_and_clears_book():
    resync_calls = []
    r = BinanceReconciler("binance", "BTC-USD", resync_cb=lambda ex, sym: resync_calls.append((ex, sym)))

    r.handle_snapshot(_snapshot("binance", "BTC-USD", bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], last_update_id=10))
    r.handle_diff(_diff("binance", "BTC-USD", [(100.0, 2.0, BookSide.BID)], first_uid=11, last_uid=11))
    assert r.is_synced
    assert r.book.last_update_id == 11

    # Next diff should have U=12 but jumps to U=15 -- a gap. Book must be
    # cleared (not left in a half-updated, untrustworthy state) and a resync
    # must be requested.
    r.handle_diff(_diff("binance", "BTC-USD", [(200.0, 1.0, BookSide.BID)], first_uid=15, last_uid=15))

    assert not r.is_synced
    assert r.book.best_bid() is None  # cleared, not left showing stale/partial state
    assert resync_calls == [("binance", "BTC-USD")]


def test_binance_recovers_cleanly_after_resync_snapshot_arrives():
    r = BinanceReconciler("binance", "BTC-USD", resync_cb=lambda ex, sym: None)

    r.handle_snapshot(_snapshot("binance", "BTC-USD", bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], last_update_id=10))
    r.handle_diff(_diff("binance", "BTC-USD", [(100.0, 2.0, BookSide.BID)], first_uid=11, last_uid=11))
    r.handle_diff(_diff("binance", "BTC-USD", [(200.0, 1.0, BookSide.BID)], first_uid=15, last_uid=15))  # gap
    assert not r.is_synced

    # A fresh snapshot (from the resync) arrives -- book must rebuild cleanly
    # from it, with no leftover state from before the gap.
    r.handle_snapshot(_snapshot("binance", "BTC-USD", bids=[(500.0, 3.0)], asks=[(501.0, 1.0)], last_update_id=100))
    r.handle_diff(_diff("binance", "BTC-USD", [(500.0, 4.0, BookSide.BID)], first_uid=101, last_uid=101))

    assert r.is_synced
    assert r.book.best_bid() == (500.0, 4.0)
    assert 100.0 not in r.book.bids and 200.0 not in r.book.bids


def test_binance_disconnect_clears_book_and_requires_resync():
    r = BinanceReconciler("binance", "BTC-USD")
    r.handle_snapshot(_snapshot("binance", "BTC-USD", bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], last_update_id=10))
    r.handle_diff(_diff("binance", "BTC-USD", [(100.0, 2.0, BookSide.BID)], first_uid=11, last_uid=11))
    assert r.is_synced

    r.handle_connection_event(ConnectionEvent(exchange="binance", symbol="*", kind="disconnected"))

    assert not r.is_synced
    assert r.book.best_bid() is None


def test_binance_multiple_buffered_diffs_all_apply_once_snapshot_bridges():
    """Several diffs can pile up in the buffer while the REST snapshot
    request is in flight -- all of them must end up applied, in the order
    received, once the snapshot arrives (not just the first one)."""
    r = BinanceReconciler("binance", "BTC-USD")

    r.handle_diff(_diff("binance", "BTC-USD", [(100.0, 1.0, BookSide.BID)], first_uid=6, last_uid=6))
    r.handle_diff(_diff("binance", "BTC-USD", [(100.0, 2.0, BookSide.BID)], first_uid=7, last_uid=7))
    r.handle_diff(_diff("binance", "BTC-USD", [(100.0, 3.0, BookSide.BID)], first_uid=8, last_uid=8))

    r.handle_snapshot(_snapshot("binance", "BTC-USD", bids=[(99.0, 1.0)], asks=[(101.0, 1.0)], last_update_id=5))

    assert r.is_synced
    assert r.book.bids[100.0] == 3.0  # last diff (uid 8) is the final state
    assert r.book.last_update_id == 8


# ---------------------------------------------------------------------------
# Coinbase: no sequence numbers -- connection-level resync only
# ---------------------------------------------------------------------------

def test_coinbase_diffs_before_snapshot_are_buffered_then_applied_in_order():
    r = CoinbaseReconciler("coinbase", "BTC-USD")

    r.handle_diff(_diff("coinbase", "BTC-USD", [(100.0, 1.0, BookSide.BID)]))
    r.handle_diff(_diff("coinbase", "BTC-USD", [(100.0, 2.0, BookSide.BID)]))
    assert not r.is_synced

    r.handle_snapshot(_snapshot("coinbase", "BTC-USD", bids=[(99.0, 1.0)], asks=[(101.0, 1.0)], last_update_id=None))

    assert r.is_synced
    # Coinbase gives us no sequence info, so "correct" here means applied in
    # arrival order -- the last one buffered wins.
    assert r.book.bids[100.0] == 2.0


def test_coinbase_disconnect_clears_book_and_desyncs():
    r = CoinbaseReconciler("coinbase", "BTC-USD")
    r.handle_snapshot(_snapshot("coinbase", "BTC-USD", bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], last_update_id=None))
    r.handle_diff(_diff("coinbase", "BTC-USD", [(100.0, 5.0, BookSide.BID)]))
    assert r.is_synced

    r.handle_connection_event(ConnectionEvent(exchange="coinbase", symbol="*", kind="disconnected"))

    assert not r.is_synced
    assert r.book.best_bid() is None


def test_coinbase_fresh_snapshot_after_disconnect_rebuilds_cleanly():
    r = CoinbaseReconciler("coinbase", "BTC-USD")
    r.handle_snapshot(_snapshot("coinbase", "BTC-USD", bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], last_update_id=None))
    r.handle_connection_event(ConnectionEvent(exchange="coinbase", symbol="*", kind="disconnected"))

    r.handle_snapshot(_snapshot("coinbase", "BTC-USD", bids=[(200.0, 5.0)], asks=[(201.0, 1.0)], last_update_id=None))

    assert r.is_synced
    assert r.book.best_bid() == (200.0, 5.0)
    assert 100.0 not in r.book.bids
