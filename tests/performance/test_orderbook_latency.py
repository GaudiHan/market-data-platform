"""
Order-book update latency: how long does it take to apply one diff to a
live book? This is the tightest hot path in the whole system -- it runs
once per depth-diff message, potentially hundreds of times per second per
symbol -- so it's the one place where an accidentally-quadratic data
structure choice would actually hurt. SortedDict (src/orderbook/book.py)
is O(log n) per update; this benchmark exists to catch a regression away
from that, not to chase a specific absolute number.
"""
from src.common.events import BookDiff, BookLevel, BookSide, BookSnapshot
from src.orderbook.book import OrderBook
from src.orderbook.reconciler import BinanceReconciler
from tests.performance._latency import measure

# Generous ceiling, not a target -- catches a real regression (e.g. someone
# swapping SortedDict for a plain list + linear scan) without being flaky
# on slower CI hardware. A real update should be one to two orders of
# magnitude faster than this in practice.
MAX_ACCEPTABLE_P95_US = 500.0


def _seeded_book(n_levels: int = 500) -> OrderBook:
    """A book with realistic depth (hundreds of price levels each side) --
    benchmarking against an empty or near-empty book would understate the
    cost of the O(log n) operations under real load."""
    book = OrderBook("binance", "BTC-USD")
    bids = tuple(BookLevel(price=100.0 - i * 0.01, size=1.0, side=BookSide.BID) for i in range(n_levels))
    asks = tuple(BookLevel(price=100.0 + i * 0.01, size=1.0, side=BookSide.ASK) for i in range(n_levels))
    book.apply_snapshot(BookSnapshot(
        ts_ns=0, exchange="binance", symbol="BTC-USD", bids=bids, asks=asks, last_update_id=1,
    ))
    return book


def test_single_level_update_latency():
    book = _seeded_book()
    counter = [0]

    def apply_one_update():
        counter[0] += 1
        # Alternate between updating an existing level and adding a new one
        # just outside current depth -- a realistic mix of what live diffs do.
        price = 100.0 - (counter[0] % 500) * 0.01
        book.apply_level(BookLevel(price=price, size=float(counter[0] % 10 + 1), side=BookSide.BID))

    stats = measure(apply_one_update, n=20_000)
    print(f"\n[orderbook] single-level apply: {stats}")

    assert stats.p95_us < MAX_ACCEPTABLE_P95_US


def test_full_reconciled_diff_latency():
    """End-to-end latency through the reconciler (the actual code path a
    live diff message takes), not just the bare data structure -- includes
    the sync-state bookkeeping in BinanceReconciler.handle_diff."""
    r = BinanceReconciler("binance", "BTC-USD")
    r.handle_snapshot(BookSnapshot(
        ts_ns=0, exchange="binance", symbol="BTC-USD",
        bids=(BookLevel(price=100.0, size=1.0, side=BookSide.BID),),
        asks=(BookLevel(price=101.0, size=1.0, side=BookSide.ASK),),
        last_update_id=0,
    ))
    counter = [0]

    def apply_one_diff():
        counter[0] += 1
        uid = counter[0]
        r.handle_diff(BookDiff(
            ts_ns=0, exchange="binance", symbol="BTC-USD",
            levels=(BookLevel(price=100.0 + (uid % 50) * 0.01, size=float(uid % 5 + 1), side=BookSide.BID),),
            first_update_id=uid, last_update_id=uid,
        ))

    stats = measure(apply_one_diff, n=20_000)
    print(f"[orderbook] reconciled diff apply: {stats}")

    assert r.is_synced  # sanity: the benchmark loop itself didn't desync
    assert stats.p95_us < MAX_ACCEPTABLE_P95_US


def test_depth_query_latency():
    """depth() is called by execution simulation and any UI/API layer --
    worth benchmarking separately since it materializes a list rather than
    just reading one key. Tighter ceiling than the other two benchmarks in
    this file: after fixing book.py's depth() to slice the SortedItemsView
    directly instead of materializing the whole book first (a real ~50x
    regression this exact benchmark caught), this should run in low tens of
    microseconds, not hundreds -- this ceiling is deliberately tight enough
    that the old, slow implementation would fail it."""
    book = _seeded_book(n_levels=1000)

    stats = measure(lambda: book.depth(n=20), n=20_000)
    print(f"[orderbook] depth(n=20) query: {stats}")

    assert stats.p95_us < 50.0
