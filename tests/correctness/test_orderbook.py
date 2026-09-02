"""
Correctness test: order book reconstruction vs. ground truth. Builds a book
from a known snapshot + a known sequence of diffs, and checks the resulting
state against values worked out by hand -- not against the code's own logic.
"""
from src.common.events import BookLevel, BookSide, BookSnapshot
from src.orderbook.book import OrderBook


def _snapshot(bids, asks, last_update_id=1):
    return BookSnapshot(
        ts_ns=0, exchange="test", symbol="BTC-USD",
        bids=tuple(BookLevel(price=p, size=s, side=BookSide.BID) for p, s in bids),
        asks=tuple(BookLevel(price=p, size=s, side=BookSide.ASK) for p, s in asks),
        last_update_id=last_update_id,
    )


def test_snapshot_establishes_best_bid_ask():
    book = OrderBook("test", "BTC-USD")
    book.apply_snapshot(_snapshot(
        bids=[(100.0, 1.0), (99.5, 2.0), (99.0, 3.0)],
        asks=[(100.5, 1.0), (101.0, 2.0)],
    ))
    assert book.best_bid() == (100.0, 1.0)
    assert book.best_ask() == (100.5, 1.0)
    assert book.spread() == 0.5
    assert book.mid_price() == 100.25


def test_zero_size_level_removes_it():
    book = OrderBook("test", "BTC-USD")
    book.apply_snapshot(_snapshot(bids=[(100.0, 1.0), (99.0, 2.0)], asks=[(101.0, 1.0)]))

    book.apply_level(BookLevel(price=100.0, size=0.0, side=BookSide.BID))

    assert book.best_bid() == (99.0, 2.0)
    assert 100.0 not in book.bids


def test_update_existing_level_replaces_size_not_adds():
    book = OrderBook("test", "BTC-USD")
    book.apply_snapshot(_snapshot(bids=[(100.0, 1.0)], asks=[(101.0, 1.0)]))

    book.apply_level(BookLevel(price=100.0, size=5.0, side=BookSide.BID))

    assert book.bids[100.0] == 5.0  # replaced, not 1.0 + 5.0


def test_new_price_level_inserted_in_correct_order():
    book = OrderBook("test", "BTC-USD")
    book.apply_snapshot(_snapshot(bids=[(100.0, 1.0), (98.0, 1.0)], asks=[(101.0, 1.0)]))

    # A new bid between the two existing ones -- becomes the new best bid
    # only if it's above the current best.
    book.apply_level(BookLevel(price=99.0, size=1.0, side=BookSide.BID))
    book.apply_level(BookLevel(price=100.5, size=1.0, side=BookSide.BID))

    assert book.best_bid() == (100.5, 1.0)
    depth = book.depth(n=10)
    assert [p for p, _ in depth["bids"]] == [100.5, 100.0, 99.0, 98.0]  # descending


def test_depth_returns_best_n_levels_in_correct_order():
    book = OrderBook("test", "BTC-USD")
    book.apply_snapshot(_snapshot(
        bids=[(p, 1.0) for p in [100, 99, 98, 97, 96]],
        asks=[(p, 1.0) for p in [101, 102, 103, 104, 105]],
    ))
    depth = book.depth(n=3)
    assert [p for p, _ in depth["bids"]] == [100, 99, 98]
    assert [p for p, _ in depth["asks"]] == [101, 102, 103]


def test_empty_book_reports_no_best_bid_ask():
    book = OrderBook("test", "BTC-USD")
    assert book.best_bid() is None
    assert book.best_ask() is None
    assert book.spread() is None
    assert book.mid_price() is None


def test_total_size_within_sums_correct_levels_only():
    book = OrderBook("test", "BTC-USD")
    book.apply_snapshot(_snapshot(
        bids=[(100.0, 1.0), (99.0, 2.0), (98.0, 3.0)],
        asks=[(101.0, 1.0)],
    ))
    # Only 100.0 and 99.0 fall in [99, 100] -- 98.0 must be excluded.
    total = book.total_size_within(BookSide.BID, 99.0, 100.0)
    assert total == 3.0


def test_apply_snapshot_clears_prior_state():
    book = OrderBook("test", "BTC-USD")
    book.apply_snapshot(_snapshot(bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], last_update_id=1))
    book.apply_snapshot(_snapshot(bids=[(200.0, 1.0)], asks=[(201.0, 1.0)], last_update_id=2))

    assert book.best_bid() == (200.0, 1.0)
    assert 100.0 not in book.bids
    assert book.last_update_id == 2
