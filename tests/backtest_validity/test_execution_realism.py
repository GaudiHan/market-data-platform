import pytest

from src.backtest.execution import ExecutionSimulator
from src.common.events import BookLevel, BookSide, BookSnapshot, Side
from src.orderbook.book import OrderBook


def _book(bids, asks):
    book = OrderBook("test", "BTC-USD")
    book.apply_snapshot(BookSnapshot(
        ts_ns=0, exchange="test", symbol="BTC-USD",
        bids=tuple(BookLevel(price=p, size=s, side=BookSide.BID) for p, s in bids),
        asks=tuple(BookLevel(price=p, size=s, side=BookSide.ASK) for p, s in asks),
        last_update_id=1,
    ))
    return book


def test_small_buy_fills_entirely_at_best_ask():
    book = _book(bids=[(99.0, 10.0)], asks=[(100.0, 5.0), (101.0, 5.0)])
    sim = ExecutionSimulator()

    fill = sim.simulate_market_order(book, Side.BUY, qty=2.0)

    assert fill.is_full_fill
    assert fill.avg_price == 100.0  # entirely within the first level
    assert fill.slippage_bps() == 0.0


def test_large_buy_walks_multiple_levels_worse_than_best_price():
    book = _book(bids=[(99.0, 10.0)], asks=[(100.0, 2.0), (101.0, 3.0), (102.0, 10.0)])
    sim = ExecutionSimulator()

    fill = sim.simulate_market_order(book, Side.BUY, qty=6.0)

    assert fill.is_full_fill
    # 2 @ 100 + 3 @ 101 + 1 @ 102 = 200 + 303 + 102 = 605 / 6 = 100.8333...
    assert fill.avg_price == pytest.approx(605 / 6)
    assert fill.avg_price > 100.0  # worse than best ask -- this IS the slippage
    assert fill.slippage_bps() > 0  # positive = worse for the trader, by convention


def test_sell_walks_bid_side_from_best_price_down():
    book = _book(bids=[(100.0, 2.0), (99.0, 3.0), (98.0, 10.0)], asks=[(101.0, 10.0)])
    sim = ExecutionSimulator()

    fill = sim.simulate_market_order(book, Side.SELL, qty=6.0)

    assert fill.is_full_fill
    assert fill.avg_price == pytest.approx((2 * 100 + 3 * 99 + 1 * 98) / 6)
    assert fill.avg_price < 100.0  # worse than best bid
    assert fill.slippage_bps() > 0  # still positive = worse for the trader (sell got less)


def test_order_larger_than_available_depth_partially_fills_with_shortfall():
    book = _book(bids=[(99.0, 10.0)], asks=[(100.0, 1.0), (101.0, 1.0)])
    sim = ExecutionSimulator()

    fill = sim.simulate_market_order(book, Side.BUY, qty=10.0)

    assert not fill.is_full_fill
    assert fill.filled_qty == 2.0  # only 1+1 available across both levels
    assert fill.shortfall == 8.0
    assert fill.avg_price == pytest.approx(100.5)


def test_empty_book_side_returns_no_fill():
    book = OrderBook("test", "BTC-USD")  # no snapshot applied -- both sides empty
    sim = ExecutionSimulator()

    fill = sim.simulate_market_order(book, Side.BUY, qty=1.0)

    assert fill.filled_qty == 0.0
    assert fill.avg_price is None
    assert fill.shortfall == 1.0
    assert fill.slippage_bps() is None


def test_slippage_increases_monotonically_with_order_size_against_fixed_book():
    book = _book(bids=[(99.0, 100.0)], asks=[(100.0, 1.0), (101.0, 1.0), (102.0, 1.0), (103.0, 100.0)])
    sim = ExecutionSimulator()

    small = sim.simulate_market_order(book, Side.BUY, qty=0.5)
    medium = sim.simulate_market_order(book, Side.BUY, qty=2.0)
    large = sim.simulate_market_order(book, Side.BUY, qty=3.5)

    assert small.slippage_bps() <= medium.slippage_bps() <= large.slippage_bps()


def test_rejects_non_positive_quantity():
    book = _book(bids=[(99.0, 1.0)], asks=[(100.0, 1.0)])
    sim = ExecutionSimulator()
    with pytest.raises(ValueError):
        sim.simulate_market_order(book, Side.BUY, qty=0)
    with pytest.raises(ValueError):
        sim.simulate_market_order(book, Side.BUY, qty=-1)
