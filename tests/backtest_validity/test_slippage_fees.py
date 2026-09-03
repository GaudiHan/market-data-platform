import pytest

from src.backtest.costs import DEFAULT_TAKER_FEE_BPS, calculate_fee
from src.backtest.execution import ExecutionSimulator
from src.common.events import BookLevel, BookSide, BookSnapshot, Side
from src.orderbook.book import OrderBook


def test_fee_is_proportional_to_notional():
    assert calculate_fee(1000.0) == pytest.approx(1000.0 * DEFAULT_TAKER_FEE_BPS / 10_000)
    assert calculate_fee(2000.0) == pytest.approx(2 * calculate_fee(1000.0))


def test_fee_is_always_non_negative_regardless_of_trade_direction():
    assert calculate_fee(1000.0) > 0
    assert calculate_fee(-1000.0) > 0  # a signed (sell) notional still produces a positive fee
    assert calculate_fee(-1000.0) == calculate_fee(1000.0)


def test_fee_zero_for_zero_notional():
    assert calculate_fee(0.0) == 0.0


def test_custom_fee_rate_is_respected():
    assert calculate_fee(1000.0, fee_bps=25.0) == pytest.approx(2.5)


def _book(bids, asks):
    book = OrderBook("test", "BTC-USD")
    book.apply_snapshot(BookSnapshot(
        ts_ns=0, exchange="test", symbol="BTC-USD",
        bids=tuple(BookLevel(price=p, size=s, side=BookSide.BID) for p, s in bids),
        asks=tuple(BookLevel(price=p, size=s, side=BookSide.ASK) for p, s in asks),
        last_update_id=1,
    ))
    return book


def test_slippage_is_zero_for_a_trade_fully_within_the_best_level():
    book = _book(bids=[(99.0, 10.0)], asks=[(100.0, 10.0)])
    fill = ExecutionSimulator().simulate_market_order(book, Side.BUY, qty=1.0)
    assert fill.slippage_bps() == pytest.approx(0.0)


def test_slippage_reflects_thin_book_more_than_deep_book():
    """Same order size, two books with identical best price but different
    depth beyond it -- the thinner book must produce worse (higher)
    slippage for an order that has to walk past the top level."""
    thin_book = _book(bids=[(99.0, 100.0)], asks=[(100.0, 0.5), (110.0, 100.0)])
    deep_book = _book(bids=[(99.0, 100.0)], asks=[(100.0, 0.5), (100.5, 100.0)])
    sim = ExecutionSimulator()

    thin_fill = sim.simulate_market_order(thin_book, Side.BUY, qty=1.0)
    deep_fill = sim.simulate_market_order(deep_book, Side.BUY, qty=1.0)

    assert thin_fill.slippage_bps() > deep_fill.slippage_bps()
