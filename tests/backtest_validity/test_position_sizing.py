"""
Position sizing. Split out from test_lookahead.py because this is a
distinct concern (how big a trade is) rather than when a strategy is
allowed to see data.
"""
import pytest

from src.backtest.data import make_synthetic_bars
from src.backtest.engine import BacktestEngine
from src.backtest.strategies.momentum import MomentumStrategy


@pytest.mark.asyncio
async def test_default_sizing_does_not_implicitly_leverage_at_btc_scale_prices():
    """Regression test for the position-sizing bug flagged after a real
    live run: with the OLD default (fixed_units=1.0 against $10,000 cash),
    a single BTC-scale trade at ~$100,000/unit meant ~10x unconstrained
    leverage from the very first trade, producing wildly unrealistic
    equity swings from a handful of trades. The default sizing mode
    (equity_fraction) must keep notional exposure bounded to a fraction of
    current equity regardless of the asset's price scale."""
    bars = make_synthetic_bars(n=200, start_price=100_000.0, trend=50.0, volatility=200.0, seed=4)
    engine = BacktestEngine()  # defaults: equity_fraction, position_fraction=0.5
    strategy = MomentumStrategy(fast=10, slow=30)

    result = await engine.run(bars, strategy)

    assert result.trades, "expected at least one trade to actually check sizing on"
    for trade in result.trades:
        notional = trade.filled_qty * trade.avg_price
        # Notional exposure per trade should be a bounded fraction of the
        # starting cash, not multiples of it -- this is what "not
        # implicitly leveraged" means concretely.
        assert notional < engine.initial_cash * 2, (
            f"trade notional {notional} implies excessive leverage against "
            f"initial cash {engine.initial_cash}"
        )


@pytest.mark.asyncio
async def test_position_fraction_controls_exposure_size():
    """A smaller position_fraction should produce smaller notional per
    trade, roughly proportionally -- confirms the parameter actually does
    what its name says, not just that it exists."""
    bars = make_synthetic_bars(n=200, start_price=50_000.0, trend=20.0, volatility=100.0, seed=6)
    strategy = MomentumStrategy(fast=10, slow=30)

    small = BacktestEngine(sizing_mode="equity_fraction", position_fraction=0.1)
    large = BacktestEngine(sizing_mode="equity_fraction", position_fraction=0.8)

    result_small = await small.run(bars, strategy)
    result_large = await large.run(bars, strategy)

    assert result_small.trades and result_large.trades
    avg_notional_small = sum(t.filled_qty * t.avg_price for t in result_small.trades) / len(result_small.trades)
    avg_notional_large = sum(t.filled_qty * t.avg_price for t in result_large.trades) / len(result_large.trades)
    assert avg_notional_small < avg_notional_large


@pytest.mark.asyncio
async def test_fixed_units_mode_still_available_for_explicit_use():
    """The old behavior isn't removed, just no longer the silent default --
    confirm it's still selectable and behaves as documented: every trade
    requests exactly position_size units (or double that on a direct
    long<->short flip), regardless of price or account size."""
    bars = make_synthetic_bars(n=60, trend=1.0, seed=2)
    engine = BacktestEngine(sizing_mode="fixed_units", position_size=2.0, initial_cash=1_000_000.0)
    strategy = MomentumStrategy(fast=5, slow=20)

    result = await engine.run(bars, strategy)

    assert result.trades
    for trade in result.trades:
        assert trade.requested_qty == pytest.approx(2.0) or trade.requested_qty == pytest.approx(4.0)


def test_rejects_unknown_sizing_mode():
    with pytest.raises(ValueError):
        BacktestEngine(sizing_mode="not_a_real_mode")


def test_equity_fraction_sizing_degrades_gracefully_at_zero_or_negative_equity():
    """A blown account (equity <= 0) must go flat rather than raise or
    divide by zero -- realistic and safe behavior for an edge case a fixed
    fee/slippage drag could genuinely produce over a long backtest."""
    engine = BacktestEngine(sizing_mode="equity_fraction", position_fraction=0.5)
    from src.backtest.strategies.base import Signal
    assert engine._target_position(Signal.LONG, current_equity=0.0, close_price=100.0) == 0.0
    assert engine._target_position(Signal.LONG, current_equity=-500.0, close_price=100.0) == 0.0
    assert engine._target_position(Signal.LONG, current_equity=1000.0, close_price=0.0) == 0.0
