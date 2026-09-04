import pandas as pd
import pytest

from src.backtest import metrics
from src.backtest.data import make_synthetic_bars
from src.backtest.engine import BacktestEngine
from src.backtest.strategies.momentum import MomentumStrategy


def test_buy_and_hold_return_matches_hand_computed_value():
    prices = pd.Series([100.0, 110.0, 90.0, 120.0])
    assert metrics.buy_and_hold_return(prices) == pytest.approx(0.20)  # 100 -> 120


def test_buy_and_hold_return_zero_for_flat_or_degenerate_series():
    assert metrics.buy_and_hold_return(pd.Series([100.0])) == 0.0
    assert metrics.buy_and_hold_return(pd.Series([])) == 0.0


@pytest.mark.asyncio
async def test_backtest_result_includes_benchmark_alongside_strategy_return():
    bars = make_synthetic_bars(n=200, trend=2.0, volatility=0.5, seed=5)
    engine = BacktestEngine()
    strategy = MomentumStrategy(fast=10, slow=30)

    result = await engine.run(bars, strategy)

    # The benchmark must be computed over the same evaluated window the
    # strategy was scored on, not the whole bars frame (which would include
    # the warmup period the strategy never got to trade during).
    warmup = strategy.warmup_bars()
    expected_benchmark = metrics.buy_and_hold_return(bars["close"].iloc[warmup:])
    assert result.benchmark_return == pytest.approx(expected_benchmark)


def test_sharpe_is_zero_not_nan_for_a_flat_equity_curve():
    flat_curve = pd.Series([100.0] * 10)
    returns = flat_curve.pct_change().dropna()
    assert metrics.sharpe_ratio(returns, periods_per_year=365) == 0.0


def test_max_drawdown_matches_hand_computed_value():
    curve = pd.Series([100.0, 120.0, 90.0, 95.0, 130.0])
    # Peak 120 -> trough 90 is the worst drawdown: (90-120)/120 = -0.25
    assert metrics.max_drawdown(curve) == pytest.approx(-0.25)


def test_total_return_matches_hand_computed_value():
    curve = pd.Series([100.0, 105.0, 95.0, 111.0])
    assert metrics.total_return(curve) == pytest.approx(0.11)
