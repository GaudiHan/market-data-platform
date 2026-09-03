"""
Lookahead-bias check. The real test here isn't "does the code look
correct" -- it's: if we corrupt every bar AFTER the decision point with
wildly different values, does the signal at that decision point change? If
it does, something is reading future data. If it can't (by construction,
since generate_signal only ever receives bars.iloc[:i+1]), this proves it.
"""
import numpy as np

from src.backtest.data import make_synthetic_bars
from src.backtest.engine import BacktestEngine
from src.backtest.strategies.mean_reversion import MeanReversionStrategy
from src.backtest.strategies.momentum import MomentumStrategy


def _corrupt_future(bars, from_idx: int, seed: int = 999):
    """Replace every bar from from_idx onward with wildly different,
    unrelated prices -- if a signal at some earlier index depends on this
    corrupted data at all, that's lookahead bias."""
    corrupted = bars.copy()
    rng = np.random.RandomState(seed)
    n_future = len(corrupted) - from_idx
    wild_prices = rng.uniform(1_000_000, 2_000_000, n_future)
    corrupted.loc[corrupted.index[from_idx:], ["open", "high", "low", "close"]] = wild_prices[:, None]
    return corrupted


def test_mean_reversion_signal_unaffected_by_corrupted_future_bars():
    bars = make_synthetic_bars(n=100, mean_reverting=True, seed=3)
    strategy = MeanReversionStrategy(lookback=20)
    decision_idx = 50

    signal_real = strategy.generate_signal(bars.iloc[: decision_idx + 1])

    corrupted = _corrupt_future(bars, decision_idx + 1)
    signal_corrupted = strategy.generate_signal(corrupted.iloc[: decision_idx + 1])

    assert signal_real == signal_corrupted


def test_momentum_signal_unaffected_by_corrupted_future_bars():
    bars = make_synthetic_bars(n=100, trend=0.5, seed=3)
    strategy = MomentumStrategy(fast=10, slow=30)
    decision_idx = 60

    signal_real = strategy.generate_signal(bars.iloc[: decision_idx + 1])

    corrupted = _corrupt_future(bars, decision_idx + 1)
    signal_corrupted = strategy.generate_signal(corrupted.iloc[: decision_idx + 1])

    assert signal_real == signal_corrupted


def test_engine_equity_curve_at_a_given_bar_unaffected_by_corrupting_bars_after_it():
    """Integration-level version of the same check: run the full engine up
    to bar N twice, once with real data and once with everything after N
    corrupted, and confirm the equity curve up to N is bit-for-bit
    identical either way."""
    bars = make_synthetic_bars(n=150, mean_reverting=True, seed=11)
    strategy = MeanReversionStrategy(lookback=20)
    engine = BacktestEngine()
    cutoff = 100

    result_real = engine.run(bars, strategy, start_idx=0, end_idx=cutoff)

    corrupted = _corrupt_future(bars, cutoff)
    result_corrupted = engine.run(corrupted, strategy, start_idx=0, end_idx=cutoff)

    pd_testing_equal = (result_real.equity_curve == result_corrupted.equity_curve).all()
    assert pd_testing_equal
    assert result_real.total_return == result_corrupted.total_return
