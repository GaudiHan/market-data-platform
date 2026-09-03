from src.backtest.data import make_synthetic_bars
from src.backtest.strategies.base import Signal
from src.backtest.strategies.mean_reversion import MeanReversionStrategy
from src.backtest.strategies.momentum import MomentumStrategy


def test_mean_reversion_flat_during_warmup():
    strategy = MeanReversionStrategy(lookback=20)
    bars = make_synthetic_bars(n=10, mean_reverting=True)
    assert strategy.generate_signal(bars) == Signal.FLAT


def test_mean_reversion_goes_long_when_price_far_below_mean():
    strategy = MeanReversionStrategy(lookback=10, z_entry=1.0)
    bars = make_synthetic_bars(n=10, mean_reverting=True)
    bars = bars.copy()
    bars.loc[bars.index[-1], "close"] = bars["close"].iloc[:-1].mean() - 10 * bars["close"].iloc[:-1].std()
    assert strategy.generate_signal(bars) == Signal.LONG


def test_mean_reversion_goes_short_when_price_far_above_mean():
    strategy = MeanReversionStrategy(lookback=10, z_entry=1.0)
    bars = make_synthetic_bars(n=10, mean_reverting=True)
    bars = bars.copy()
    bars.loc[bars.index[-1], "close"] = bars["close"].iloc[:-1].mean() + 10 * bars["close"].iloc[:-1].std()
    assert strategy.generate_signal(bars) == Signal.SHORT


def test_mean_reversion_flat_within_band():
    strategy = MeanReversionStrategy(lookback=10, z_entry=100.0)  # impossible to breach
    bars = make_synthetic_bars(n=15, mean_reverting=True)
    assert strategy.generate_signal(bars) == Signal.FLAT


def test_momentum_flat_during_warmup():
    strategy = MomentumStrategy(fast=5, slow=20)
    bars = make_synthetic_bars(n=10, trend=1.0)
    assert strategy.generate_signal(bars) == Signal.FLAT


def test_momentum_long_in_strong_uptrend():
    strategy = MomentumStrategy(fast=5, slow=20)
    bars = make_synthetic_bars(n=40, trend=5.0, volatility=0.1, seed=1)
    assert strategy.generate_signal(bars) == Signal.LONG


def test_momentum_short_in_strong_downtrend():
    strategy = MomentumStrategy(fast=5, slow=20)
    bars = make_synthetic_bars(n=40, trend=-5.0, volatility=0.1, seed=1)
    assert strategy.generate_signal(bars) == Signal.SHORT


def test_momentum_rejects_fast_ge_slow():
    import pytest
    with pytest.raises(ValueError):
        MomentumStrategy(fast=20, slow=10)
