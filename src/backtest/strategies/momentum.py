"""
Mechanical momentum strategy: fast moving average above slow -> long
(trend up), fast below slow -> short (trend down). Classic dual-MA
crossover, fixed windows, no fitting.
"""
from __future__ import annotations

import pandas as pd

from src.backtest.strategies.base import Signal, Strategy


class MomentumStrategy(Strategy):
    name = "momentum"

    def __init__(self, fast: int = 10, slow: int = 30):
        if fast >= slow:
            raise ValueError("fast window must be shorter than slow window")
        self.fast = fast
        self.slow = slow

    def warmup_bars(self) -> int:
        return self.slow

    def generate_signal(self, history: pd.DataFrame) -> Signal:
        if len(history) < self.slow:
            return Signal.FLAT

        fast_ma = history["close"].iloc[-self.fast:].mean()
        slow_ma = history["close"].iloc[-self.slow:].mean()

        if fast_ma > slow_ma:
            return Signal.LONG
        if fast_ma < slow_ma:
            return Signal.SHORT
        return Signal.FLAT
