"""
Mechanical mean-reversion strategy: go long when price is unusually far
below its recent rolling mean (expecting reversion up), short when unusually
far above (expecting reversion down), flat within the band. No fitted
parameters -- lookback/entry threshold are fixed constructor args, not
calibrated on data, which is the honest version of "mechanical strategy."
"""
from __future__ import annotations

import pandas as pd

from src.backtest.strategies.base import Signal, Strategy


class MeanReversionStrategy(Strategy):
    name = "mean_reversion"

    def __init__(self, lookback: int = 20, z_entry: float = 1.0):
        self.lookback = lookback
        self.z_entry = z_entry

    def warmup_bars(self) -> int:
        return self.lookback

    def generate_signal(self, history: pd.DataFrame) -> Signal:
        if len(history) < self.lookback:
            return Signal.FLAT

        window = history["close"].iloc[-self.lookback:]
        mean = window.mean()
        std = window.std()
        if std == 0 or pd.isna(std):
            return Signal.FLAT

        z = (history["close"].iloc[-1] - mean) / std
        if z < -self.z_entry:
            return Signal.LONG   # price unusually low -> bet on reversion up
        if z > self.z_entry:
            return Signal.SHORT  # price unusually high -> bet on reversion down
        return Signal.FLAT
