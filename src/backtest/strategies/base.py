"""
Strategy interface. The core design decision here is what "no lookahead
bias" actually means in code, not just in principle: a strategy's
generate_signal() receives a DataFrame that has ALREADY been sliced to end
at the current decision point (see BacktestEngine.run in ../engine.py,
which always passes `bars.iloc[:i+1]`, never the full frame). A strategy
literally cannot read a future bar, because future bars are never in the
object it's holding -- there's no discipline required, no "don't peek"
convention to violate. tests/backtest_validity/test_lookahead.py verifies
this holds by mutating future rows and confirming the signal at a fixed
index never changes.
"""
from __future__ import annotations

import abc
from enum import IntEnum

import pandas as pd


class Signal(IntEnum):
    SHORT = -1
    FLAT = 0
    LONG = 1


class Strategy(abc.ABC):
    name: str

    @abc.abstractmethod
    def generate_signal(self, history: pd.DataFrame) -> Signal:
        """`history` is OHLCV bars up to and including the current bar --
        index -1 is "now". Must return the desired position direction as of
        the close of the current bar. Implementations should only ever
        index from the end (`.iloc[-n:]`) or by label already known to be
        <= the last index; there is no future data available to misuse,
        but keep the habit anyway for anyone extending this."""
        raise NotImplementedError

    def warmup_bars(self) -> int:
        """How many bars of history this strategy needs before its first
        real (non-FLAT-by-default) signal. The engine uses this to skip the
        warmup period rather than start trading noise."""
        return 0
