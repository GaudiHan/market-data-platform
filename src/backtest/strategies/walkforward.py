"""
Walk-forward split: the discipline of evaluating a strategy over sequential,
non-overlapping windows where each test window comes strictly after the
window before it, rather than evaluating on one big randomly-shuffled blob
of history (which would let information from the "future" relative to any
given test point leak in via global statistics, cross-validation shuffling,
etc.). The strategies here are mechanical/parameter-free, so the "train"
portion of each fold isn't used for fitting -- it exists so the split
structure itself is real and testable (tests/backtest_validity verifies
folds are sequential and non-overlapping), and so this same splitter is
ready to support parameter search later without changing its interface.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Fold:
    train_start: int
    train_end: int   # exclusive
    test_start: int  # == train_end; test always begins exactly where train ends
    test_end: int    # exclusive


class WalkForwardSplitter:
    def __init__(self, train_size: int, test_size: int, step: int | None = None):
        if train_size <= 0 or test_size <= 0:
            raise ValueError("train_size and test_size must be positive")
        self.train_size = train_size
        self.test_size = test_size
        self.step = step or test_size  # default: non-overlapping test windows

    def split(self, n_bars: int) -> list[Fold]:
        folds = []
        start = 0
        while start + self.train_size + self.test_size <= n_bars:
            train_end = start + self.train_size
            test_end = train_end + self.test_size
            folds.append(Fold(train_start=start, train_end=train_end, test_start=train_end, test_end=test_end))
            start += self.step
        return folds
