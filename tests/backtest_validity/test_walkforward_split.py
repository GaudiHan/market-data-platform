import pytest

from src.backtest.walkforward import WalkForwardSplitter


def test_folds_are_sequential_and_non_overlapping_by_default():
    splitter = WalkForwardSplitter(train_size=50, test_size=20)
    folds = splitter.split(n_bars=200)

    assert len(folds) > 1
    for fold in folds:
        assert fold.train_start < fold.train_end == fold.test_start < fold.test_end

    # With the default step (== test_size), TEST windows tile the timeline
    # exactly: no gaps, no overlap between consecutive test periods. Train
    # windows sliding forward and overlapping between folds is normal walk-
    # forward behavior (each fold retrains on a shifted window) -- what must
    # never overlap is a fold's own train/test (checked above) and the test
    # windows used to score consecutive folds against each other.
    for a, b in zip(folds, folds[1:]):
        assert a.test_end == b.test_start


def test_train_always_strictly_precedes_test_in_time():
    splitter = WalkForwardSplitter(train_size=30, test_size=10)
    folds = splitter.split(n_bars=100)
    for fold in folds:
        assert fold.train_end == fold.test_start  # test begins exactly where train ends, never before
        assert fold.train_start < fold.train_end
        assert fold.test_start < fold.test_end


def test_no_fold_exceeds_available_bars():
    n_bars = 137
    splitter = WalkForwardSplitter(train_size=50, test_size=30)
    folds = splitter.split(n_bars)
    for fold in folds:
        assert fold.test_end <= n_bars


def test_insufficient_bars_produces_no_folds():
    splitter = WalkForwardSplitter(train_size=100, test_size=50)
    assert splitter.split(n_bars=120) == []


def test_custom_step_allows_overlapping_train_windows_but_folds_still_sequential():
    """A smaller step than test_size means consecutive folds' train windows
    can overlap (useful for more frequent re-evaluation) -- but each
    individual fold's test must still start exactly at its own train end,
    and fold start points must still advance forward in time."""
    splitter = WalkForwardSplitter(train_size=50, test_size=20, step=10)
    folds = splitter.split(n_bars=200)

    assert len(folds) > 1
    for fold in folds:
        assert fold.train_end == fold.test_start
    for a, b in zip(folds, folds[1:]):
        assert b.train_start > a.train_start  # always advancing, never repeating or going backward


def test_rejects_non_positive_sizes():
    with pytest.raises(ValueError):
        WalkForwardSplitter(train_size=0, test_size=10)
    with pytest.raises(ValueError):
        WalkForwardSplitter(train_size=10, test_size=-5)
