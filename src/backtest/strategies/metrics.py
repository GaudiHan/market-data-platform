"""
Risk-adjusted metrics. Sharpe and max drawdown, not just raw return --
raw return alone can't distinguish "steady modest gains" from "one lucky
spike followed by a wipeout," which is exactly the failure mode these
exist to catch.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def total_return(equity_curve: pd.Series) -> float:
    if len(equity_curve) < 2 or equity_curve.iloc[0] == 0:
        return 0.0
    return float(equity_curve.iloc[-1] / equity_curve.iloc[0] - 1)


def sharpe_ratio(returns: pd.Series, periods_per_year: float) -> float:
    """Annualized Sharpe assuming zero risk-free rate (a reasonable
    simplification for a project-scope backtest; real desks subtract a
    financing rate). periods_per_year must match the bar interval used --
    e.g. 24*365 for hourly bars, 365 for daily -- passed explicitly rather
    than assumed, since a hardcoded default silently produces a wrong
    annualization for whatever data actually gets loaded."""
    if len(returns) < 2 or returns.std() == 0 or pd.isna(returns.std()):
        return 0.0
    return float((returns.mean() / returns.std()) * np.sqrt(periods_per_year))


def max_drawdown(equity_curve: pd.Series) -> float:
    """Largest peak-to-trough decline, as a negative fraction (e.g. -0.23
    means a 23% drawdown at the worst point)."""
    if len(equity_curve) < 2:
        return 0.0
    running_max = equity_curve.cummax()
    drawdowns = (equity_curve - running_max) / running_max
    return float(drawdowns.min())


def buy_and_hold_return(prices: pd.Series) -> float:
    """The benchmark: what you'd have made just holding the asset over the
    same window, no trading at all. A strategy with a worse Sharpe AND a
    worse return than this isn't earning its complexity."""
    if len(prices) < 2 or prices.iloc[0] == 0:
        return 0.0
    return float(prices.iloc[-1] / prices.iloc[0] - 1)
