"""
Fee model. Slippage is NOT modeled here as a flat percentage -- that's the
execution simulator's job (src/backtest/execution.py), which derives
slippage honestly from walking the reconstructed order book rather than
assuming a fixed number. This module only covers the exchange's taker fee,
which really is a fixed rate.
"""
from __future__ import annotations

# 10 bps (0.10%) approximates a typical retail taker fee on Binance/Coinbase
# before any volume-tier discounts. Deliberately not zero -- a backtest that
# ignores fees entirely overstates strategy performance, especially for
# mean-reversion strategies that tend to trade frequently.
DEFAULT_TAKER_FEE_BPS = 10.0


def calculate_fee(notional: float, fee_bps: float = DEFAULT_TAKER_FEE_BPS) -> float:
    """Fee in quote currency for a fill of the given notional value.
    Always non-negative regardless of trade direction (notional may be
    signed elsewhere; this function only cares about magnitude)."""
    return abs(notional) * (fee_bps / 10_000)
