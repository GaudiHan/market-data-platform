"""
Three ways to get OHLCV bars into the backtest engine, all zero-cost:

1. TimescaleBarsSource -- reads the continuous aggregates (bars_1m/1h/1d)
   that Layer 2's schema already maintains. The real path, once ingestion
   has been running long enough to accumulate meaningful history.
2. CsvBarsSource -- reads a local CSV, meant to pair with
   scripts/backfill_binance_klines.py, which pulls free historical OHLCV
   directly from Binance's public REST API (no key, no account) so the
   backtest is runnable immediately rather than waiting days/weeks for live
   ingestion to build up history.
3. make_synthetic_bars -- a deterministic synthetic price series, used by
   tests and for exercising the engine without any infrastructure at all.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

BAR_TABLES = {"1m": "bars_1m", "1h": "bars_1h", "1d": "bars_1d"}


class TimescaleBarsSource:
    def __init__(self, dsn: str):
        self.dsn = dsn

    async def load(
        self, exchange: str, symbol: str, interval: str = "1h",
        start=None, end=None,
    ) -> pd.DataFrame:
        import asyncpg

        table = BAR_TABLES.get(interval)
        if table is None:
            raise ValueError(f"unsupported interval '{interval}', expected one of {list(BAR_TABLES)}")

        conditions = ["exchange = $1", "symbol = $2"]
        params = [exchange, symbol]
        if start is not None:
            params.append(start)
            conditions.append(f"bucket >= ${len(params)}")
        if end is not None:
            params.append(end)
            conditions.append(f"bucket <= ${len(params)}")

        query = (
            f"SELECT bucket, open, high, low, close, volume FROM {table} "
            f"WHERE {' AND '.join(conditions)} ORDER BY bucket ASC"
        )

        conn = await asyncpg.connect(self.dsn)
        try:
            rows = await conn.fetch(query, *params)
        finally:
            await conn.close()

        if not rows:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        df = pd.DataFrame([dict(r) for r in rows]).set_index("bucket")
        df.index.name = "timestamp"
        return df


class CsvBarsSource:
    def __init__(self, path: str):
        self.path = path

    def load(self) -> pd.DataFrame:
        df = pd.read_csv(self.path, parse_dates=["timestamp"]).set_index("timestamp")
        return df[["open", "high", "low", "close", "volume"]].sort_index()


def make_synthetic_bars(
    n: int = 300,
    start_price: float = 100.0,
    seed: int = 42,
    mean_reverting: bool = False,
    trend: float = 0.0,
    volatility: float = 1.0,
) -> pd.DataFrame:
    """Deterministic synthetic OHLCV, used by tests and demos. Two regimes:
    a simple Ornstein-Uhlenbeck-style mean-reverting walk (for exercising
    MeanReversionStrategy meaningfully) or a drift+noise random walk (for
    exercising MomentumStrategy meaningfully)."""
    rng = np.random.RandomState(seed)
    prices = np.empty(n)
    prices[0] = start_price

    if mean_reverting:
        theta = 0.15
        for i in range(1, n):
            prices[i] = prices[i - 1] + theta * (start_price - prices[i - 1]) + rng.normal(0, volatility)
    else:
        steps = rng.normal(trend, volatility, n - 1)
        prices[1:] = start_price + np.cumsum(steps)

    prices = np.maximum(prices, 0.01)  # keep prices positive for degenerate seeds
    idx = pd.date_range("2024-01-01", periods=n, freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "open": prices,
            "high": prices + rng.uniform(0, volatility, n),
            "low": prices - rng.uniform(0, volatility, n),
            "close": prices,
            "volume": rng.uniform(1, 10, n),
        },
        index=idx,
    )
