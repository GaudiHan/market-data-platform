"""
Pulls historical OHLCV bars directly from Binance's public REST klines
endpoint (no API key, no account -- same zero-budget constraint as
everything else here) and writes them to a local CSV that
src/backtest/data.CsvBarsSource can read. This exists so the backtest
engine is runnable immediately, rather than requiring days/weeks of live
ingestion (Layers 1+2) to accumulate enough history to backtest against.

Usage:
    python -m scripts.backfill_binance_klines --symbol BTC-USD --interval 1h --limit 1000
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

import aiohttp
import pandas as pd

sys.path.insert(0, ".")

from src.common.symbols import to_binance

KLINES_URL = "https://api.binance.com/api/v3/klines"
OUTPUT_DIR = "data/historical"


async def fetch_klines(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    native_symbol = to_binance(symbol).upper()
    params = {"symbol": native_symbol, "interval": interval, "limit": min(limit, 1000)}

    async with aiohttp.ClientSession() as session:
        async with session.get(KLINES_URL, params=params) as resp:
            resp.raise_for_status()
            raw = await resp.json()

    # Binance kline array format: [open_time, open, high, low, close, volume,
    # close_time, quote_asset_volume, trades, taker_buy_base, taker_buy_quote, ignore]
    rows = [
        {
            "timestamp": pd.to_datetime(k[0], unit="ms", utc=True),
            "open": float(k[1]), "high": float(k[2]),
            "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
        }
        for k in raw
    ]
    return pd.DataFrame(rows).set_index("timestamp")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTC-USD", help="Common format, e.g. BTC-USD")
    parser.add_argument("--interval", default="1h", choices=["1m", "5m", "15m", "1h", "4h", "1d"])
    parser.add_argument("--limit", type=int, default=1000, help="Max 1000 per Binance's API limit")
    args = parser.parse_args()

    df = await fetch_klines(args.symbol, args.interval, args.limit)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{args.symbol}_{args.interval}.csv")
    df.to_csv(out_path)
    print(f"Wrote {len(df)} bars ({df.index.min()} to {df.index.max()}) to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
