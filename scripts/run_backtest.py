"""
Runs both mechanical strategies over historical bars using walk-forward
folds, printing risk-adjusted results for each fold plus an aggregate.

Data source priority:
  1. CSV backfill at data/historical/{symbol}_{interval}.csv, if present
     (see scripts/backfill_binance_klines.py -- run that first if you
     haven't yet, it's free and takes a few seconds)
  2. TimescaleDB continuous aggregates, if the CSV isn't there
  3. Synthetic data, as a last-resort demo so this script always runs

Execution model: if TimescaleDB has any book_events rows for the chosen
exchange/symbol (accumulated by scripts/run_pipeline.py), trades are
executed against the reconstructed historical order book via
OrderBookReplayer -- real slippage from real depth, not an assumption.
Otherwise it falls back to a documented flat-slippage model per bar. Pass
--no-order-book to force the fallback even when book history exists.

Usage:
    python -m scripts.backfill_binance_klines --symbol BTC-USD --interval 1h
    python -m scripts.run_backtest --symbol BTC-USD --interval 1h
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, ".")

from src.backtest.data import CsvBarsSource, TimescaleBarsSource, make_synthetic_bars
from src.backtest.engine import BacktestEngine
from src.backtest.strategies.mean_reversion import MeanReversionStrategy
from src.backtest.strategies.momentum import MomentumStrategy
from src.backtest.walkforward import WalkForwardSplitter
from src.config import settings
from src.orderbook.replay import OrderBookReplayer

PERIODS_PER_YEAR = {"1m": 60 * 24 * 365, "5m": 12 * 24 * 365, "15m": 4 * 24 * 365,
                     "1h": 24 * 365, "4h": 6 * 365, "1d": 365}


async def load_bars(symbol: str, interval: str, exchange: str):
    csv_path = os.path.join("data", "historical", f"{symbol}_{interval}.csv")
    if os.path.exists(csv_path):
        print(f"Loading bars from {csv_path}")
        return CsvBarsSource(csv_path).load()

    print("No CSV backfill found, trying TimescaleDB...")
    try:
        source = TimescaleBarsSource(settings.timescale.dsn)
        df = await source.load(exchange, symbol, interval)
        if not df.empty:
            return df
        print("TimescaleDB reachable but has no bars for this symbol/interval yet.")
    except Exception as exc:  # noqa: BLE001 -- any DB problem should fall through, not crash the script
        print(f"Could not read from TimescaleDB ({exc.__class__.__name__}: {exc}).")

    print("Using synthetic data as a demo -- run scripts/backfill_binance_klines.py "
          "or let ingestion run for a while for real data.")
    return make_synthetic_bars(n=500, seed=7)


async def build_order_book_provider(exchange: str, symbol: str):
    """Returns (provider, pool). provider is an async callable usable
    directly as BacktestEngine.run's order_book_provider, or None if no
    book_events history is available for this exchange/symbol -- in which
    case the engine falls back to its documented flat-slippage model.
    Caller is responsible for closing `pool` when done, if not None."""
    import asyncpg

    try:
        pool = await asyncpg.create_pool(settings.timescale.dsn, min_size=1, max_size=3)
    except Exception as exc:  # noqa: BLE001 -- no book-history execution is a graceful degradation, not a crash
        print(f"No order-book history available ({exc.__class__.__name__}) -- using flat-slippage fallback.")
        return None, None

    async with pool.acquire() as conn:
        row_count = await conn.fetchval(
            "SELECT count(*) FROM book_events WHERE exchange = $1 AND symbol = $2", exchange, symbol,
        )
    if not row_count:
        await pool.close()
        print("TimescaleDB reachable but has no book_events for this symbol yet "
              "(run scripts/run_pipeline.py for a while to accumulate some) "
              "-- using flat-slippage fallback.")
        return None, None

    print(f"Found {row_count:,} book_events rows for {exchange}:{symbol} -- "
          "executing trades against the reconstructed order book.")
    replayer = OrderBookReplayer(pool)

    async def provider(ts):
        return await replayer.reconstruct(exchange, symbol, ts.to_pydatetime())

    return provider, pool


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTC-USD")
    parser.add_argument("--interval", default="1h", choices=list(PERIODS_PER_YEAR))
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--train-size", type=int, default=200)
    parser.add_argument("--test-size", type=int, default=100)
    parser.add_argument("--no-order-book", action="store_true",
                         help="Skip order-book execution even if book_events history exists "
                              "(always use the flat-slippage fallback).")
    args = parser.parse_args()

    bars = await load_bars(args.symbol, args.interval, args.exchange)
    print(f"Loaded {len(bars)} bars from {bars.index.min()} to {bars.index.max()}\n")

    splitter = WalkForwardSplitter(train_size=args.train_size, test_size=args.test_size)
    folds = splitter.split(len(bars))
    if not folds:
        print(f"Not enough bars ({len(bars)}) for even one walk-forward fold "
              f"(need >= {args.train_size + args.test_size}). Reduce --train-size/--test-size.")
        return

    order_book_provider, pool = (None, None) if args.no_order_book else await build_order_book_provider(
        args.exchange, args.symbol
    )
    print()

    engine = BacktestEngine(periods_per_year=PERIODS_PER_YEAR[args.interval])
    strategies = [MeanReversionStrategy(), MomentumStrategy()]

    try:
        for strategy in strategies:
            print(f"=== {strategy.name} ===")
            for fold_i, fold in enumerate(folds):
                result = await engine.run(
                    bars, strategy, start_idx=fold.test_start, end_idx=fold.test_end,
                    order_book_provider=order_book_provider,
                )
                print(f"  fold {fold_i} [{bars.index[fold.test_start]} .. {bars.index[fold.test_end - 1]}]: "
                      f"{result.summary()}")
            print()
    finally:
        if pool is not None:
            await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
