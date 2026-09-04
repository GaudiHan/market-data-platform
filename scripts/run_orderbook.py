"""
Layer 1 + 3 wired together: streams live data from both exchanges into
reconstructed L2 order books and periodically prints top-of-book so you can
watch reconstruction happen and (if you kill your network briefly) watch it
resync cleanly.

Usage:
    python -m scripts.run_orderbook
"""
from __future__ import annotations

import asyncio
import logging
import sys

sys.path.insert(0, ".")

from src.config import settings
from src.ingestion.manager import IngestionManager
from src.orderbook.manager import OrderBookRegistry

logger = logging.getLogger(__name__)


async def _print_top_of_book_periodically(registry: OrderBookRegistry, exchanges: list[str], symbols: list[str]):
    while True:
        await asyncio.sleep(3)
        lines = []
        for exchange in exchanges:
            for symbol in symbols:
                tob = registry.top_of_book(exchange, symbol)
                if tob is None:
                    lines.append(f"{exchange:8s} {symbol:8s} (no book yet)")
                else:
                    synced = "SYNCED" if tob["synced"] else "resyncing..."
                    lines.append(
                        f"{exchange:8s} {symbol:8s} bid={tob['best_bid']} ask={tob['best_ask']} "
                        f"spread={tob['spread']} [{synced}]"
                    )
        print("\n".join(lines) + "\n" + "-" * 60)


async def main() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    manager = IngestionManager(settings.symbols)
    registry = OrderBookRegistry(manager)
    await manager.start()

    printer = asyncio.create_task(
        _print_top_of_book_periodically(registry, ["binance", "coinbase"], settings.symbols)
    )
    try:
        await registry.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        printer.cancel()
        await asyncio.gather(printer, return_exceptions=True)
        await manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
