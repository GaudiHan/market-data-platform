"""
Standalone entrypoint for Layer 1 (Ingestion) on its own, before storage is
wired in. Run this to sanity-check that both exchange connections come up,
trades and book events flow, and reconnect/malformed-message handling
behaves -- before trusting it to write anything to a database.

Usage:
    python -m scripts.run_ingestion
"""
from __future__ import annotations

import asyncio
import logging
import sys

sys.path.insert(0, ".")  # allow running as `python scripts/run_ingestion.py` too

from src.config import settings
from src.common.events import BookDiff, BookSnapshot, ConnectionEvent, Trade
from src.ingestion.manager import IngestionManager


def _describe(event) -> str:
    if isinstance(event, Trade):
        return f"[TRADE]    {event.exchange:8s} {event.symbol:8s} {event.side.value:4s} {event.size}@{event.price}"
    if isinstance(event, BookSnapshot):
        return f"[SNAPSHOT] {event.exchange:8s} {event.symbol:8s} bids={len(event.bids)} asks={len(event.asks)}"
    if isinstance(event, BookDiff):
        return f"[DIFF]     {event.exchange:8s} {event.symbol:8s} levels={len(event.levels)}"
    if isinstance(event, ConnectionEvent):
        return f"[CONN]     {event.exchange:8s} {event.symbol:8s} {event.kind} {event.detail}"
    return f"[UNKNOWN] {event!r}"


async def main() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    manager = IngestionManager(settings.symbols)
    await manager.start()

    counts = {"trade": 0, "snapshot": 0, "diff": 0, "conn": 0}
    try:
        async for event in manager.events():
            print(_describe(event))
            if isinstance(event, Trade):
                counts["trade"] += 1
            elif isinstance(event, BookSnapshot):
                counts["snapshot"] += 1
            elif isinstance(event, BookDiff):
                counts["diff"] += 1
            elif isinstance(event, ConnectionEvent):
                counts["conn"] += 1
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nEvent counts:", counts)
        await manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
