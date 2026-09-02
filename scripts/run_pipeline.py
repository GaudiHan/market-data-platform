"""
Layer 1 + Layer 2 wired together: streams live trades and book events from
both exchanges straight into TimescaleDB. This is the first point where
data actually lands in a database rather than just printing to the console.

Usage:
    docker compose up -d          # start TimescaleDB + Mongo first
    python -m scripts.run_pipeline
"""
from __future__ import annotations

import asyncio
import logging
import sys

sys.path.insert(0, ".")

from src.config import settings
from src.ingestion.manager import IngestionManager
from src.storage.timescale.writer import TimescaleWriter

logger = logging.getLogger(__name__)


async def main() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    manager = IngestionManager(settings.symbols)
    writer = TimescaleWriter(settings.timescale.dsn)

    await writer.connect()
    await manager.start()

    logger.info("pipeline running -- ingestion -> TimescaleDB. Ctrl+C to stop.")
    try:
        await writer.run(manager.events())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        logger.info("shutting down: stats=%s", writer.stats)
        await manager.stop()
        await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
