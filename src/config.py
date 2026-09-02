"""
Central config, loaded once from environment / .env.
Nothing here requires a paid API key -- Binance and Coinbase both expose
public market-data WebSocket streams with no authentication.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _split_symbols(raw: str) -> list[str]:
    return [s.strip() for s in raw.split(",") if s.strip()]


@dataclass(frozen=True)
class TimescaleConfig:
    host: str = os.getenv("TSDB_HOST", "localhost")
    port: int = int(os.getenv("TSDB_PORT", "5432"))
    user: str = os.getenv("TSDB_USER", "mdp")
    password: str = os.getenv("TSDB_PASSWORD", "mdp_local_pw")
    database: str = os.getenv("TSDB_DB", "marketdata")

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class MongoConfig:
    host: str = os.getenv("MONGO_HOST", "localhost")
    port: int = int(os.getenv("MONGO_PORT", "27017"))
    user: str = os.getenv("MONGO_USER", "mdp")
    password: str = os.getenv("MONGO_PASSWORD", "mdp_local_pw")
    database: str = os.getenv("MONGO_DB", "marketdata")

    @property
    def uri(self) -> str:
        return (
            f"mongodb://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/?authSource=admin"
        )


@dataclass(frozen=True)
class Settings:
    symbols: list[str] = field(
        default_factory=lambda: _split_symbols(os.getenv("SYMBOLS", "BTC-USD,ETH-USD"))
    )
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    timescale: TimescaleConfig = field(default_factory=TimescaleConfig)
    mongo: MongoConfig = field(default_factory=MongoConfig)


settings = Settings()
