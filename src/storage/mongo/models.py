"""
Typed shapes for the Mongo-backed "flexible state" side of the platform.
Mongo itself is schemaless, but the Python code that talks to it shouldn't
be -- these dataclasses are what the rest of the codebase imports and
constructs, with explicit to_doc/from_doc conversion at the one seam where
we cross into raw dict-land.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class Watchlist:
    user_id: str
    name: str
    symbols: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_doc(self) -> dict:
        return {
            "userId": self.user_id,
            "name": self.name,
            "symbols": self.symbols,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }

    @staticmethod
    def from_doc(doc: dict) -> "Watchlist":
        return Watchlist(
            user_id=doc["userId"], name=doc["name"], symbols=doc.get("symbols", []),
            created_at=doc.get("createdAt"), updated_at=doc.get("updatedAt"),
        )


@dataclass
class Position:
    symbol: str
    qty: float
    avg_price: float

    def to_doc(self) -> dict:
        return {"symbol": self.symbol, "qty": self.qty, "avgPrice": self.avg_price}

    @staticmethod
    def from_doc(doc: dict) -> "Position":
        return Position(symbol=doc["symbol"], qty=doc["qty"], avg_price=doc["avgPrice"])


@dataclass
class Portfolio:
    user_id: str
    name: str
    positions: list[Position] = field(default_factory=list)
    cash: float = 0.0
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_doc(self) -> dict:
        return {
            "userId": self.user_id,
            "name": self.name,
            "positions": [p.to_doc() for p in self.positions],
            "cash": self.cash,
            "updatedAt": self.updated_at,
        }

    @staticmethod
    def from_doc(doc: dict) -> "Portfolio":
        return Portfolio(
            user_id=doc["userId"], name=doc["name"],
            positions=[Position.from_doc(p) for p in doc.get("positions", [])],
            cash=doc.get("cash", 0.0), updated_at=doc.get("updatedAt"),
        )


@dataclass
class AlertCondition:
    type: str          # e.g. "price_above", "price_below", "pct_change"
    threshold: float

    def to_doc(self) -> dict:
        return {"type": self.type, "threshold": self.threshold}

    @staticmethod
    def from_doc(doc: dict) -> "AlertCondition":
        return AlertCondition(type=doc["type"], threshold=doc["threshold"])


@dataclass
class AlertRule:
    user_id: str
    symbol: str
    condition: AlertCondition
    active: bool = True
    last_triggered_at: datetime | None = None

    def to_doc(self) -> dict:
        return {
            "userId": self.user_id,
            "symbol": self.symbol,
            "condition": self.condition.to_doc(),
            "active": self.active,
            "lastTriggeredAt": self.last_triggered_at,
        }

    @staticmethod
    def from_doc(doc: dict) -> "AlertRule":
        return AlertRule(
            user_id=doc["userId"], symbol=doc["symbol"],
            condition=AlertCondition.from_doc(doc["condition"]),
            active=doc.get("active", True), last_triggered_at=doc.get("lastTriggeredAt"),
        )
