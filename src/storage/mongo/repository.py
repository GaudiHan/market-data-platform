"""
Repository for the Mongo-backed state: watchlists, portfolios, alert rules.

Deliberately synchronous (plain pymongo, not motor/async) -- unlike the
ingestion/storage-writer path, this isn't on the hot tick-processing loop.
It's read/written by a future API layer and by the alert-evaluation job,
both of which are fine issuing a blocking call per request. Keeping it
synchronous also means it works unmodified against mongomock in tests
(no event loop needed at all).

`ensure_indexes()` mirrors infra/mongo-init/001_init.js. That JS file only
runs once, on first container creation -- if someone wipes the volume, points
at a different Mongo instance, or runs against mongomock in a test, the
init script never fires. Calling ensure_indexes() here makes index creation
idempotent and explicit rather than relying on container lifecycle timing.
"""
from __future__ import annotations

from src.storage.mongo.models import AlertRule, Portfolio, Watchlist


class MongoRepository:
    def __init__(self, db):
        """`db` is any pymongo-compatible Database object -- a real
        pymongo.database.Database or a mongomock.Database in tests."""
        self.db = db
        self.watchlists = db["watchlists"]
        self.portfolios = db["portfolios"]
        self.alert_rules = db["alert_rules"]

    def ensure_indexes(self) -> None:
        self.watchlists.create_index([("userId", 1), ("name", 1)], unique=True)
        self.watchlists.create_index([("symbols", 1)])

        self.portfolios.create_index([("userId", 1)])
        self.portfolios.create_index([("positions.symbol", 1)])

        # Compound index matching the exact filter shape used at query time
        # (see get_active_rules_for_symbol) -- this is the hot-path query,
        # run once per tick/bar per watched symbol, so it needs to hit the
        # index directly rather than filter-then-scan.
        self.alert_rules.create_index([("symbol", 1), ("active", 1)])
        self.alert_rules.create_index([("userId", 1)])

    # ---- watchlists ---------------------------------------------------

    def create_watchlist(self, watchlist: Watchlist):
        return self.watchlists.insert_one(watchlist.to_doc()).inserted_id

    def get_watchlists_for_user(self, user_id: str) -> list[Watchlist]:
        docs = self.watchlists.find({"userId": user_id})
        return [Watchlist.from_doc(d) for d in docs]

    def add_symbol_to_watchlist(self, user_id: str, name: str, symbol: str) -> bool:
        result = self.watchlists.update_one(
            {"userId": user_id, "name": name},
            {"$addToSet": {"symbols": symbol}},
        )
        return result.modified_count > 0

    def get_watchlists_containing_symbol(self, symbol: str) -> list[Watchlist]:
        """Exercises the {symbols: 1} index -- "which watchlists would need
        updating/alerting if this symbol moves."""
        docs = self.watchlists.find({"symbols": symbol})
        return [Watchlist.from_doc(d) for d in docs]

    # ---- portfolios -----------------------------------------------------

    def upsert_portfolio(self, portfolio: Portfolio) -> None:
        self.portfolios.update_one(
            {"userId": portfolio.user_id, "name": portfolio.name},
            {"$set": portfolio.to_doc()},
            upsert=True,
        )

    def get_portfolio(self, user_id: str, name: str) -> Portfolio | None:
        doc = self.portfolios.find_one({"userId": user_id, "name": name})
        return Portfolio.from_doc(doc) if doc else None

    def get_portfolios_containing_symbol(self, symbol: str) -> list[Portfolio]:
        docs = self.portfolios.find({"positions.symbol": symbol})
        return [Portfolio.from_doc(d) for d in docs]

    # ---- alert rules ------------------------------------------------------

    def create_alert_rule(self, rule: AlertRule):
        return self.alert_rules.insert_one(rule.to_doc()).inserted_id

    def get_active_rules_for_symbol(self, symbol: str) -> list[AlertRule]:
        """The hot-path query: evaluated every time a new tick/bar lands for
        a symbol. Filter shape must match the compound index exactly
        ({symbol, active}) so this is an index seek, not a collection scan."""
        docs = self.alert_rules.find({"symbol": symbol, "active": True})
        return [AlertRule.from_doc(d) for d in docs]

    def get_rules_for_user(self, user_id: str) -> list[AlertRule]:
        docs = self.alert_rules.find({"userId": user_id})
        return [AlertRule.from_doc(d) for d in docs]

    def deactivate_rule(self, user_id: str, symbol: str) -> int:
        result = self.alert_rules.update_many(
            {"userId": user_id, "symbol": symbol, "active": True},
            {"$set": {"active": False}},
        )
        return result.modified_count
