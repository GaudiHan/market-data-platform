"""
Correctness tests for the Mongo repository layer. Runs against mongomock
(in-process, no server needed) so these are fast and don't depend on infra
being up -- they test query correctness and index enforcement logic, which
mongomock models accurately enough for that purpose. See
test_mongo_index_usage.py for a real-Mongo check that the compound index is
actually what serves the hot-path query, which mongomock can't verify
(it doesn't implement a real query planner).
"""
import mongomock
import pytest

from src.storage.mongo.models import AlertCondition, AlertRule, Portfolio, Position, Watchlist
from src.storage.mongo.repository import MongoRepository


@pytest.fixture
def repo():
    client = mongomock.MongoClient()
    db = client["marketdata_test"]
    r = MongoRepository(db)
    r.ensure_indexes()
    return r


def test_watchlist_create_and_fetch(repo):
    repo.create_watchlist(Watchlist(user_id="u1", name="crypto", symbols=["BTC-USD"]))
    lists = repo.get_watchlists_for_user("u1")
    assert len(lists) == 1
    assert lists[0].symbols == ["BTC-USD"]


def test_watchlist_unique_index_prevents_duplicate_name(repo):
    repo.create_watchlist(Watchlist(user_id="u1", name="crypto", symbols=["BTC-USD"]))
    with pytest.raises(Exception):
        # Same (userId, name) pair -- must be rejected by the unique index,
        # not silently create a second "crypto" watchlist for the same user.
        repo.create_watchlist(Watchlist(user_id="u1", name="crypto", symbols=["ETH-USD"]))


def test_watchlist_same_name_different_user_is_allowed(repo):
    repo.create_watchlist(Watchlist(user_id="u1", name="crypto", symbols=["BTC-USD"]))
    repo.create_watchlist(Watchlist(user_id="u2", name="crypto", symbols=["ETH-USD"]))
    assert len(repo.get_watchlists_for_user("u1")) == 1
    assert len(repo.get_watchlists_for_user("u2")) == 1


def test_add_symbol_to_watchlist_is_idempotent(repo):
    repo.create_watchlist(Watchlist(user_id="u1", name="crypto", symbols=["BTC-USD"]))
    repo.add_symbol_to_watchlist("u1", "crypto", "ETH-USD")
    repo.add_symbol_to_watchlist("u1", "crypto", "ETH-USD")  # duplicate add
    wl = repo.get_watchlists_for_user("u1")[0]
    assert sorted(wl.symbols) == ["BTC-USD", "ETH-USD"]  # $addToSet, no duplicate


def test_get_watchlists_containing_symbol(repo):
    repo.create_watchlist(Watchlist(user_id="u1", name="a", symbols=["BTC-USD", "ETH-USD"]))
    repo.create_watchlist(Watchlist(user_id="u2", name="b", symbols=["ETH-USD"]))
    repo.create_watchlist(Watchlist(user_id="u3", name="c", symbols=["SOL-USD"]))

    result = repo.get_watchlists_containing_symbol("ETH-USD")
    assert {w.user_id for w in result} == {"u1", "u2"}


def test_portfolio_upsert_creates_then_updates(repo):
    p = Portfolio(user_id="u1", name="main", positions=[Position("BTC-USD", 0.5, 40000)], cash=1000)
    repo.upsert_portfolio(p)

    p.cash = 500
    p.positions.append(Position("ETH-USD", 2.0, 2000))
    repo.upsert_portfolio(p)

    fetched = repo.get_portfolio("u1", "main")
    assert fetched.cash == 500
    assert len(fetched.positions) == 2


def test_get_portfolios_containing_symbol(repo):
    repo.upsert_portfolio(Portfolio("u1", "main", [Position("BTC-USD", 1, 40000)], 0))
    repo.upsert_portfolio(Portfolio("u2", "main", [Position("ETH-USD", 1, 2000)], 0))

    result = repo.get_portfolios_containing_symbol("BTC-USD")
    assert len(result) == 1
    assert result[0].user_id == "u1"


def test_alert_rule_hot_path_query_only_returns_active_for_symbol(repo):
    repo.create_alert_rule(AlertRule("u1", "BTC-USD", AlertCondition("price_above", 60000), active=True))
    repo.create_alert_rule(AlertRule("u2", "BTC-USD", AlertCondition("price_below", 50000), active=False))
    repo.create_alert_rule(AlertRule("u1", "ETH-USD", AlertCondition("price_above", 3000), active=True))

    active_btc_rules = repo.get_active_rules_for_symbol("BTC-USD")
    assert len(active_btc_rules) == 1
    assert active_btc_rules[0].user_id == "u1"
    assert active_btc_rules[0].condition.threshold == 60000


def test_deactivate_rule(repo):
    repo.create_alert_rule(AlertRule("u1", "BTC-USD", AlertCondition("price_above", 60000)))
    modified = repo.deactivate_rule("u1", "BTC-USD")
    assert modified == 1
    assert repo.get_active_rules_for_symbol("BTC-USD") == []
