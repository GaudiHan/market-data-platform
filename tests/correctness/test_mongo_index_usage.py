"""
mongomock is great for logic correctness but doesn't implement a real query
planner, so it can't tell us whether get_active_rules_for_symbol() actually
uses the {symbol, active} compound index rather than a collection scan.
This test connects to a real MongoDB (e.g. the one docker-compose brings up)
and checks the query's `.explain()` output for an IXSCAN on that index.

Skips cleanly if no real Mongo is reachable -- MongoDB isn't installable via
apt in this sandbox (Ubuntu dropped the mongodb-org package over licensing),
so this test could not be run in the environment these files were authored
in. Run it yourself after `docker compose up -d` to get the real answer.
"""
import os

import pymongo
import pytest

from src.storage.mongo.models import AlertCondition, AlertRule
from src.storage.mongo.repository import MongoRepository

TEST_MONGO_URI = os.getenv("TEST_MONGO_URI", "mongodb://mdp:mdp_local_pw@localhost:27017/?authSource=admin")


def _mongo_available() -> bool:
    try:
        client = pymongo.MongoClient(TEST_MONGO_URI, serverSelectionTimeoutMS=500)
        client.admin.command("ping")
        return True
    except Exception:
        return False


@pytest.fixture
def real_repo():
    if not _mongo_available():
        pytest.skip(f"no live MongoDB reachable at {TEST_MONGO_URI} (expected in this sandbox)")

    client = pymongo.MongoClient(TEST_MONGO_URI)
    db = client["marketdata_index_test"]
    repo = MongoRepository(db)
    repo.ensure_indexes()

    yield repo

    client.drop_database("marketdata_index_test")


def test_active_rules_query_uses_compound_index(real_repo):
    for i in range(50):
        real_repo.create_alert_rule(
            AlertRule(f"user{i}", "BTC-USD", AlertCondition("price_above", 60000), active=(i % 2 == 0))
        )

    plan = real_repo.alert_rules.find({"symbol": "BTC-USD", "active": True}).explain()
    winning = plan["queryPlanner"]["winningPlan"]

    def _find_stage(node, stage_name):
        if node.get("stage") == stage_name:
            return node
        for child_key in ("inputStage", "inputStages"):
            child = node.get(child_key)
            if isinstance(child, dict):
                found = _find_stage(child, stage_name)
                if found:
                    return found
            elif isinstance(child, list):
                for c in child:
                    found = _find_stage(c, stage_name)
                    if found:
                        return found
        return None

    ixscan = _find_stage(winning, "IXSCAN")
    assert ixscan is not None, f"expected an index scan, got plan: {winning}"
    assert set(ixscan["keyPattern"].keys()) == {"symbol", "active"}
