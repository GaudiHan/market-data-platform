// Runs automatically on first container start (docker-entrypoint-initdb.d).
// Sets up the "flexible state" side of the platform: watchlists, portfolios,
// alert rules. These are document-shaped, user-editable, and don't need
// time-series partitioning -- exactly what Mongo is good at, as opposed to
// forcing them into the relational/time-series side.

db = db.getSiblingDB(process.env.MONGO_INITDB_DATABASE || "marketdata");

// ---------------------------------------------------------------------------
// watchlists: { userId, name, symbols: [...], createdAt, updatedAt }
// Query pattern: "get all watchlists for a user" -> index on userId.
// ---------------------------------------------------------------------------
db.createCollection("watchlists");
db.watchlists.createIndex({ userId: 1, name: 1 }, { unique: true });
db.watchlists.createIndex({ "symbols": 1 }); // "which watchlists contain BTC-USD"

// ---------------------------------------------------------------------------
// portfolios: { userId, name, positions: [{symbol, qty, avgPrice}], cash, updatedAt }
// Query pattern: fetch by userId (mostly single-doc reads), occasionally
// aggregate exposure across all portfolios for a symbol.
// ---------------------------------------------------------------------------
db.createCollection("portfolios");
db.portfolios.createIndex({ userId: 1 });
db.portfolios.createIndex({ "positions.symbol": 1 });

// ---------------------------------------------------------------------------
// alert_rules: { userId, symbol, condition: {type, threshold}, active, lastTriggeredAt }
// Query pattern: the hot path is "give me all ACTIVE rules for symbol X"
// every time a new tick/bar lands -- this needs to be fast and is the
// clearest indexing story in the project (compound index matching the exact
// filter shape used at evaluation time, not just a single-field index).
// ---------------------------------------------------------------------------
db.createCollection("alert_rules");
db.alert_rules.createIndex({ symbol: 1, active: 1 });
db.alert_rules.createIndex({ userId: 1 });

print("Mongo collections + indexes initialized.");
