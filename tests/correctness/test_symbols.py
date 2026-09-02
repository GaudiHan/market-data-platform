from src.common.symbols import from_binance, to_binance, to_coinbase


def test_binance_round_trip_usd_maps_to_usdt():
    assert to_binance("BTC-USD") == "btcusdt"
    assert from_binance("BTCUSDT") == "BTC-USD"


def test_binance_round_trip_non_usd_quote():
    assert to_binance("ETH-BTC") == "ethbtc"
    assert from_binance("ETHBTC", quote_len=3) == "ETH-BTC"


def test_coinbase_is_identity():
    assert to_coinbase("BTC-USD") == "BTC-USD"
