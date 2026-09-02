"""
We standardize on "BASE-QUOTE" (e.g. "BTC-USD") as the common symbol format
used everywhere in storage/orderbook/backtest. Each exchange client is
responsible for translating to/from its own native format at the edge, so
nothing downstream needs to know exchange quirks exist.
"""
from __future__ import annotations


def to_binance(common_symbol: str) -> str:
    """BTC-USD -> btcusdt. Binance has no native USD pairs for most assets,
    so we map USD -> USDT at the edge (documented assumption, not hidden)."""
    base, quote = common_symbol.split("-")
    if quote == "USD":
        quote = "USDT"
    return f"{base}{quote}".lower()


def from_binance(native_symbol: str, quote_len: int = 4) -> str:
    """btcusdt -> BTC-USD. Assumes a 4-char quote (USDT) unless told otherwise;
    good enough for the symbol set this project targets."""
    native_symbol = native_symbol.upper()
    base, quote = native_symbol[:-quote_len], native_symbol[-quote_len:]
    if quote == "USDT":
        quote = "USD"
    return f"{base}-{quote}"


def to_coinbase(common_symbol: str) -> str:
    """BTC-USD -> BTC-USD. Coinbase already uses our common format natively,
    which is precisely why we chose it as the common format."""
    return common_symbol


def from_coinbase(native_symbol: str) -> str:
    return native_symbol
