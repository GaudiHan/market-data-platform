"""
Ties strategy signals, order-book execution, fees, and metrics together.
The single most important structural decision here: at bar i, the engine
passes the strategy `bars.iloc[:i+1]` -- a slice, not the full frame -- so
lookahead bias isn't something the strategy could introduce even by
accident (see strategies/base.py). Everything else (fee application,
execution realism, benchmark comparison) follows from that one anchor.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pandas as pd

from src.backtest import metrics
from src.backtest.costs import DEFAULT_TAKER_FEE_BPS, calculate_fee
from src.backtest.execution import ExecutionSimulator, Fill
from src.backtest.strategies.base import Signal, Strategy
from src.common.events import Side
from src.orderbook.book import OrderBook

# Fallback slippage used only when no order-book snapshot is available for
# a given decision timestamp (e.g. book_events history doesn't reach back
# that far yet). Applied against the trader, same direction logic as real
# slippage would have -- this keeps the backtest usable before enough book
# history has accumulated, while being clearly a fallback, not the primary
# execution model the assignment asks for.
FALLBACK_SLIPPAGE_BPS = 5.0


@dataclass
class TradeRecord:
    timestamp: pd.Timestamp
    side: Side
    requested_qty: float
    filled_qty: float
    avg_price: float
    fee: float
    used_order_book: bool
    slippage_bps: float | None


@dataclass
class BacktestResult:
    strategy_name: str
    equity_curve: pd.Series
    trades: list[TradeRecord] = field(default_factory=list)
    total_return: float = 0.0
    sharpe: float = 0.0
    max_drawdown: float = 0.0
    benchmark_return: float = 0.0
    fill_shortfalls: int = 0  # count of trades that couldn't be fully filled

    def summary(self) -> str:
        return (
            f"{self.strategy_name}: return={self.total_return:+.2%} "
            f"(benchmark buy&hold={self.benchmark_return:+.2%}) "
            f"sharpe={self.sharpe:.2f} max_drawdown={self.max_drawdown:.2%} "
            f"trades={len(self.trades)} shortfalls={self.fill_shortfalls}"
        )


class BacktestEngine:
    def __init__(
        self,
        initial_cash: float = 10_000.0,
        position_size: float = 1.0,
        fee_bps: float = DEFAULT_TAKER_FEE_BPS,
        periods_per_year: float = 24 * 365,  # matches hourly bars; override for other intervals
    ):
        self.initial_cash = initial_cash
        self.position_size = position_size
        self.fee_bps = fee_bps
        self.periods_per_year = periods_per_year
        self.execution_sim = ExecutionSimulator()

    def run(
        self,
        bars: pd.DataFrame,
        strategy: Strategy,
        start_idx: int = 0,
        end_idx: int | None = None,
        order_book_provider: Callable[[pd.Timestamp], OrderBook | None] | None = None,
    ) -> BacktestResult:
        """Run `strategy` over bars[start_idx:end_idx]. `order_book_provider`,
        if given, is called with each decision timestamp and should return an
        OrderBook reconstructed as of that time (see
        src/orderbook/replay.OrderBookReplayer) or None to fall back to the
        simple slippage model for that bar."""
        end_idx = len(bars) if end_idx is None else end_idx
        warmup = max(start_idx, strategy.warmup_bars())

        cash = self.initial_cash
        position = 0.0
        equity_values = []
        equity_index = []
        trades: list[TradeRecord] = []
        fill_shortfalls = 0

        for i in range(warmup, end_idx):
            history = bars.iloc[: i + 1]  # <-- the entire lookahead-prevention mechanism
            signal = strategy.generate_signal(history)
            target_position = float(signal.value) * self.position_size

            ts = bars.index[i]
            close_price = float(bars["close"].iloc[i])
            trade_qty = target_position - position

            if abs(trade_qty) > 1e-12:
                order_side = Side.BUY if trade_qty > 0 else Side.SELL
                requested = abs(trade_qty)

                book = order_book_provider(ts) if order_book_provider else None
                if book is not None and (book.best_bid() or book.best_ask()):
                    fill: Fill = self.execution_sim.simulate_market_order(book, order_side, requested)
                    used_book = True
                    if not fill.is_full_fill:
                        fill_shortfalls += 1
                    filled_qty_signed = fill.filled_qty if order_side == Side.BUY else -fill.filled_qty
                    exec_price = fill.avg_price if fill.avg_price is not None else close_price
                    slippage_bps = fill.slippage_bps()
                else:
                    # Fallback: no book history available for this timestamp.
                    used_book = False
                    slip_sign = 1 if order_side == Side.BUY else -1
                    exec_price = close_price * (1 + slip_sign * FALLBACK_SLIPPAGE_BPS / 10_000)
                    filled_qty_signed = trade_qty
                    slippage_bps = FALLBACK_SLIPPAGE_BPS

                notional = filled_qty_signed * exec_price
                fee = calculate_fee(notional, self.fee_bps)
                cash -= notional + fee
                position += filled_qty_signed

                trades.append(TradeRecord(
                    timestamp=ts, side=order_side, requested_qty=requested,
                    filled_qty=abs(filled_qty_signed), avg_price=exec_price, fee=fee,
                    used_order_book=used_book, slippage_bps=slippage_bps,
                ))

            equity_values.append(cash + position * close_price)
            equity_index.append(ts)

        equity_curve = pd.Series(equity_values, index=pd.Index(equity_index, name="timestamp"))
        returns = equity_curve.pct_change().dropna()
        benchmark_prices = bars["close"].iloc[warmup:end_idx]

        return BacktestResult(
            strategy_name=strategy.name,
            equity_curve=equity_curve,
            trades=trades,
            total_return=metrics.total_return(equity_curve),
            sharpe=metrics.sharpe_ratio(returns, self.periods_per_year),
            max_drawdown=metrics.max_drawdown(equity_curve),
            benchmark_return=metrics.buy_and_hold_return(benchmark_prices),
            fill_shortfalls=fill_shortfalls,
        )
