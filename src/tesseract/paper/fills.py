"""Paper fills for market orders. Pure logic: no network, no live trading.

Rules
- A decision at time t (ms) targets t + latency_ms. It fills against the first book state at or
  after the target, but only if a synced book covers the target: the state is in the same
  depth_synced segment as the state before it, or lands exactly on the target. Otherwise
  (before the first state, between segments, after the last state) the fill is "assumed".
- Book fill: a buy walks asks from the lowest price, a sell walks bids from the highest, until
  the quantity is filled or the recorded levels run out. What the book cannot absorb is recorded
  as unfilled_qty; it is never assumed to fill.
- Assumed fill: full quantity at reference * (1 +/- (fallback_spread_bps / 2 +
  fallback_slippage_bps) / 10000), at the target time. Tagged source="assumed".
- fee = taker_fee * filled notional (quote currency). Fill price = notional / filled_qty.
- slippage_bps is signed so that positive is worse for the trader:
  buy (fill - ref) / ref * 10000, sell (ref - fill) / ref * 10000.

Arithmetic is Decimal only. Sums and products (notional, fee, remaining qty, assumed price) use a
context that raises on any rounding, so they are exact or the run fails. The two divisions (VWAP
fill price, slippage bps) are rounded half-even to 28 significant digits.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import (ROUND_HALF_EVEN, Context, Decimal, DivisionByZero, Inexact, InvalidOperation,
                     Overflow)
from typing import Any, Iterable, Literal

from ..config import PaperConfig
from .book import BUY, SELL, BookState

BOOK: Literal["book"] = "book"
ASSUMED: Literal["assumed"] = "assumed"
Source = Literal["book", "assumed"]

EXACT = Context(prec=60, rounding=ROUND_HALF_EVEN,
                traps=[Inexact, InvalidOperation, DivisionByZero, Overflow])
RATIO = Context(prec=28, rounding=ROUND_HALF_EVEN, traps=[InvalidOperation, DivisionByZero, Overflow])
BPS = Decimal(10_000)
ZERO = Decimal(0)


@dataclass(frozen=True)
class Order:
    decision_time_ms: int
    side: str  # "buy" | "sell"
    qty: Decimal  # base asset (BTC)
    reference_price: Decimal  # the price the decision was made at

    def __post_init__(self) -> None:
        if self.side not in (BUY, SELL):
            raise ValueError(f"side must be {BUY!r} or {SELL!r}, got {self.side!r}")
        for name in ("qty", "reference_price"):
            v = getattr(self, name)
            if not isinstance(v, Decimal) or not v.is_finite() or v <= 0:
                raise ValueError(f"{name} must be a positive finite Decimal, got {v!r}")


@dataclass(frozen=True)
class Fill:
    decision_time_ms: int
    fill_time_ms: int
    side: str
    order_qty: Decimal
    reference_price: Decimal
    fill_price: Decimal | None  # None when nothing filled
    filled_qty: Decimal
    unfilled_qty: Decimal
    notional: Decimal
    fee: Decimal
    slippage_bps: Decimal | None  # None when nothing filled
    source: Source
    book_update_id: int | None  # Binance u of the state filled against; None for assumed
    levels_consumed: int

    def __post_init__(self) -> None:
        # A fill's source must match its provenance, so the two kinds can never be confused.
        if self.source == BOOK:
            if self.book_update_id is None:
                raise ValueError("book fill without a book_update_id")
        elif self.source == ASSUMED:
            if self.book_update_id is not None or self.levels_consumed != 0:
                raise ValueError("assumed fill carries book provenance")
        else:
            raise ValueError(f"source must be {BOOK!r} or {ASSUMED!r}, got {self.source!r}")
        if EXACT.add(self.filled_qty, self.unfilled_qty) != self.order_qty:
            raise ValueError("filled_qty + unfilled_qty != order_qty")

    def to_record(self) -> dict[str, Any]:
        """JSON-ready: decimals as exact strings, null where nothing filled."""
        out: dict[str, Any] = {}
        for k, v in self.__dict__.items():
            out[k] = str(v) if isinstance(v, Decimal) else v
        return out


def slippage_bps(side: str, reference: Decimal, price: Decimal) -> Decimal:
    diff = EXACT.subtract(price, reference) if side == BUY else EXACT.subtract(reference, price)
    return RATIO.divide(EXACT.multiply(diff, BPS), reference)


def book_fill(order: Order, state: BookState, cfg: PaperConfig) -> Fill:
    remaining, filled, notional, used = order.qty, ZERO, ZERO, 0
    for price, size in state.book.levels_for(order.side):
        if remaining == 0:
            break
        take = min(remaining, size)
        notional = EXACT.add(notional, EXACT.multiply(price, take))
        filled = EXACT.add(filled, take)
        remaining = EXACT.subtract(remaining, take)
        used += 1
    price = RATIO.divide(notional, filled) if filled > 0 else None
    return Fill(order.decision_time_ms, state.time_ms, order.side, order.qty, order.reference_price,
                price, filled, remaining, notional, EXACT.multiply(notional, cfg.taker_fee),
                slippage_bps(order.side, order.reference_price, price) if price is not None else None,
                BOOK, state.update_id, used)


def assumed_fill(order: Order, cfg: PaperConfig) -> Fill:
    adverse = EXACT.add(EXACT.divide(cfg.fallback_spread_bps, Decimal(2)), cfg.fallback_slippage_bps)
    move = EXACT.divide(adverse, BPS)
    factor = EXACT.add(1, move) if order.side == BUY else EXACT.subtract(1, move)
    price = EXACT.multiply(order.reference_price, factor)
    notional = EXACT.multiply(price, order.qty)
    return Fill(order.decision_time_ms, order.decision_time_ms + cfg.latency_ms, order.side,
                order.qty, order.reference_price, price, order.qty, ZERO, notional,
                EXACT.multiply(notional, cfg.taker_fee),
                slippage_bps(order.side, order.reference_price, price), ASSUMED, None, 0)


def simulate(orders: Iterable[Order], states: Iterable[BookState], cfg: PaperConfig) -> list[Fill]:
    """Fill every order once, in decision-time order. `states` must be in time order (as
    book.replay yields them); each state is used before the next is taken."""
    pending = sorted(orders, key=lambda o: o.decision_time_ms)
    fills: list[Fill] = []
    i = 0
    prev: BookState | None = None
    for s in states:
        while i < len(pending) and pending[i].decision_time_ms + cfg.latency_ms <= s.time_ms:
            o = pending[i]
            target = o.decision_time_ms + cfg.latency_ms
            covered = s.time_ms == target or (prev is not None and prev.segment == s.segment)
            fills.append(book_fill(o, s, cfg) if covered else assumed_fill(o, cfg))
            i += 1
        if i == len(pending):
            break
        prev = s
    fills.extend(assumed_fill(o, cfg) for o in pending[i:])
    return fills


def _total(values: Iterable[Decimal]) -> Decimal:
    t = ZERO
    for v in values:
        t = EXACT.add(t, v)
    return t


def summarize(fills: Iterable[Fill]) -> dict[str, dict[str, Any]]:
    """Totals per source. Book and assumed fills are never added together."""
    fills = list(fills)
    out: dict[str, dict[str, Any]] = {}
    for src in (BOOK, ASSUMED):
        fs = [f for f in fills if f.source == src]
        out[src] = {"count": len(fs),
                    "partial": sum(1 for f in fs if f.unfilled_qty > 0),
                    "filled_qty": _total(f.filled_qty for f in fs),
                    "notional": _total(f.notional for f in fs),
                    "fee": _total(f.fee for f in fs)}
    return out
