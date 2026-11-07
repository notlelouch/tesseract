"""Rebuild Binance USD-M book states from typed depth records, only where the recording is synced.

Follows the README replay rule: a book exists only from a `depth_synced` record onward. It
starts from the `rest_depth` snapshot whose `lastUpdateId` matches, applies the diff identified
by `first_U`/`first_u`, then each following diff while `pu` equals the previous `u`. A segment
ends at:
  - a `pu` mismatch (checked here too, so a missing verdict can never stitch two books);
  - the first `gap` record written after the `depth_synced` record: diffs received after the
    gap's `gap_start_ns` are not applied;
  - the next `depth_synced` record's first diff.

Each applied diff yields one BookState. Its time is the diff's Binance event time `E` (ms), the
exchange clock the order would meet. A snapshot alone is not a state: under the USD-M rule the
book is only valid once the bridging diff is applied.

Verdict records (`depth_synced`, `gap`) are ordered among themselves by their own `recv_ns` and
matched to diffs by update id or by `gap_start_ns`, never by comparing a verdict's `recv_ns`
with a diff's `recv_ns`.

Pure logic, no I/O. Prices and sizes are Decimal.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Iterator

BUY, SELL = "buy", "sell"


class Book:
    """One side map per side: price -> size. Mutated in place as diffs apply."""

    def __init__(self, bids: list[list[str]], asks: list[list[str]]):
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.apply(bids, asks)

    def apply(self, bids: list[list[str]], asks: list[list[str]]) -> None:
        for side, levels in ((self.bids, bids), (self.asks, asks)):
            for p, q in levels:
                price, size = Decimal(p), Decimal(q)
                if size == 0:
                    side.pop(price, None)
                else:
                    side[price] = size

    def levels_for(self, side: str) -> list[tuple[Decimal, Decimal]]:
        """Levels a market order of `side` walks, best first: buys lift asks, sells hit bids."""
        if side == BUY:
            return sorted(self.asks.items())
        if side == SELL:
            return sorted(self.bids.items(), reverse=True)
        raise ValueError(f"side must be {BUY!r} or {SELL!r}, got {side!r}")

    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None


@dataclass(frozen=True)
class BookState:
    """`book` is live: it changes when the replay advances, so use it before taking the next state."""

    time_ms: int  # event time E of the diff that produced this state
    update_id: int  # that diff's final update id u
    segment: int  # index of the depth_synced segment; states in one segment are contiguous
    book: Book


def _verdicts(depth_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    v = [r for r in depth_records if r.get("type") in ("depth_synced", "gap")]
    return sorted(v, key=lambda r: r["recv_ns"])  # stable: equal recv_ns keep file order


def replay(depth_records: Iterable[dict[str, Any]],
           snapshot_records: Iterable[dict[str, Any]]) -> Iterator[BookState]:
    """depth_records: typed records of the Binance depth WS stream (diffs, gap, depth_synced,
    session_start). snapshot_records: typed `rest_depth` records. Yields states segment by
    segment in time order."""
    depth_records = list(depth_records)
    diffs = sorted((r for r in depth_records if r.get("kind") == "depth_diff"),
                   key=lambda r: r["recv_ns"])
    index = {(d["first_update_id"], d["final_update_id"]): i for i, d in enumerate(diffs)}
    verdicts = _verdicts(depth_records)
    wanted = {v["last_update_id"] for v in verdicts if v["type"] == "depth_synced"}
    snaps = {s["last_update_id"]: s for s in snapshot_records
             if s.get("kind") == "depth_snapshot" and s["last_update_id"] in wanted}

    segment = 0
    for n, v in enumerate(verdicts):
        if v["type"] != "depth_synced":
            continue
        start = index.get((v["first_U"], v["first_u"]))
        snap = snaps.get(v["last_update_id"])
        if start is None or snap is None:
            continue  # verdict without its recorded diff or snapshot: no book, fills fall back
        # What ends this segment: the next verdict of either kind.
        nxt = next(iter(verdicts[n + 1:]), None)
        cut_ns = nxt["gap_start_ns"] if nxt is not None and nxt["type"] == "gap" else None
        stop_at = (index.get((nxt["first_U"], nxt["first_u"]))
                   if nxt is not None and nxt["type"] == "depth_synced" else None)

        book = Book(snap["bids"], snap["asks"])
        prev_u = None
        for i in range(start, len(diffs)):
            d = diffs[i]
            if stop_at is not None and i >= stop_at:
                break
            if cut_ns is not None and d["recv_ns"] > cut_ns:
                break
            if prev_u is not None and d["prev_final_update_id"] != prev_u:
                break
            book.apply(d["bids"], d["asks"])
            prev_u = d["final_update_id"]
            yield BookState(d["event_time_ms"], prev_u, segment, book)
        segment += 1
