"""Calibrate the paper model's fallback spread and slippage from recorded Binance USD-M depth.

    python -m tesseract.paper.calibrate --typed data/typed [--interval-s 60] [--out summary.json]

Reads only typed Binance records (`binance/ws_btcusdt_depth_100ms`, `binance/rest_depth`); Mudrex
depth is not recorded and is never read. Prints (and optionally writes) aggregate numbers only:
no book levels, prices or raw records.

Method
- Books come from tesseract.paper.book.replay, so only depth_synced segments count. Segments are
  replayed one at a time (each with its own verdict, the verdict after it, its snapshot and the
  diffs received between them) so a multi-day recording fits in memory; replay applies the same
  segment-end rules either way.
- Sampling: one book state per `interval` (default 60 s) of Binance event time. For each interval
  boundary m inside a segment (the segment's first state is at or before m), the sample is the
  first state at or after m, provided it is less than one interval after m.
- spread_bps = (best ask - best bid) / mid * 10000, mid = (best bid + best ask) / 2.
- Slippage: for every sample, each size in SIZES and each side, a market order with
  reference_price = mid is filled by tesseract.paper.fills.book_fill (the existing walk; no new
  fill logic). slippage_bps is that fill's slippage versus mid. Orders the book cannot fully
  absorb are excluded and counted.
- impact_bps: the same fill's move beyond the touch, (notional - touch * filled) / (mid * filled)
  * 10000, signed so positive is worse; it equals slippage_bps - spread_bps / 2 up to rounding and
  is exactly 0 when the whole order fills at the best level. The assumed fill applies
  spread/2 + fallback_slippage_bps, so impact, not slippage versus mid, is what
  fallback_slippage_bps stands for.
- Percentiles are nearest-rank (an observed value, no interpolation), so they stay exact decimals.

Decimal throughout, using the fill model's contexts: sums and products exact, divisions rounded
half-even to 28 significant digits.
"""

from __future__ import annotations

import argparse
import json
import sys
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from ..config import PaperConfig
from .book import BUY, SELL, BookState, replay
from .fills import BPS, EXACT, RATIO, Order, book_fill

DEPTH_STREAM = "ws_btcusdt_depth_100ms"
SNAPSHOT_STREAM = "rest_depth"
SIZES = (Decimal("0.01"), Decimal("0.1"), Decimal("1"))  # BTC
PERCENTILES = (50, 75, 90)
REORDER_SLACK_NS = 10 * 10**9
# Fees and latency do not affect fill price or slippage; book_fill needs a config regardless.
_WALK_CFG = PaperConfig(taker_fee=Decimal(0), latency_ms=0,
                        fallback_spread_bps=Decimal(0), fallback_slippage_bps=Decimal(0))
_DIFF_KEYS = ("v", "type", "kind", "recv_ns", "event_time_ms", "first_update_id",
              "final_update_id", "prev_final_update_id", "bids", "asks")


def percentile(sorted_values: list[Decimal], p: int) -> Decimal:
    """Nearest-rank percentile: the smallest value with at least p% of values at or below it."""
    if not sorted_values:
        raise ValueError("no values")
    rank = -(-p * len(sorted_values) // 100)  # ceil(p * n / 100)
    return sorted_values[max(rank, 1) - 1]


def _files(typed_root: Path, stream: str) -> list[Path]:
    return sorted((typed_root / "binance" / stream).glob("*.jsonl"))


def _records(typed_root: Path, stream: str) -> Iterator[dict[str, Any]]:
    for f in _files(typed_root, stream):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    yield json.loads(line)


@dataclass
class _Segment:
    verdict: dict[str, Any]
    after: dict[str, Any] | None  # the next verdict of either kind
    start_ns: int  # recv_ns of the bridging diff
    end_ns: int | None  # last diff recv_ns that can belong to this segment; None = to the end
    diffs: list[dict[str, Any]] = field(default_factory=list)


def _segments(typed_root: Path) -> list[_Segment]:
    verdicts = sorted((r for r in _records(typed_root, DEPTH_STREAM)
                       if r.get("type") in ("depth_synced", "gap")), key=lambda r: r["recv_ns"])
    out = []
    for n, v in enumerate(verdicts):
        if v["type"] != "depth_synced":
            continue
        nxt = verdicts[n + 1] if n + 1 < len(verdicts) else None
        if nxt is None:
            end = None
        elif nxt["type"] == "gap":
            end = nxt["gap_start_ns"]  # replay drops diffs received after this
        else:
            end = nxt["first_recv_ns"] - 1  # the next segment's bridging diff starts there
        out.append(_Segment(v, nxt, v["first_recv_ns"], end))
    return out


def segment_states(typed_root: Path) -> Iterator[BookState]:
    """Every replayed state of every depth_synced segment, in order, one segment in memory at a
    time. `segment` numbers count segments that produced a book."""
    segs = _segments(typed_root)
    wanted = {s.verdict["last_update_id"] for s in segs}
    snaps = {}
    for r in _records(typed_root, SNAPSHOT_STREAM):
        if r.get("kind") == "depth_snapshot" and r["last_update_id"] in wanted:
            snaps.setdefault(r["last_update_id"], r)

    seg_no = 0

    def flush(seg: _Segment) -> Iterator[BookState]:
        nonlocal seg_no
        snap = snaps.get(seg.verdict["last_update_id"])
        recs = seg.diffs + [seg.verdict] + ([seg.after] if seg.after else [])
        produced = False
        for s in replay(recs, [snap] if snap else []):
            produced = True
            yield BookState(s.time_ms, s.update_id, seg_no, s.book)
        seg.diffs = []
        if produced:
            seg_no += 1

    # Files are in arrival order, but recv_ns is wall clock and can step back a little; replay
    # sorts by recv_ns, so each diff goes to the segment its recv_ns falls in, and a segment is
    # replayed only once the stream is REORDER_SLACK_NS past its end.
    starts = [s.start_ns for s in segs]
    i = 0  # first segment not yet replayed
    for r in _records(typed_root, DEPTH_STREAM):
        if r.get("kind") != "depth_diff":
            continue
        ns = r["recv_ns"]
        while i < len(segs) and segs[i].end_ns is not None and ns > segs[i].end_ns + REORDER_SLACK_NS:
            yield from flush(segs[i])
            i += 1
        k = bisect_right(starts, ns) - 1
        if k < 0 or (segs[k].end_ns is not None and ns > segs[k].end_ns):
            continue  # outside every segment
        if k < i:
            raise ValueError(f"diff at recv_ns {ns} is more than {REORDER_SLACK_NS} ns out of order")
        segs[k].diffs.append({key: r[key] for key in _DIFF_KEYS if key in r})
    for seg in segs[i:]:
        yield from flush(seg)


@dataclass(frozen=True)
class Sample:
    time_ms: int
    segment: int
    spread_bps: Decimal
    # (side, size) -> (slippage_bps vs mid, impact_bps); None when the book could not absorb it
    fills: dict[tuple[str, Decimal], tuple[Decimal, Decimal] | None]


def sample(state: BookState, sizes: tuple[Decimal, ...] = SIZES) -> Sample:
    bid, ask = state.book.best_bid(), state.book.best_ask()
    mid = EXACT.divide(EXACT.add(bid, ask), Decimal(2))
    spread = RATIO.divide(EXACT.multiply(EXACT.subtract(ask, bid), BPS), mid)
    out: dict[tuple[str, Decimal], tuple[Decimal, Decimal] | None] = {}
    for side, touch in ((BUY, ask), (SELL, bid)):
        for q in sizes:
            f = book_fill(Order(state.time_ms, side, q, mid), state, _WALK_CFG)
            if f.unfilled_qty != 0:
                out[(side, q)] = None
                continue
            beyond = EXACT.subtract(f.notional, EXACT.multiply(touch, f.filled_qty))
            if side == SELL:
                beyond = -beyond
            impact = RATIO.divide(EXACT.multiply(beyond, BPS), EXACT.multiply(mid, f.filled_qty))
            out[(side, q)] = (f.slippage_bps, impact)
    return Sample(state.time_ms, state.segment, spread, out)


def samples(states: Iterator[BookState], interval_ms: int = 60_000,
            sizes: tuple[Decimal, ...] = SIZES) -> Iterator[Sample]:
    seg, m = None, 0
    for s in states:
        if s.segment != seg:  # first state of a segment: next boundary at or after it
            seg, m = s.segment, -(-s.time_ms // interval_ms) * interval_ms
            if s.time_ms < m:
                continue
        if s.time_ms < m:
            continue
        if s.time_ms - m < interval_ms:
            yield sample(s, sizes)
        m = (s.time_ms // interval_ms + 1) * interval_ms


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stats(values: list[Decimal]) -> dict[str, str]:
    v = sorted(values)
    return {f"p{p}": str(percentile(v, p)) for p in PERCENTILES}


def calibrate(typed_root: Path, interval_ms: int = 60_000,
              sizes: tuple[Decimal, ...] = SIZES) -> dict[str, Any]:
    """Aggregate summary only (strings for exact decimals)."""
    typed_root = Path(typed_root)
    taken = list(samples(segment_states(typed_root), interval_ms, sizes))
    if not taken:
        raise ValueError(f"no synced Binance book samples under {typed_root}")
    spreads = [s.spread_bps for s in taken]
    slip_all, impact_all, excluded = [], [], 0
    per_size: dict[str, dict[str, Any]] = {}
    for q in sizes:
        slip_q, impact_q = [], []
        for s in taken:
            for side in (BUY, SELL):
                v = s.fills[(side, q)]
                if v is None:
                    excluded += 1
                    continue
                slip_q.append(v[0])
                impact_q.append(v[1])
        slip_all += slip_q
        impact_all += impact_q
        per_size[str(q)] = {"n": len(slip_q), "slippage_vs_mid_bps": _stats(slip_q),
                            "impact_beyond_half_spread_bps": _stats(impact_q)}
    return {
        "source": "binance USD-M BTCUSDT depth@100ms, depth_synced segments only",
        "interval_s": interval_ms / 1000,
        "first_sample": _iso(taken[0].time_ms),
        "last_sample": _iso(taken[-1].time_ms),
        "segments_sampled": len({s.segment for s in taken}),
        "spread": {"n": len(spreads), **_stats(spreads)},
        "order_sizes_btc": [str(q) for q in sizes],
        "slippage_vs_mid_bps": {"n": len(slip_all), "excluded_not_absorbed": excluded,
                                **_stats(slip_all)},
        "impact_beyond_half_spread_bps": {"n": len(impact_all), **_stats(impact_all)},
        "by_size": per_size,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--typed", type=Path, default=Path("data/typed"))
    ap.add_argument("--interval-s", type=int, default=60)
    ap.add_argument("--out", type=Path, help="also write the summary JSON here")
    args = ap.parse_args(argv)
    summary = calibrate(args.typed, args.interval_s * 1000)
    text = json.dumps(summary, indent=2) + "\n"
    if args.out:
        args.out.write_text(text, encoding="utf-8")
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
