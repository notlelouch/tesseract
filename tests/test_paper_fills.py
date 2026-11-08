"""Paper fill model on real recorded data only.

The data path is the production one: tests/fixtures/binance_depth_session.jsonl and the Mudrex
kline fixture are recorded into a raw tree by the recorder's own code (test_parser.record_tree,
which also writes the recorder's real `gap` and `depth_synced` verdicts), parsed to typed JSONL by
tesseract.parser, and then replayed. No book level, price or size is written by hand: order sizes
and reference prices are derived from the replayed book or the Mudrex candles.

The recorded Mudrex klines (15:51-15:56 UTC) and Binance depth (22:24-22:25 UTC, 2026-11-05) do
not overlap, so book-fill tests use the Binance mid at decision time as the reference price, and
the Mudrex reference is exercised on the assumed path.
"""

import asyncio
import json
import re
from dataclasses import replace
from decimal import ROUND_CEILING, Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from tesseract.config import ConfigError, load_paper_config
from tesseract.paper.book import Book, BookState, replay
from tesseract.paper.fills import (ASSUMED, BOOK, EXACT, Fill, Order, assumed_fill, simulate,
                                   summarize)
from tesseract.paper.klines import KlineSeries, NoReference, interval_ms

from test_parser import parse, record_tree, typed

ROOT = Path(__file__).parent.parent
CFG = load_paper_config(ROOT / "config.toml")
DEPTH_STREAM = "ws_btcusdt_depth_100ms"


@pytest.fixture(scope="module")
def rec(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("paper")
    raw = asyncio.run(record_tree(tmp))
    summary = parse(tmp, raw)
    assert summary["totals"]["parse_failures"] == 0
    return SimpleNamespace(depth=typed(tmp, "binance", DEPTH_STREAM),
                           snaps=typed(tmp, "binance", "rest_depth"),
                           klines=typed(tmp, "mudrex", "rest_kline"))


def states(rec):
    return replay(rec.depth, rec.snaps)


@pytest.fixture(scope="module")
def frozen(rec):
    """Every replayed state with its own copy of the book, for computing expectations."""
    out = []
    for s in states(rec):
        b = Book([], [])
        b.bids, b.asks = dict(s.book.bids), dict(s.book.asks)
        out.append(BookState(s.time_ms, s.update_id, s.segment, b))
    return out


def mid(s: BookState) -> Decimal:
    return EXACT.divide(EXACT.add(s.book.best_bid(), s.book.best_ask()), Decimal(2))


def one(rec, order, cfg=CFG) -> Fill:
    (f,) = simulate([order], states(rec), cfg)
    return f


def decision_for(state: BookState, cfg=CFG) -> int:
    """A decision whose target (t + latency) lands 1 ms before `state`, so `state` is the first
    state at or after the target."""
    return state.time_ms - cfg.latency_ms - 1


# --------------------------------------------------------------------------- replay


def test_replay_starts_at_depth_synced_and_stays_contiguous(rec, frozen):
    (synced,) = [r for r in rec.depth if r["type"] == "depth_synced"]
    diffs = [r for r in rec.depth if r.get("kind") == "depth_diff"]
    first = next(d for d in diffs if (d["first_update_id"], d["final_update_id"])
                 == (synced["first_U"], synced["first_u"]))
    assert frozen[0].update_id == synced["first_u"] and frozen[0].time_ms == first["event_time_ms"]
    # Every diff from the bridge to the end of the session (the recording had no breaks).
    assert [s.update_id for s in frozen] == [d["final_update_id"] for d in diffs
                                            if d["recv_ns"] >= first["recv_ns"]]
    assert {s.segment for s in frozen} == {0}
    times = [s.time_ms for s in frozen]
    assert times == sorted(times)
    # The stale snapshot (gap: snapshot_stale) never seeds a book.
    (gap,) = [r for r in rec.depth if r["type"] == "gap"]
    assert gap["last_update_id"] != synced["last_update_id"]
    for s in frozen:
        assert s.book.best_bid() < s.book.best_ask()


# --------------------------------------------------------------------------- fills


def test_full_fill_from_one_level(rec, frozen):
    s = frozen[3]
    price, size = s.book.levels_for("buy")[0]
    f = one(rec, Order(decision_for(s), "buy", size, mid(frozen[2])))
    assert f.source == BOOK and f.book_update_id == s.update_id and f.fill_time_ms == s.time_ms
    assert f.levels_consumed == 1
    assert f.fill_price == price and f.filled_qty == size and f.unfilled_qty == 0
    assert f.notional == price * size
    assert f.fee == price * size * CFG.taker_fee


def test_multi_level_walk(rec, frozen):
    s = frozen[5]
    lv = s.book.levels_for("sell")  # bids, best (highest) first
    qty = lv[0][1] + lv[1][1] + lv[2][1] / 2
    f = one(rec, Order(decision_for(s), "sell", qty, mid(frozen[4])))
    assert f.source == BOOK and f.book_update_id == s.update_id
    assert f.levels_consumed == 3 and f.filled_qty == qty and f.unfilled_qty == 0
    expected = lv[0][0] * lv[0][1] + lv[1][0] * lv[1][1] + lv[2][0] * (lv[2][1] / 2)
    assert f.notional == expected
    assert lv[2][0] < f.fill_price < lv[0][0]  # VWAP strictly inside the levels walked
    assert f.slippage_bps > 0  # selling below the reference is a cost


def test_partial_fill_when_depth_runs_out(rec, frozen):
    s = frozen[7]
    asks = s.book.levels_for("buy")
    depth = sum((q for _, q in asks), Decimal(0))
    qty = depth + 1
    f = one(rec, Order(decision_for(s), "buy", qty, mid(frozen[6])))
    assert f.source == BOOK
    assert f.filled_qty == depth and f.unfilled_qty == 1  # the rest is not assumed to fill
    assert f.levels_consumed == len(asks)
    assert f.notional == sum((p * q for p, q in asks), Decimal(0))
    assert f.fee == f.notional * CFG.taker_fee  # fee on what filled, not on the order size


def test_latency_selects_first_state_at_or_after_t_plus_latency(rec, frozen):
    assert CFG.latency_ms == 250  # shipped default
    t = frozen[2].time_ms - 1
    for latency in (0, CFG.latency_ms, frozen[6].time_ms - t):  # last one lands exactly on a state
        cfg = replace(CFG, latency_ms=latency)
        want = next(s for s in frozen if s.time_ms >= t + latency)
        f = one(rec, Order(t, "buy", Decimal("0.001"), mid(frozen[1])), cfg)
        assert (f.source, f.book_update_id, f.fill_time_ms) == (BOOK, want.update_id, want.time_ms)
        assert f.decision_time_ms == t
    # The default latency really moves the fill to a later book than zero latency would.
    no_lat = one(rec, Order(t, "buy", Decimal("0.001"), mid(frozen[1])), replace(CFG, latency_ms=0))
    with_lat = one(rec, Order(t, "buy", Decimal("0.001"), mid(frozen[1])))
    assert with_lat.fill_time_ms - no_lat.fill_time_ms >= CFG.latency_ms - 102


def test_assumed_source_when_no_book_covers_the_fill_time(rec, frozen):
    series = KlineSeries.from_typed(rec.klines, interval_ms("1m"))
    last = series.candles[-1]
    ref = series.reference_at(last.close_time_ms)
    assert ref.close == Decimal("76098.2") == Decimal(rec.klines[0]["candles"][-1]["close"])

    orders = [
        Order(last.close_time_ms, "buy", Decimal("0.5"), ref.close),  # Mudrex time: no Binance book
        Order(frozen[0].time_ms - CFG.latency_ms - 1, "sell", Decimal("0.5"), mid(frozen[0])),  # before sync
        Order(frozen[-1].time_ms - CFG.latency_ms + 1, "buy", Decimal("0.5"), mid(frozen[-1])),  # after end
    ]
    fills = simulate(orders, states(rec), CFG)
    assert [f.source for f in fills] == [ASSUMED] * 3
    adverse = CFG.fallback_spread_bps / 2 + CFG.fallback_slippage_bps
    for o, f in zip(sorted(orders, key=lambda o: o.decision_time_ms), fills):
        assert f.book_update_id is None and f.levels_consumed == 0
        assert f.fill_time_ms == o.decision_time_ms + CFG.latency_ms
        assert f.filled_qty == o.qty and f.unfilled_qty == 0
        assert f.slippage_bps == adverse
        sign = 1 if o.side == "buy" else -1
        assert f.fill_price == o.reference_price * (1 + sign * adverse / 10_000)


def test_exact_decimal_arithmetic(rec, frozen):
    s = frozen[9]
    lv = s.book.levels_for("buy")
    qty = sum((q for _, q in lv[:4]), Decimal(0)) + lv[4][1] / 4
    f = one(rec, Order(decision_for(s), "buy", qty, mid(frozen[8])))
    exact = sum((p * q for p, q in lv[:4]), Decimal(0)) + lv[4][0] * (lv[4][1] / 4)
    assert f.notional == exact and f.fee == exact * CFG.taker_fee
    for name in ("order_qty", "reference_price", "fill_price", "filled_qty", "unfilled_qty",
                 "notional", "fee", "slippage_bps"):
        assert type(getattr(f, name)) is Decimal, name
    # VWAP is the one rounded division: notional / qty to 28 significant digits.
    assert abs(f.fill_price * f.filled_qty - f.notional) < Decimal("1e-20")
    # Records carry exact decimal text that round-trips.
    r = f.to_record()
    assert Decimal(r["notional"]) == f.notional and Decimal(r["fee"]) == f.fee
    assert r["fee"] == str(exact * CFG.taker_fee)
    # Config values are exact decimals too (0.0005 as a float would not be).
    assert CFG.taker_fee == Decimal("0.0005") and type(CFG.taker_fee) is Decimal
    with pytest.raises(ValueError):
        Order(decision_for(s), "buy", 0.5, mid(frozen[8]))  # float qty refused


def test_assumed_fill_is_never_counted_as_a_book_fill(rec, frozen):
    """Fails if any fill is labelled `book` without a book having covered its fill time, or if an
    assumed fill is added into book totals."""
    first, last = frozen[0].time_ms, frozen[-1].time_ms
    times = [first - CFG.latency_ms - 500, first - CFG.latency_ms - 1, first - CFG.latency_ms,
             *(s.time_ms - CFG.latency_ms - 1 for s in frozen[1:]), last - CFG.latency_ms,
             last - CFG.latency_ms + 1, last + 5_000]
    orders = [Order(t, ("buy", "sell")[i % 2], Decimal("0.010"), mid(frozen[0]))
              for i, t in enumerate(times)]
    fills = simulate(orders, states(rec), CFG)
    assert len(fills) == len(orders)

    def covered(target: int) -> bool:  # independent of the engine: inside the synced span
        return first <= target <= last

    book_ids = {s.update_id: s.time_ms for s in frozen}
    for f in fills:
        target = f.decision_time_ms + CFG.latency_ms
        if covered(target):
            assert f.source == BOOK, target
            assert book_ids[f.book_update_id] == f.fill_time_ms >= target
        else:
            assert f.source == ASSUMED, target
            assert f.book_update_id is None and f.levels_consumed == 0
    n_book = sum(covered(t + CFG.latency_ms) for t in times)
    assert 0 < n_book < len(fills)

    totals = summarize(fills)
    assert totals[BOOK]["count"] == n_book
    assert totals[ASSUMED]["count"] == len(fills) - n_book
    assert totals[BOOK]["notional"] == sum((f.notional for f in fills if f.book_update_id), Decimal(0))

    # The type itself refuses mislabelled provenance.
    a = next(f for f in fills if f.source == ASSUMED)
    with pytest.raises(ValueError):
        replace(a, source=BOOK)  # "book" with no book behind it
    b = next(f for f in fills if f.source == BOOK)
    with pytest.raises(ValueError):
        replace(b, source=ASSUMED)  # "assumed" carrying a book update id
    with pytest.raises(ValueError):
        replace(a, source="mixed")


# --------------------------------------------------------------------------- reference + config


def test_kline_reference_never_falls_back_to_a_stale_close(rec):
    series = KlineSeries.from_typed(rec.klines, interval_ms("1m"))
    body = rec.klines[0]["candles"]
    assert [c.open_time_ms for c in series.candles] == [c["open_time_ms"] for c in body]
    last = series.candles[-1]
    assert series.reference_at(last.close_time_ms + 59_999) == last
    with pytest.raises(NoReference):
        series.reference_at(last.close_time_ms + 60_000)  # next candle missing
    with pytest.raises(NoReference):
        series.reference_at(series.candles[0].open_time_ms)  # nothing closed yet


def test_paper_config_shipped_and_validated(tmp_path):
    assert load_paper_config(ROOT / "config.dev.toml") == CFG
    text = (ROOT / "config.toml").read_text()
    for bad, match in ((text.replace("latency_ms = 250\n", ""), "missing"),
                       (text.replace("taker_fee = 0.0005", "taker_fee = 5"), "fraction"),
                       (re.sub(r"(?m)^fallback_slippage_bps = .*$", "fallback_slippage_bps = -1",
                               text), ">= 0"),
                       (text.replace("latency_ms = 250", "latency_ms = 0.25"), "integer")):
        p = tmp_path / "c.toml"
        p.write_text(bad)
        with pytest.raises(ConfigError, match=match):
            load_paper_config(p)


def test_assumed_fill_uses_config_not_constants(frozen):
    o = Order(0, "sell", Decimal("1"), mid(frozen[0]))
    wide = replace(CFG, fallback_spread_bps=Decimal("10"), fallback_slippage_bps=Decimal("0"))
    assert assumed_fill(o, wide).slippage_bps == Decimal("5")
    assert assumed_fill(o, wide).fill_time_ms == CFG.latency_ms


PLACEHOLDER_BPS = Decimal(2)  # the uncalibrated value both fallbacks shipped with


def test_fallback_costs_are_calibrated_not_placeholders():
    for name in ("config.toml", "config.dev.toml"):
        cfg = load_paper_config(ROOT / name)
        assert (cfg.fallback_spread_bps, cfg.fallback_slippage_bps) != (PLACEHOLDER_BPS,) * 2
        assert cfg.fallback_spread_bps != PLACEHOLDER_BPS, name
        assert cfg.fallback_slippage_bps != PLACEHOLDER_BPS, name
        text = (ROOT / name).read_text()
        # The replaced placeholders stay visible, commented out, for traceability.
        assert "# Previous uncalibrated placeholders: fallback_spread_bps = 2, " \
               "fallback_slippage_bps = 2" in text, name


def test_fallback_costs_match_committed_calibration_summary():
    """Config = committed summary's p90, rounded up to 4 dp. The summary comes from a local
    full-data run (tesseract.paper.calibrate); the data itself is never committed."""
    s = json.loads((ROOT / "calibration" / "binance_fallback_summary.json").read_text())
    assert s["config_percentile"].startswith("p90")
    up = lambda v: Decimal(v).quantize(Decimal("0.0001"), rounding=ROUND_CEILING)
    assert CFG.fallback_spread_bps == up(s["spread"]["p90"]) == Decimal("0.0133")
    assert CFG.fallback_slippage_bps == up(s["impact_beyond_half_spread_bps"]["p90"]) == 0
    assert s["spread"]["n"] == 1857 and s["slippage_vs_mid_bps"]["n"] == 11142
    assert s["slippage_vs_mid_bps"]["excluded_not_absorbed"] == 0
