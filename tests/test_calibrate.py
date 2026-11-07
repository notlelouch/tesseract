"""Fallback spread/slippage calibration on the committed fixtures only.

The fixtures are recorded into a raw tree by the recorder's own code and parsed, as in
test_paper_fills. The recorded Binance session has ~1.3 s of synced book (13 states), so no
minute boundary falls inside it: the reproduction test samples every 100 ms instead of the
production 60 s. Full-data calibration runs locally; only its summary is committed.
"""

import asyncio
import json
import shutil
from decimal import Decimal
from pathlib import Path

import pytest

from tesseract.paper.book import Book, BookState, replay
from tesseract.paper.calibrate import (DEPTH_STREAM, calibrate, main, percentile, sample,
                                       segment_states)

from test_parser import parse, record_tree, typed


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("calib")
    raw = asyncio.run(record_tree(tmp))
    assert parse(tmp, raw)["totals"]["parse_failures"] == 0
    return tmp


def frozen(states):
    out = []
    for s in states:
        b = Book([], [])
        b.bids, b.asks = dict(s.book.bids), dict(s.book.asks)
        out.append(BookState(s.time_ms, s.update_id, s.segment, b))
    return out


def full_replay(tmp: Path):
    return frozen(replay(typed(tmp, "binance", DEPTH_STREAM), typed(tmp, "binance", "rest_depth")))


def key(states):
    return [(s.time_ms, s.update_id, s.segment, s.book.bids, s.book.asks) for s in states]


# --------------------------------------------------------------------------- reproduction

# Reported fixture numbers (interval 100 ms). Exact decimal strings.
SPREAD = "0.01316689500257083624925195578"
HALF = "0.006583447501285418124625977889"
EXPECTED = {
    "interval_s": 0.1,
    "first_sample": "2026-11-05T22:25:42Z",
    "last_sample": "2026-11-05T22:25:43Z",
    "segments_sampled": 1,
    "spread": {"n": 12, "p50": SPREAD, "p75": SPREAD, "p90": SPREAD},
    "order_sizes_btc": ["0.01", "0.1", "1"],
    "slippage_vs_mid_bps": {"n": 72, "excluded_not_absorbed": 0, "p50": HALF, "p75": HALF,
                            "p90": HALF},
    "impact_beyond_half_spread_bps": {"n": 72, "p50": "0", "p75": "0", "p90": "0"},
}
EXPECTED_1BTC = {"n": 24,
                 "slippage_vs_mid_bps": {"p50": HALF, "p75": HALF,
                                         "p90": "0.2530018874743986185293763303"},
                 "impact_beyond_half_spread_bps": {"p50": "0", "p75": "0",
                                                   "p90": "0.2464184399731132004047503524"}}


def test_calibration_reproduces_reported_fixture_numbers(tree, capsys):
    got = calibrate(tree / "typed", interval_ms=100)
    assert {k: got[k] for k in EXPECTED} == EXPECTED
    assert got["by_size"]["1"] == EXPECTED_1BTC
    # The CLI prints the same summary, and nothing but aggregates.
    assert main(["--typed", str(tree / "typed"), "--interval-s", "1"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed == calibrate(tree / "typed", interval_ms=1000)
    text = json.dumps(printed)
    assert not any(f'"{k}"' in text for k in ("bids", "asks", "raw", "raw_line", "recv_ns"))


def test_production_interval_needs_a_minute_of_book(tree):
    with pytest.raises(ValueError, match="no synced Binance book samples"):
        calibrate(tree / "typed")  # 60 s default: the fixture's 1.3 s of book has no boundary


def test_mudrex_data_is_never_read(tree, tmp_path):
    only = tmp_path / "typed"
    shutil.copytree(tree / "typed" / "binance", only / "binance")
    assert not (only / "mudrex").exists()
    assert calibrate(only, 100) == calibrate(tree / "typed", 100)


# --------------------------------------------------------------------------- method


def test_segmented_replay_equals_full_replay(tree):
    assert key(frozen(segment_states(tree / "typed"))) == key(full_replay(tree))


def test_segmented_replay_equals_full_replay_with_recv_clock_step_back(tree, tmp_path):
    """Recorded data has wall-clock steps back of up to ~64 ms; the segmenter must hand replay the
    same diffs it would see unsegmented."""
    shutil.copytree(tree / "typed", tmp_path / "typed")
    (f,) = sorted((tmp_path / "typed" / "binance" / DEPTH_STREAM).glob("*.jsonl"))
    recs = [json.loads(l) for l in f.read_text().splitlines()]
    diffs = [i for i, r in enumerate(recs) if r.get("kind") == "depth_diff"]
    j = diffs[-4]
    recs[j]["recv_ns"] = recs[diffs[-5]]["recv_ns"] - 64_000_000
    f.write_text("".join(json.dumps(r) + "\n" for r in recs))
    assert key(frozen(segment_states(tmp_path / "typed"))) == key(full_replay(tmp_path))


def test_spread_and_impact_match_independent_arithmetic(tree):
    for s in full_replay(tree):
        bid, ask = max(s.book.bids), min(s.book.asks)
        mid = (bid + ask) / 2
        got = sample(s)
        assert abs(got.spread_bps - (ask - bid) / mid * 10_000) < Decimal("1e-24")
        for (side, q), (slip, impact) in got.fills.items():
            # slippage vs mid = half spread + impact, up to the 28-digit rounding of each division
            assert abs(slip - got.spread_bps / 2 - impact) < Decimal("1e-24")
            touch = ask if side == "buy" else bid
            size_at_touch = (s.book.asks if side == "buy" else s.book.bids)[touch]
            if q <= size_at_touch:
                assert impact == 0  # filled entirely at the best level: exactly zero
            else:
                assert impact > 0


def test_nearest_rank_percentile():
    v = [Decimal(x) for x in range(1, 11)]  # 1..10
    assert [percentile(v, p) for p in (50, 75, 90, 100)] == [5, 8, 9, 10]
    assert percentile([Decimal("0.5")], 90) == Decimal("0.5")
    assert type(percentile(v, 90)) is Decimal
    with pytest.raises(ValueError):
        percentile([], 50)
