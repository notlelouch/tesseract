"""Replay orders Binance depth diffs by sequence (`u`, then `E`), never by local receive time.

Data path is the production one, as in test_paper_fills: the recorded fixture
tests/fixtures/binance_depth_session.jsonl is written into a raw tree by the recorder's own code,
parsed, then replayed. No payload is written by hand; tests only move recorded frames or shift
their `recv_ns`, the way a macOS NTP step back (30-64 ms in the calibration recording) does.
"""

import asyncio
import copy
import json
import random
import shutil

import pytest

from tesseract.paper.book import Book, BookState, replay
from tesseract.paper.calibrate import DEPTH_STREAM, calibrate

from test_parser import parse, record_tree, typed

NTP_STEP_NS = 64_000_000  # largest step back seen in the calibration recording


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("order")
    raw = asyncio.run(record_tree(tmp))
    assert parse(tmp, raw)["totals"]["parse_failures"] == 0
    return tmp


@pytest.fixture(scope="module")
def depth(tree):
    return typed(tree, "binance", DEPTH_STREAM)


@pytest.fixture(scope="module")
def snaps(tree):
    return typed(tree, "binance", "rest_depth")


def run(depth, snaps):
    """Replay with each state's book copied, as (time, u, segment, bids, asks)."""
    return [(s.time_ms, s.update_id, s.segment, dict(s.book.bids), dict(s.book.asks))
            for s in replay(depth, snaps)]


def synced_ms(states) -> int:
    """Synced book time: per segment, last state E minus first state E."""
    spans: dict[int, list[int]] = {}
    for t, _, seg, _, _ in states:
        spans.setdefault(seg, [t, t])[1] = t
    return sum(b - a for a, b in spans.values())


def segment_diff_positions(depth) -> list[int]:
    """List positions of the diffs replay applies in the synced segment, bridge first."""
    (synced,) = [r for r in depth if r["type"] == "depth_synced"]
    pos = [i for i, r in enumerate(depth) if r.get("kind") == "depth_diff"]
    start = next(n for n, i in enumerate(pos)
                 if (depth[i]["first_update_id"], depth[i]["final_update_id"])
                 == (synced["first_U"], synced["first_u"]))
    return pos[start:]


def ntp_step(depth, a: int, b: int) -> list[dict]:
    """Adjacent diffs at positions a < b: b now arrives first in the file and its recv_ns is
    stepped back NTP_STEP_NS below a's, as when the wall clock steps back between them."""
    out = copy.deepcopy(depth)
    out[b]["recv_ns"] = out[a]["recv_ns"] - NTP_STEP_NS
    out[a], out[b] = out[b], out[a]
    return out


@pytest.fixture(scope="module")
def baseline(depth, snaps):
    states = run(depth, snaps)
    assert len(states) > 10 and {s[2] for s in states} == {0}
    return states


def test_ntp_step_swapping_adjacent_frames_keeps_segment_and_book(depth, snaps, baseline):
    pos = segment_diff_positions(depth)
    a, b = pos[len(pos) // 2], pos[len(pos) // 2 + 1]
    stepped = ntp_step(depth, a, b)
    # The swap really reverses these two frames by local receive time.
    assert stepped[a]["recv_ns"] < stepped[b]["recv_ns"]
    assert stepped[a]["final_update_id"] > stepped[b]["final_update_id"]
    got = run(stepped, snaps)
    assert {s[2] for s in got} == {0}  # still one unbroken segment
    assert got == baseline  # same states, same books, same order as the in-order replay


def test_clock_step_never_reduces_synced_time(depth, snaps, baseline, tree, tmp_path):
    want_ms, want_n = synced_ms(baseline), len(baseline)
    pos = segment_diff_positions(depth)
    for a, b in zip(pos, pos[1:]):  # a step between every adjacent pair of synced frames
        got = run(ntp_step(depth, a, b), snaps)
        assert (synced_ms(got), len(got), {s[2] for s in got}) == (want_ms, want_n, {0}), (a, b)

    # Same through the calibration path (segmenter + replay) on a typed tree with the step.
    shutil.copytree(tree / "typed", tmp_path / "typed")
    (f,) = sorted((tmp_path / "typed" / "binance" / DEPTH_STREAM).glob("*.jsonl"))
    recs = [json.loads(l) for l in f.read_text().splitlines()]
    fpos = segment_diff_positions(recs)
    a, b = fpos[len(fpos) // 2], fpos[len(fpos) // 2 + 1]
    f.write_text("".join(json.dumps(r) + "\n" for r in ntp_step(recs, a, b)))
    assert calibrate(tmp_path / "typed", 100) == calibrate(tree / "typed", 100)


def test_replay_does_not_order_by_receive_time(depth, snaps, baseline):
    """Diff recv_ns is reversed (later frames 'arrive' earlier) and the list is shuffled. Sorting
    by recv_ns would apply the segment backwards and break at the first pu check; arrival order
    would apply it shuffled. Only sequence order reproduces the in-order replay."""
    pos = segment_diff_positions(depth)
    out = copy.deepcopy(depth)
    t0 = out[pos[0]]["recv_ns"]  # keep the bridge's recv_ns; later diffs step back from it
    for k, i in enumerate(pos[1:], 1):
        out[i]["recv_ns"] = t0 - k * NTP_STEP_NS
    random.Random(0).shuffle(out)

    by_recv = sorted((r for r in out if r.get("kind") == "depth_diff"), key=lambda r: r["recv_ns"])
    seg_u = [depth[i]["final_update_id"] for i in pos]
    assert [r["final_update_id"] for r in by_recv if r["final_update_id"] in set(seg_u)] \
        == list(reversed(seg_u[1:])) + [seg_u[0]]  # receive-time order really is wrong

    assert run(out, snaps) == baseline


def test_states_follow_the_pu_u_chain(baseline, depth):
    """Each yielded state's diff links to the previous one: pu == previous u."""
    by_u = {r["final_update_id"]: r for r in depth if r.get("kind") == "depth_diff"}
    us = [s[1] for s in baseline]
    assert all(by_u[u]["prev_final_update_id"] == p for p, u in zip(us, us[1:]))
    assert [s[0] for s in baseline] == sorted(s[0] for s in baseline)
