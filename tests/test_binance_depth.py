"""Binance depth wiring: snapshot on first connect, depth_synced / gap records, re-snapshot.
Uses the real recorded session in tests/fixtures/binance_depth_session.jsonl; REST is stubbed."""

import dataclasses
import json
from pathlib import Path

from tesseract.config import load_config
from tesseract.recorder.binance import Binance
from tesseract.recorder.http import RestResult
from tesseract.recorder.latency import LatencyLog
from tesseract.recorder.writer import JsonlWriter

from conftest import FixedNsClock, fixture_lines

ROOT = Path(__file__).parent.parent
DEPTH = "btcusdt@depth@100ms"


def load():
    snaps, frames, start = [], [], None
    for line in fixture_lines("binance_depth_session.jsonl"):
        rec = json.loads(line)
        t = rec.get("req_start_ns") or rec["recv_ns"]
        start = t if start is None else min(start, t)
        if rec["stream"] == "rest_depth":
            snaps.append(rec["raw"])
        else:
            frames.append((json.loads(rec["raw"]), rec["recv_ns"]))
    return snaps, frames, start


# SESSION_START_NS: earliest timestamp in the recorded session (2026-11-05 22:24 UTC); the
# injected clock starts there.
SNAPS, FRAMES, SESSION_START_NS = load()
STALE, GOOD = SNAPS
GOOD_ID = json.loads(GOOD)["lastUpdateId"]


def make(tmp_path, responses):
    cfg = load_config(ROOT / "config.toml")
    cfg = dataclasses.replace(cfg, binance=dataclasses.replace(cfg.binance, depth_resync_delay_s=0))
    writer = JsonlWriter(tmp_path, fsync=False)
    # Pinned to the recorded session so every timestamp written lands on the fixtures' UTC day,
    # whatever date the tests run on.
    clock = FixedNsClock(SESSION_START_NS)
    b = Binance(cfg.binance, cfg.backoff, None, writer, LatencyLog(writer), timeout_s=10, max_retries=0,
                clock=clock)
    calls = []

    async def fake_get(endpoint, path, params, **kw):
        # Stamps start/end from the injected clock, as the real RestClient does.
        calls.append(endpoint)
        start = clock()
        if not responses:  # out of scripted snapshots: behave like a failed request
            return RestResult(endpoint, "https://fapi.binance.com" + path, params, 503, "",
                              start, clock(), 1)
        return RestResult(endpoint, "https://fapi.binance.com" + path, params, 200,
                          responses.pop(0), start, clock(), 1)

    b.rest.get = fake_get
    return b, writer, calls


def depth_records(tmp_path, writer):
    writer.flush()
    d = tmp_path / "raw" / "binance" / "ws_btcusdt_depth_100ms"
    return [json.loads(l) for p in sorted(d.glob("*.jsonl")) for l in p.read_text().splitlines()]


async def test_first_connect_takes_snapshot_and_syncs(tmp_path):
    b, writer, calls = make(tmp_path, [GOOD])
    await b._on_public_subscribed(False)  # first connect
    assert calls == ["depth"]  # snapshot requested immediately, not only on reconnects
    for obj, recv in FRAMES:
        b._on_public_frame(DEPTH, obj, recv)
    recs = depth_records(tmp_path, writer)
    synced = [r for r in recs if r["type"] == "depth_synced"]
    assert len(synced) == 1 and not [r for r in recs if r["type"] == "gap"]
    s = synced[0]
    assert s["last_update_id"] == GOOD_ID
    assert s["first_U"] <= GOOD_ID <= s["first_u"]


async def test_stale_first_snapshot_writes_gap_then_resyncs(tmp_path):
    b, writer, calls = make(tmp_path, [STALE, GOOD])
    await b._on_public_subscribed(False)
    obj, recv = FRAMES[0]
    b._on_public_frame(DEPTH, obj, recv)  # first diff is newer than the stale snapshot
    task = b._resnap_task
    assert task is not None
    await task
    assert calls == ["depth", "depth"]
    for obj, recv in FRAMES[1:]:
        b._on_public_frame(DEPTH, obj, recv)
    recs = depth_records(tmp_path, writer)
    types = [r["type"] for r in recs]
    assert types == ["gap", "depth_synced"]
    gap = recs[0]
    assert gap["reason"] == "snapshot_stale" and gap["last_update_id"] == json.loads(STALE)["lastUpdateId"]
    assert gap["gap_start_ns"] <= gap["gap_end_ns"]


async def test_missing_frame_writes_sequence_gap_and_resnapshots(tmp_path):
    b, writer, calls = make(tmp_path, [GOOD, GOOD])
    await b._on_public_subscribed(False)
    feed = [f for f in FRAMES if f[0]["data"]["u"] >= GOOD_ID]
    for obj, recv in feed[:3]:
        b._on_public_frame(DEPTH, obj, recv)
    b._on_public_frame(DEPTH, *feed[4])  # feed[3] lost
    await b._resnap_task  # re-snapshot returns the same (now old) snapshot -> judged stale
    await b._resnap_task  # next re-snapshot fails (503) -> nothing fed, loop stops
    assert b._resnap_task is None
    recs = depth_records(tmp_path, writer)
    gaps = [r for r in recs if r["type"] == "gap"]
    assert [x["reason"] for x in gaps] == ["sequence_break", "snapshot_stale"]
    g = gaps[0]
    assert g["reason"] == "sequence_break"
    assert g["expected_pu"] == feed[2][0]["data"]["u"] and g["got_pu"] == feed[4][0]["data"]["pu"]
    assert (g["gap_start_ns"], g["gap_end_ns"]) == (feed[2][1], feed[4][1])
    assert calls == ["depth", "depth", "depth"]


async def test_snapshot_requested_before_subscribe_is_not_fed(tmp_path):
    b, writer, calls = make(tmp_path, [GOOD])
    b._sync_epoch_ns = b.clock() + 10**12  # subscription "after" this request started
    await b.snapshot_depth()
    assert b.depth_sync.state == "waiting_snapshot"
    assert depth_records(tmp_path, writer) == []
    # The REST response itself is still recorded verbatim.
    writer.flush()
    rest = (tmp_path / "raw" / "binance" / "rest_depth").glob("*.jsonl")
    assert [json.loads(l)["raw"] for p in rest for l in p.read_text().splitlines()] == [GOOD]


async def test_non_depth_frames_ignored(tmp_path):
    b, writer, calls = make(tmp_path, [GOOD])
    b._on_public_frame("btcusdt@bookTicker", {"data": {}}, 1)
    assert depth_records(tmp_path, writer) == []
