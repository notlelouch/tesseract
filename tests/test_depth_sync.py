"""DepthSync against a real recorded Binance USD-M session (tests/fixtures/binance_depth_session.jsonl):
two REST depth snapshots and 21 diff frames from btcusdt@depth@100ms, in receive order."""

import json

import pytest

from tesseract.recorder.depth_sync import SYNCED, WAITING, Break, DepthSync, Diff, NeedSnapshot, Synced

from conftest import fixture_lines


def load_session():
    snaps, diffs, timeline = [], [], []
    for line in fixture_lines("binance_depth_session.jsonl"):
        rec = json.loads(line)
        raw = json.loads(rec["raw"])
        if rec["stream"] == "rest_depth":
            item = ("snap", raw["lastUpdateId"], rec["recv_ns"])
            snaps.append(item)
        else:
            d = raw["data"]
            item = ("diff", Diff(d["U"], d["u"], d["pu"], rec["recv_ns"]))
            diffs.append(item[1])
        timeline.append(item)
    return snaps, diffs, timeline


SNAPS, DIFFS, TIMELINE = load_session()
STALE_SNAP, GOOD_SNAP = SNAPS  # first was taken before the WS started; second mid-stream


def test_fixture_is_a_real_contiguous_session():
    assert len(SNAPS) == 2 and len(DIFFS) == 21
    assert all(b.pu == a.u for a, b in zip(DIFFS, DIFFS[1:]))  # recorded stream had no breaks
    assert STALE_SNAP[1] < DIFFS[0].U  # really stale


def test_buffers_until_snapshot_then_syncs_in_receive_order():
    s = DepthSync()
    events = []
    for item in TIMELINE:
        if item[0] == "diff":
            events += s.on_diff(item[1])
        elif item == GOOD_SNAP:
            assert s.state == WAITING and s.buffered > 0  # diffs were buffered while waiting
            events += s.on_snapshot(item[1], item[2])
    synced = [e for e in events if isinstance(e, Synced)]
    assert len(synced) == 1 and not any(isinstance(e, Break) for e in events)
    sy = synced[0]
    L = GOOD_SNAP[1]
    # Documented USD-M rule: U <= lastUpdateId AND u >= lastUpdateId.
    assert sy.first.U <= L <= sy.first.u
    # Every event dropped had u < lastUpdateId; every applied one is at or after the bridge.
    # In this real session the snapshot was ahead of every diff buffered at that moment
    # (stream lagged REST), so the bridge arrived after the snapshot.
    assert sy.first.recv_ns > GOOD_SNAP[2]
    buffered_before = [d for d in DIFFS if d.recv_ns <= sy.first.recv_ns]
    assert sy.dropped == sum(1 for d in buffered_before if d.u < L)
    assert s.state == SYNCED
    assert s.last == DIFFS[-1]
    assert s.applied == sum(1 for d in DIFFS if d.u >= L)


def test_snapshot_before_any_diff_also_syncs():
    s = DepthSync()
    assert s.on_snapshot(GOOD_SNAP[1], GOOD_SNAP[2]) == []  # nothing buffered yet
    events = []
    for d in DIFFS:
        events += s.on_diff(d)
    assert [type(e) for e in events] == [Synced]
    assert s.state == SYNCED


def test_stale_snapshot_is_a_gap_and_requests_a_new_one():
    s = DepthSync()
    for d in DIFFS[:5]:
        s.on_diff(d)
    events = s.on_snapshot(STALE_SNAP[1], STALE_SNAP[2])
    assert [type(e) for e in events] == [Break, NeedSnapshot]
    brk = events[0]
    assert brk.kind == "snapshot_stale"
    assert brk.detail["last_update_id"] == STALE_SNAP[1] and brk.detail["first_U"] == DIFFS[0].U
    assert brk.gap_start_ns == DIFFS[0].recv_ns
    assert s.state == WAITING
    # The fresh snapshot then syncs.
    for d in DIFFS[5:]:
        s.on_diff(d)
    assert [type(e) for e in s.on_snapshot(GOOD_SNAP[1], GOOD_SNAP[2])] == [Synced]


def test_missing_diff_after_sync_is_a_sequence_break():
    s = DepthSync()
    s.on_snapshot(GOOD_SNAP[1], GOOD_SNAP[2])
    synced_at = next(i for i, d in enumerate(DIFFS) if any(isinstance(e, Synced) for e in s.on_diff(d)))
    last_good = DIFFS[synced_at + 1]
    assert s.on_diff(last_good) == []
    missing, after = DIFFS[synced_at + 2], DIFFS[synced_at + 3]  # drop one frame
    events = s.on_diff(after)
    assert [type(e) for e in events] == [Break, NeedSnapshot]
    brk = events[0]
    assert brk.kind == "sequence_break"
    assert brk.detail == {"expected_pu": last_good.u, "got_pu": after.pu, "U": after.U, "u": after.u}
    assert (brk.gap_start_ns, brk.gap_end_ns) == (last_good.recv_ns, after.recv_ns)
    assert missing.u == after.pu  # the pu chain names exactly the dropped frame
    assert s.state == WAITING and s.buffered == 1


def test_break_inside_buffer_is_detected_at_sync_time():
    s = DepthSync()
    pre = [d for d in DIFFS if d.u >= GOOD_SNAP[1]]
    for d in pre[:3] + pre[4:]:  # a frame missing while still waiting for the snapshot
        s.on_diff(d)
    events = s.on_snapshot(GOOD_SNAP[1], GOOD_SNAP[2])
    assert [type(e) for e in events] == [Break, NeedSnapshot]
    assert events[0].kind == "sequence_break" and events[0].detail["expected_pu"] == pre[2].u


def test_snapshot_ignored_while_synced():
    s = DepthSync()
    s.on_snapshot(GOOD_SNAP[1], GOOD_SNAP[2])
    for d in DIFFS:
        s.on_diff(d)
    assert s.on_snapshot(STALE_SNAP[1], STALE_SNAP[2]) == []
    assert s.state == SYNCED


@pytest.mark.parametrize(
    "U,u,ok",
    [
        (100, 100, True),   # u == lastUpdateId: accepted by the USD-M rule (spot rule would reject)
        (90, 110, True),
        (101, 110, False),  # U == lastUpdateId+1: rejected by the USD-M rule (spot rule would accept)
    ],
)
def test_usdm_first_event_boundary(U, u, ok):
    s = DepthSync()
    s.on_diff(Diff(U, u, U - 1, 1))
    events = s.on_snapshot(100, 2)
    assert (type(events[0]) is Synced) == ok
