"""Binance USD-M diff-depth sequence validator.

Implements the sequence part of Binance's documented USD-M procedure
("How to manage a local order book correctly"):

  - buffer stream events until a REST snapshot is loaded;
  - drop events where u < lastUpdateId;
  - the first processed event must have U <= lastUpdateId AND u >= lastUpdateId
    (USD-M rule; the spot API uses lastUpdateId+1 instead);
  - each next event's pu must equal the previous event's u, otherwise
    re-initialize from a new snapshot.

It tracks update IDs only, not price levels: the recorder stores raw frames,
and the book itself is rebuilt later from those. What this adds is the
verdict, written alongside the raw data: `depth_synced` when the recorded
stream becomes rebuildable and a `gap` record when it stops being so.
Pure logic, no I/O.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass

log = logging.getLogger(__name__)

WAITING = "waiting_snapshot"
SYNCED = "synced"


@dataclass(frozen=True)
class Diff:
    U: int  # first update id in event
    u: int  # final update id in event
    pu: int  # final update id of previous event
    recv_ns: int


@dataclass(frozen=True)
class Synced:
    last_update_id: int
    snapshot_recv_ns: int
    first: Diff  # first processed event: U <= lastUpdateId <= u
    applied: int  # buffered events accepted at sync time (including `first`)
    dropped: int  # events dropped since waiting began because u < lastUpdateId


@dataclass(frozen=True)
class Break:
    """The recorded stream cannot rebuild the book from gap_start_ns until the next Synced."""

    kind: str  # "sequence_break" | "snapshot_stale"
    gap_start_ns: int
    gap_end_ns: int
    detail: dict


@dataclass(frozen=True)
class NeedSnapshot:
    reason: str


Event = Synced | Break | NeedSnapshot


class DepthSync:
    def __init__(self, max_buffer: int = 20_000):
        self.state = WAITING
        self._buf: deque[Diff] = deque()
        self._max_buffer = max_buffer
        self._snap: tuple[int, int] | None = None  # (lastUpdateId, recv_ns) awaiting a bridging event
        self.last: Diff | None = None
        self.applied = 0
        self._dropped = 0

    def reset(self) -> None:
        """Start over (new connection): wait for a fresh snapshot."""
        self.state = WAITING
        self._buf.clear()
        self._snap = None
        self.last = None
        self._dropped = 0

    @property
    def buffered(self) -> int:
        return len(self._buf)

    # ------------------------------------------------------------ inputs
    def on_diff(self, d: Diff) -> list[Event]:
        if self.state == SYNCED:
            assert self.last is not None
            if d.pu != self.last.u:
                brk = Break("sequence_break", self.last.recv_ns, d.recv_ns,
                            {"expected_pu": self.last.u, "got_pu": d.pu, "U": d.U, "u": d.u})
                self.state = WAITING
                self.last = None
                self._snap = None
                self._buf.clear()
                self._push(d)
                return [brk, NeedSnapshot("sequence_break")]
            self.last = d
            self.applied += 1
            return []
        self._push(d)
        return self._try_sync() if self._snap is not None else []

    def on_snapshot(self, last_update_id: int, recv_ns: int) -> list[Event]:
        if self.state == SYNCED:
            return []  # periodic checkpoint while healthy; nothing to do
        self._snap = (last_update_id, recv_ns)
        return self._try_sync()

    # ------------------------------------------------------------ internals
    def _push(self, d: Diff) -> None:
        if len(self._buf) >= self._max_buffer:
            self._buf.popleft()
            log.warning("depth sync buffer full (%d); dropping oldest buffered id range", self._max_buffer)
        self._buf.append(d)

    def _try_sync(self) -> list[Event]:
        assert self._snap is not None
        last_id, snap_ns = self._snap
        while self._buf and self._buf[0].u < last_id:
            self._buf.popleft()
            self._dropped += 1
        if not self._buf:
            return []  # snapshot is ahead of everything buffered; wait for the bridging event
        first = self._buf[0]
        if not (first.U <= last_id <= first.u):
            # first.U > lastUpdateId: ids between the snapshot and the stream are missing.
            self._snap = None
            return [
                Break("snapshot_stale", first.recv_ns, max(first.recv_ns, snap_ns),
                      {"last_update_id": last_id, "first_U": first.U, "first_u": first.u}),
                NeedSnapshot("snapshot_stale"),
            ]
        events = list(self._buf)
        for i in range(1, len(events)):
            prev, cur = events[i - 1], events[i]
            if cur.pu != prev.u:
                self._snap = None
                self._buf = deque(events[i:])
                return [
                    Break("sequence_break", prev.recv_ns, cur.recv_ns,
                          {"expected_pu": prev.u, "got_pu": cur.pu, "U": cur.U, "u": cur.u}),
                    NeedSnapshot("sequence_break"),
                ]
        self.state = SYNCED
        self.last = events[-1]
        self.applied = len(events)
        self._buf.clear()
        self._snap = None
        dropped, self._dropped = self._dropped, 0
        return [Synced(last_id, snap_ns, first, len(events), dropped)]
