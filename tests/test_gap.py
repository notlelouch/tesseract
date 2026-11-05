import asyncio
import json

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer

from tesseract.recorder.backoff import Backoff
from tesseract.recorder.latency import LatencyLog
from tesseract.recorder.ratelimit import WindowLimiter
from tesseract.recorder.ws import GapTracker, WsConnection, stream_dir
from tesseract.recorder.writer import JsonlWriter

from conftest import fixture_lines

STREAMS = ["kline@1m@btcusdt", "markKline@1m@btcusdt", "ticker@1s"]


# ------------------------------------------------------------ GapTracker (pure)
def test_first_connect_is_not_a_gap():
    g = GapTracker()
    assert g.on_connected(100) is None


def test_gap_starts_at_last_message_before_drop():
    g = GapTracker()
    g.on_connected(100)
    g.on_message(150)
    g.on_message(200)
    g.on_disconnect(260)
    assert g.on_connected(900) == (200, 900)


def test_gap_without_messages_starts_at_disconnect():
    g = GapTracker()
    g.on_connected(100)
    g.on_disconnect(300)
    assert g.on_connected(500) == (300, 500)


def test_failed_reconnects_extend_one_gap():
    g = GapTracker()
    g.on_connected(100)
    g.on_message(200)
    g.on_disconnect(250)
    g.on_disconnect(400)  # connect attempt failed
    g.on_disconnect(800)  # and again
    assert g.on_connected(1000) == (200, 1000)
    # Next gap is measured fresh, not from the old last message.
    g.on_disconnect(1100)
    assert g.on_connected(1200) == (1100, 1200)


def test_stream_dir():
    assert stream_dir("kline@1m@btcusdt") == "ws_kline_1m_btcusdt"
    assert stream_dir("markKline@1m@btcusdt") == "ws_markkline_1m_btcusdt"
    assert stream_dir("ticker@1s") == "ws_ticker_1s"
    assert stream_dir("btcusdt@depth@100ms") == "ws_btcusdt_depth_100ms"


# ------------------------------------------------------------ WsConnection against a local server
class FakeVenue:
    """WS server that replays recorded frames. Drops connection #1 after sending them."""

    def __init__(self, frames, *, drop_first=True, silent=False):
        self.frames = frames
        self.drop_first = drop_first
        self.silent = silent
        self.connections = 0
        self.subscribes: list[list[dict]] = []
        self.second_connection = asyncio.Event()

    async def handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.connections += 1
        n = self.connections
        subs = []
        self.subscribes.append(subs)
        subs.append(json.loads((await ws.receive()).data))
        subs.append(json.loads((await ws.receive()).data))
        if not self.silent:
            for f in self.frames:
                await ws.send_str(f)
        if n >= 2:
            self.second_connection.set()
        if n == 1 and self.drop_first:
            await ws.close()
            return ws
        async for _ in ws:  # keep open until client closes
            pass
        return ws


async def fast_sleep(_s: float) -> None:
    await asyncio.sleep(0.01)


async def run_until_reconnected(tmp_path, venue: FakeVenue, stale_timeout_s=5.0):
    app = web.Application()
    app.router.add_get("/ws", venue.handler)
    server = TestServer(app)
    await server.start_server()
    session = aiohttp.ClientSession()
    writer = JsonlWriter(tmp_path, fsync=False)
    subscribed_calls = []
    frames_seen = []

    async def on_subscribed(is_reconnect):
        subscribed_calls.append(is_reconnect)

    conn = WsConnection(
        session, venue="mudrex", name="linear", url=str(server.make_url("/ws")), streams=STREAMS,
        subscribe_messages=lambda: [{"id": 1, "method": "SUBSCRIBE", "params": STREAMS[:2]},
                                    {"id": 2, "method": "SUBSCRIBE", "params": ["ticker@1s"], "assets": ["btcusdt"]}],
        exch_ts=lambda obj: None, writer=writer, latency=LatencyLog(writer),
        connect_limiter=WindowLimiter(10, 60), backoff=Backoff(1.0, 60.0),
        ping_interval_s=20, stale_timeout_s=stale_timeout_s, on_subscribed=on_subscribed,
        on_frame=lambda stream, obj, recv: frames_seen.append(stream), sleep=fast_sleep)
    task = asyncio.create_task(conn.run())
    try:
        await asyncio.wait_for(venue.second_connection.wait(), timeout=10)
        await asyncio.sleep(0.2)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await session.close()
        await server.close()
    writer.close()
    return conn, subscribed_calls, frames_seen


def rows(tmp_path, *parts):
    out = []
    for p in sorted(tmp_path.joinpath(*parts).rglob("*.jsonl")):
        out += [json.loads(l) for l in p.read_text().splitlines()]
    return out


async def test_reconnect_resubscribes_and_writes_gap_markers(tmp_path):
    frames = fixture_lines("mudrex_ws_frames.jsonl")
    venue = FakeVenue(frames)
    conn, subscribed_calls, frames_seen = await run_until_reconnected(tmp_path, venue)

    # Re-subscribed with the same messages on the new connection.
    assert venue.connections == 2
    assert venue.subscribes[0] == venue.subscribes[1]
    assert subscribed_calls == [False, True]  # fires on first connect and on reconnect

    for s in STREAMS:
        recs = rows(tmp_path, "raw", "mudrex", stream_dir(s))
        types = [r["type"] for r in recs]
        assert types[0] == "session_start"
        gaps = [r for r in recs if r["type"] == "gap"]
        assert len(gaps) == 1, s
        gap = gaps[0]
        assert gap["conn_id"] == 2 and gap["reason"] == "reconnect"
        assert gap["gap_start_ns"] <= gap["gap_end_ns"]
        # The gap starts at the last frame received on connection 1.
        conn1_data = [r for r in rows(tmp_path, "raw", "mudrex") if r.get("conn_id") == 1 and r["type"] == "data"]
        assert gap["gap_start_ns"] == max(r["recv_ns"] for r in conn1_data)

    # on_frame sees stream frames only (not subscribe acks), once per frame per connection.
    stream_frames = [f for f in frames if '"stream"' in f]
    assert len(frames_seen) == 2 * len(stream_frames)

    # Every recorded frame is stored verbatim; frames arrived on both connections.
    data = [r for r in rows(tmp_path, "raw", "mudrex") if r["type"] == "data"]
    assert sorted(r["raw"] for r in data) == sorted(frames * 2)

    lat = rows(tmp_path, "latency")
    assert [r["event"] for r in lat].count("ws_connect") == 2
    assert any(r["event"] == "ws_disconnect" and "closed by server" in r["reason"] for r in lat)


async def test_stale_connection_is_recycled_with_gap(tmp_path):
    venue = FakeVenue([], drop_first=False, silent=True)
    conn, _, _ = await run_until_reconnected(tmp_path, venue, stale_timeout_s=0.2)
    lat = rows(tmp_path, "latency")
    assert any(r["event"] == "ws_disconnect" and r["reason"].startswith("stale") for r in lat)
    gaps = [r for r in rows(tmp_path, "raw", "mudrex", "ws_ticker_1s") if r["type"] == "gap"]
    assert len(gaps) == 1
