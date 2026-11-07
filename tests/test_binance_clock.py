"""The Binance client must take every timestamp it writes from its injected clock, never from the
real wall clock. Otherwise recorded-fixture tests depend on the date they run on.

Two checks:
- runtime: the real client (real RestClient and WsConnection against a local server replaying the
  recorded depth session, with one reconnect) runs while every real-clock read through
  records.now_ns is logged; none may happen, and every *_ns field written must come from the
  injected clock;
- static: binance.py never imports a time source and names now_ns only as the clock default.
"""

import ast
import asyncio
import dataclasses
import inspect
import json
import traceback
from pathlib import Path

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer

from tesseract.config import load_config
from tesseract.recorder import binance as binance_mod
from tesseract.recorder import records
from tesseract.recorder.binance import Binance
from tesseract.recorder.latency import LatencyLog
from tesseract.recorder.writer import JsonlWriter

from conftest import FixedNsClock, fixture_lines
from test_binance_depth import GOOD, SESSION_START_NS

ROOT = Path(__file__).parent.parent
FRAMES = [json.loads(l)["raw"] for l in fixture_lines("binance_depth_session.jsonl")
          if json.loads(l)["stream"] != "rest_depth"]


class RealClockSpy:
    """Stands in for the `time` module inside records.py, so every now_ns() call from any module
    is caught. Logs rather than raises: WsConnection swallows exceptions from on_frame hooks."""

    def __init__(self):
        self.calls: list[str] = []

    def time_ns(self) -> int:
        self.calls.append("".join(traceback.format_stack(limit=6)[:-1]))
        return 0


class FakeBinance:
    """REST depth snapshot + public WS that replays the recorded depth frames. Connection 1 is
    closed by the server after the frames, so the client also writes a reconnect gap."""

    def __init__(self):
        self.connections = 0

    async def depth(self, request):
        return web.Response(text=GOOD, content_type="application/json")

    async def ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.connections += 1
        await ws.receive()  # SUBSCRIBE
        for f in FRAMES:
            await ws.send_str(f)
        if self.connections == 1:
            await ws.close()
            return ws
        async for _ in ws:
            pass
        return ws


def ns_fields(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.endswith("_ns") and isinstance(v, int):
                yield f"{path}{k}", v
            yield from ns_fields(v, f"{path}{k}.")


async def test_binance_client_never_reads_real_clock(tmp_path, monkeypatch):
    venue = FakeBinance()
    app = web.Application()
    app.router.add_get("/fapi/v1/depth", venue.depth)
    app.router.add_get("/ws", venue.ws)
    server = TestServer(app)
    await server.start_server()

    cfg = load_config(ROOT / "config.toml")
    bcfg = dataclasses.replace(cfg.binance, rest_base=str(server.make_url("")).rstrip("/"),
                               ws_public_url=str(server.make_url("/ws")), depth_resync_delay_s=0)
    backoff = dataclasses.replace(cfg.backoff, base_s=0.01, cap_s=0.01)

    spy = RealClockSpy()
    monkeypatch.setattr(records, "time", spy)
    clock = FixedNsClock(SESSION_START_NS)
    t0 = clock.t

    session = aiohttp.ClientSession()
    writer = JsonlWriter(tmp_path, fsync=False)
    b = Binance(bcfg, backoff, session, writer, LatencyLog(writer), timeout_s=5, max_retries=0,
                clock=clock)
    task = asyncio.create_task(b.ws_public.run())
    try:
        async def synced_after_reconnect():
            while not (venue.connections >= 2 and b.ws_public.reconnects == 1
                       and b.depth_sync.state == "synced"):
                await asyncio.sleep(0.01)
        try:
            await asyncio.wait_for(synced_after_reconnect(), timeout=5)
            synced = True
        except TimeoutError:  # a real-clock read can stall sync; report that cause first
            synced = False
        await b.poll_premium_index()  # another REST path (404 here; still recorded)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await session.close()
        await server.close()
    writer.close()

    assert spy.calls == [], "Binance client read the real clock:\n" + "\n---\n".join(spy.calls)
    assert synced, "depth never re-synced after the reconnect"

    recs = [json.loads(l) for p in sorted(tmp_path.rglob("*.jsonl")) for l in p.read_text().splitlines()]
    types = {r.get("type") or r.get("event") for r in recs}
    assert {"session_start", "gap", "depth_synced", "data", "rest", "ws_connect", "ws_disconnect"} <= types
    # Every timestamp written is either from the injected clock or copied from it (e.g. gap ends).
    stamped = [(k, v) for r in recs for k, v in ns_fields(r)]
    assert stamped
    bad = [(k, v) for k, v in stamped if not t0 <= v < clock.t]
    assert bad == []


def test_binance_module_has_no_direct_clock_access():
    tree = ast.parse(inspect.getsource(binance_mod))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not {a.name.split(".")[0] for a in node.names} & {"time", "datetime"}
        if isinstance(node, ast.ImportFrom):
            assert (node.module or "").split(".")[0] not in {"time", "datetime"}
    # now_ns may appear only as the default value of the `clock` parameter.
    init = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "__init__"
                and any(a.arg == "clock" for a in n.args.kwonlyargs))
    defaults = {id(d) for d in init.args.kw_defaults if d is not None}
    uses = [n for n in ast.walk(tree) if isinstance(n, ast.Name) and n.id == "now_ns"]
    assert uses and all(id(n) in defaults for n in uses), [n.lineno for n in uses if id(n) not in defaults]
