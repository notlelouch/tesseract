import asyncio
import json
from email.utils import format_datetime
from datetime import datetime, timezone

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from tesseract.recorder.http import RestClient
from tesseract.recorder.latency import LatencyLog
from tesseract.recorder.ratelimit import WindowLimiter, parse_retry_after
from tesseract.recorder.writer import JsonlWriter

from conftest import FakeClock, fixture_text


# ------------------------------------------------------------ Retry-After parsing
def test_retry_after_seconds():
    assert parse_retry_after("7") == 7.0
    assert parse_retry_after(" 0 ") == 0.0
    assert parse_retry_after("-3") == 0.0


def test_retry_after_http_date():
    now = 1_800_000_000.0
    hdr = format_datetime(datetime.fromtimestamp(now + 30, tz=timezone.utc), usegmt=True)
    assert parse_retry_after(hdr, now=now) == pytest.approx(30, abs=1)
    past = format_datetime(datetime.fromtimestamp(now - 30, tz=timezone.utc), usegmt=True)
    assert parse_retry_after(past, now=now) == 0.0


@pytest.mark.parametrize("v", [None, "", "soon", "Mon, 99 Foo"])
def test_retry_after_missing_or_garbage(v):
    assert parse_retry_after(v) is None


# ------------------------------------------------------------ WindowLimiter
async def test_limiter_allows_up_to_limit_then_waits(clock: FakeClock):
    lim = WindowLimiter(3, 60, clock=clock, sleep=clock.sleep)
    for _ in range(3):
        assert await lim.acquire() == 0
    waited = await lim.acquire()
    assert waited == pytest.approx(60)  # oldest event expires after the window
    assert lim.used == 1


async def test_limiter_weights(clock: FakeClock):
    lim = WindowLimiter(100, 60, clock=clock, sleep=clock.sleep)
    await lim.acquire(80)
    clock.t += 10
    await lim.acquire(20)
    assert lim.wait_time(1) == pytest.approx(50)  # first spend (80) frees at t+60
    with pytest.raises(ValueError):
        lim.wait_time(101)


async def test_limiter_block_holds_all_callers(clock: FakeClock):
    lim = WindowLimiter(1000, 60, clock=clock, sleep=clock.sleep)
    lim.block_for(12)
    assert await lim.acquire() == pytest.approx(12)
    assert await lim.acquire() == 0


# ------------------------------------------------------------ RestClient against a local server
class Script:
    """Serve a scripted list of (status, headers, body | 'SLEEP')."""

    def __init__(self, steps):
        self.steps = list(steps)
        self.calls = 0

    async def handler(self, request):
        self.calls += 1
        status, headers, body = self.steps.pop(0)
        if body == "SLEEP":
            await asyncio.sleep(2)
            body = "{}"
        return web.Response(status=status, headers=headers, text=body, content_type="application/json")


async def make_client(tmp_path, script, clock, *, max_retries=3, timeout_s=10.0):
    app = web.Application()
    app.router.add_get("/kline", script.handler)
    server = TestServer(app)
    await server.start_server()
    session = aiohttp.ClientSession()
    writer = JsonlWriter(tmp_path, fsync=False)
    limiter = WindowLimiter(300, 60, clock=clock, sleep=clock.sleep)
    client = RestClient(session, venue="mudrex", base_url=str(server.make_url("")), latency=LatencyLog(writer),
                        limiter=limiter, timeout_s=timeout_s, max_retries=max_retries,
                        backoff_base_s=1.0, backoff_cap_s=60.0, sleep=clock.sleep)
    return client, server, session, writer


def latency_rows(tmp_path, writer):
    writer.flush()
    return [json.loads(l) for p in (tmp_path / "latency").rglob("*.jsonl") for l in p.read_text().splitlines()]


async def test_429_honors_retry_after(tmp_path, clock):
    body = fixture_text("mudrex_rest_kline.json")
    script = Script([(429, {"Retry-After": "7"}, '{"success":false}'), (200, {}, body)])
    client, server, session, writer = await make_client(tmp_path, script, clock)
    try:
        res = await client.get("kline", "/kline", {"assets": "BTC/USDT"})
    finally:
        await session.close(); await server.close()
    assert res.ok and res.text == body and res.attempts == 2
    assert 7 in clock.sleeps  # waited exactly Retry-After via the shared limiter
    rows = latency_rows(tmp_path, writer)
    assert [r["status"] for r in rows] == [429, 200]
    assert all(r["end_ns"] >= r["start_ns"] for r in rows)


async def test_429_without_retry_after_uses_backoff(tmp_path, clock):
    script = Script([(429, {}, "{}"), (200, {}, "{}")])
    client, server, session, writer = await make_client(tmp_path, script, clock)
    try:
        res = await client.get("kline", "/kline", {})
    finally:
        await session.close(); await server.close()
    assert res.ok
    assert len(clock.sleeps) == 1 and 0.5 <= clock.sleeps[0] <= 1.0


async def test_418_ip_ban_honors_retry_after(tmp_path, clock):
    script = Script([(418, {"Retry-After": "120"}, "{}"), (200, {}, "{}")])
    client, server, session, writer = await make_client(tmp_path, script, clock)
    try:
        res = await client.get("kline", "/kline", {})
    finally:
        await session.close(); await server.close()
    assert res.ok and 120 in clock.sleeps


async def test_400_not_retried(tmp_path, clock):
    script = Script([(400, {}, '{"success":false,"errors":[{"code":400,"text":"assets are required"}]}')])
    client, server, session, writer = await make_client(tmp_path, script, clock)
    try:
        res = await client.get("kline", "/kline", {})
    finally:
        await session.close(); await server.close()
    assert not res.ok and res.status == 400 and script.calls == 1 and clock.sleeps == []


async def test_5xx_retries_then_gives_up(tmp_path, clock):
    script = Script([(503, {}, "{}")] * 3)
    client, server, session, writer = await make_client(tmp_path, script, clock, max_retries=2)
    try:
        res = await client.get("kline", "/kline", {})
    finally:
        await session.close(); await server.close()
    assert not res.ok and res.status == 503 and res.attempts == 3 and script.calls == 3
    assert len(clock.sleeps) == 2


async def test_timeout_is_retried_and_logged(tmp_path, clock):
    script = Script([(200, {}, "SLEEP"), (200, {}, "{}")])
    client, server, session, writer = await make_client(tmp_path, script, clock, timeout_s=0.2)
    try:
        res = await client.get("kline", "/kline", {})
    finally:
        await session.close(); await server.close()
    assert res.ok and res.attempts == 2
    rows = latency_rows(tmp_path, writer)
    assert rows[0]["error"] == "timeout" and rows[0]["status"] is None
