"""Generic WebSocket recorder connection.

One `WsConnection` = one socket with a fixed list of subscriptions. It:
- connects (respecting a per-venue connection-rate limiter), subscribes, and
  writes every received frame verbatim as a `data` record;
- sends protocol-level PING frames on a timer and reconnects if nothing at all
  arrives for `stale_timeout_s`;
- on reconnect, re-subscribes and writes a `gap` record to every stream it
  serves, covering last-message-before-drop -> resubscribed;
- reconnects with exponential backoff + jitter (cap 60s).
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp

from .backoff import Backoff
from .latency import LatencyLog
from .ratelimit import WindowLimiter
from .records import data_record, event_record, gap_record, now_ns
from .writer import JsonlWriter

log = logging.getLogger(__name__)

CONTROL_STREAM = "ws_control"


def stream_dir(stream: str) -> str:
    """Raw stream name -> directory name, e.g. 'kline@1m@btcusdt' -> 'ws_kline_1m_btcusdt'."""
    return "ws_" + re.sub(r"[^a-z0-9]+", "_", stream.lower()).strip("_")


class GapTracker:
    """Tracks the disconnect window for one connection. Pure logic, no I/O.

    gap_start = time of the last message received before the drop (anything
    after it may be missing). If no message was ever received on the previous
    connection, the time that connection went down is used. Consecutive failed
    reconnect attempts extend one gap; they do not create several.
    """

    def __init__(self) -> None:
        self.last_msg_ns: int | None = None
        self.down_since_ns: int | None = None
        self.has_connected = False
        self._gap_start_ns: int | None = None

    def on_message(self, ns: int) -> None:
        self.last_msg_ns = ns

    def on_disconnect(self, ns: int) -> None:
        if self._gap_start_ns is None:
            self._gap_start_ns = self.last_msg_ns if self.last_msg_ns is not None else ns
            self.down_since_ns = ns

    def on_connected(self, ns: int) -> tuple[int, int] | None:
        """Call after (re)subscribing. Returns (gap_start_ns, gap_end_ns) for a reconnect, else None."""
        first = not self.has_connected
        self.has_connected = True
        if first or self._gap_start_ns is None:
            self._gap_start_ns = None
            self.down_since_ns = None
            return None
        gap = (self._gap_start_ns, ns)
        self._gap_start_ns = None
        self.down_since_ns = None
        self.last_msg_ns = None
        return gap


class WsConnection:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        venue: str,
        name: str,
        url: str,
        streams: list[str],
        subscribe_messages: Callable[[], list[dict[str, Any]]],
        exch_ts: Callable[[Any], int | None],
        writer: JsonlWriter,
        latency: LatencyLog,
        connect_limiter: WindowLimiter,
        backoff: Backoff,
        ping_interval_s: float,
        stale_timeout_s: float,
        connect_timeout_s: float = 10.0,
        on_subscribed: Callable[[bool], Awaitable[None]] | None = None,
        on_frame: Callable[[str, Any, int], None] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self.session = session
        self.venue = venue
        self.name = name
        self.url = url
        self.streams = streams
        self.subscribe_messages = subscribe_messages
        self.exch_ts = exch_ts
        self.writer = writer
        self.latency = latency
        self.connect_limiter = connect_limiter
        self.backoff = backoff
        self.ping_interval_s = ping_interval_s
        self.stale_timeout_s = stale_timeout_s
        self.connect_timeout_s = connect_timeout_s
        # Called (as a task) after every successful subscribe; arg is True on reconnects.
        self.on_subscribed = on_subscribed
        # Called after each stream frame is written: (raw stream name, parsed JSON, recv_ns).
        self.on_frame = on_frame
        self._sleep = sleep
        self.gaps = GapTracker()
        self.conn_id = 0
        self.messages = 0
        self.reconnects = 0
        self._bg: set[asyncio.Task[None]] = set()

    # ------------------------------------------------------------ helpers
    def _write_stream_markers(self, make: Callable[[str], dict[str, Any]]) -> None:
        for s in self.streams:
            self.writer.write(("raw", self.venue, stream_dir(s)), make(s))

    def _handle_frame(self, msg: aiohttp.WSMessage) -> None:
        recv = now_ns()
        self.gaps.on_message(recv)
        self.messages += 1
        extra: dict[str, Any] = {"conn": self.name, "conn_id": self.conn_id}
        if msg.type == aiohttp.WSMsgType.TEXT:
            raw = msg.data
        else:  # binary: keep it, encoded, rather than drop it
            raw = ""
            extra["raw_b64"] = base64.b64encode(msg.data).decode()
        stream_key, exch = CONTROL_STREAM, None
        try:
            obj = json.loads(raw) if raw else None
        except ValueError:
            obj = None
        if isinstance(obj, dict) and isinstance(obj.get("stream"), str):
            stream_key = stream_dir(obj["stream"])
            try:
                exch = self.exch_ts(obj)
            except Exception:  # never drop a frame because ts extraction failed
                exch = None
        elif isinstance(obj, dict) and "error" in obj:
            log.error("%s/%s server error: %.300s", self.venue, self.name, raw)
        self.writer.write(
            ("raw", self.venue, stream_key),
            data_record(venue=self.venue, stream=stream_key, source="ws", recv_ns=recv,
                        raw=raw, exch_ts_ms=exch, **extra),
        )
        if self.on_frame is not None and stream_key != CONTROL_STREAM:
            try:
                self.on_frame(obj["stream"], obj, recv)
            except Exception:  # a validator bug must never stop recording
                log.exception("%s/%s on_frame hook failed", self.venue, self.name)

    async def _pinger(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while not ws.closed:
            await self._sleep(self.ping_interval_s)
            if not ws.closed:
                await ws.ping()

    # ------------------------------------------------------------ main loop
    async def run(self) -> None:
        while True:
            await self.connect_limiter.acquire(1)
            self.conn_id += 1
            start = now_ns()
            try:
                ws = await asyncio.wait_for(
                    self.session.ws_connect(self.url, autoping=True, max_msg_size=0),
                    timeout=self.connect_timeout_s,
                )
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                err = f"{type(e).__name__}: {e}"
                self.latency.ws_connect(venue=self.venue, conn=self.name, conn_id=self.conn_id,
                                        start_ns=start, end_ns=now_ns(), ok=False, error=err)
                self.gaps.on_disconnect(now_ns())
                delay = self.backoff.next_delay()
                log.warning("%s/%s connect failed (%s); retry in %.1fs", self.venue, self.name, err, delay)
                await self._sleep(delay)
                continue
            self.latency.ws_connect(venue=self.venue, conn=self.name, conn_id=self.conn_id,
                                    start_ns=start, end_ns=now_ns(), ok=True)
            reason = "unknown"
            connected_at = now_ns()
            msgs_before = self.messages
            pinger: asyncio.Task[None] | None = None
            try:
                for sub in self.subscribe_messages():
                    await ws.send_str(json.dumps(sub))
                subscribed_ns = now_ns()
                gap = self.gaps.on_connected(subscribed_ns)
                if gap is None:
                    self._write_stream_markers(lambda s: event_record(
                        venue=self.venue, stream=stream_dir(s), recv_ns=subscribed_ns,
                        type="session_start", conn=self.name, conn_id=self.conn_id, url=self.url))
                else:
                    self.reconnects += 1
                    log.warning("%s/%s reconnected; gap %.1fs", self.venue, self.name, (gap[1] - gap[0]) / 1e9)
                    self._write_stream_markers(lambda s: gap_record(
                        venue=self.venue, stream=stream_dir(s), recv_ns=subscribed_ns,
                        gap_start_ns=gap[0], gap_end_ns=gap[1], reason="reconnect",
                        conn=self.name, conn_id=self.conn_id))
                if self.on_subscribed is not None:
                    t = asyncio.create_task(self.on_subscribed(gap is not None))
                    self._bg.add(t)
                    t.add_done_callback(self._bg.discard)
                log.info("%s/%s connected (conn_id=%d) %s", self.venue, self.name, self.conn_id, self.url)
                pinger = asyncio.create_task(self._pinger(ws))
                while True:
                    try:
                        # Outer deadline on purpose: aiohttp's own receive timeout restarts on
                        # every control frame, so pongs would hide a data-dead socket.
                        async with asyncio.timeout(self.stale_timeout_s):
                            msg = await ws.receive()
                    except TimeoutError:
                        reason = f"stale: no message for {self.stale_timeout_s}s"
                        break
                    if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                        self._handle_frame(msg)
                        if self.messages - msgs_before == 1:
                            self.backoff.reset()  # healthy again
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                                      aiohttp.WSMsgType.CLOSED):
                        reason = f"closed by server (code={ws.close_code})"
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        reason = f"error: {ws.exception()}"
                        break
            except asyncio.CancelledError:
                reason = "shutdown"
                raise
            except (aiohttp.ClientError, ConnectionError, OSError) as e:
                reason = f"{type(e).__name__}: {e}"
            finally:
                down = now_ns()
                if pinger is not None:
                    pinger.cancel()
                if reason != "shutdown":
                    self.gaps.on_disconnect(down)
                self.latency.ws_disconnect(
                    venue=self.venue, conn=self.name, conn_id=self.conn_id, at_ns=down, reason=reason,
                    connected_s=(down - connected_at) / 1e9, messages=self.messages - msgs_before)
                try:
                    await asyncio.shield(ws.close())
                except Exception:
                    pass
                if reason != "shutdown":
                    log.warning("%s/%s disconnected: %s", self.venue, self.name, reason)
            await self._sleep(self.backoff.next_delay())
