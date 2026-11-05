"""Binance USD-M futures public market data: REST pollers and WS streams.

Only public, unauthenticated market-data endpoints. No API key header is ever sent.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import aiohttp

from ..config import BackoffConfig, BinanceConfig
from .backoff import Backoff
from .depth_sync import Break, DepthSync, Diff, NeedSnapshot, Synced
from .http import RestClient, RestResult
from .latency import LatencyLog
from .ratelimit import WindowLimiter
from .records import data_record, event_record, gap_record, now_ns
from .ws import WsConnection, stream_dir
from .writer import JsonlWriter

VENUE = "binance"

log = logging.getLogger(__name__)


def ws_exch_ts(obj: Any) -> int | None:
    """Combined-stream envelope {"stream":..,"data":{"E":<ms>,..}} -> event time ms."""
    data = obj.get("data") if isinstance(obj, dict) else None
    e = data.get("E") if isinstance(data, dict) else None
    return int(e) if isinstance(e, (int, float)) else None


def rest_exch_ts(endpoint: str, text: str | None) -> int | None:
    """depth: E (message output time); premiumIndex: time. Others carry none."""
    if not text:
        return None
    try:
        obj = json.loads(text)
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    key = {"depth": "E", "premiumIndex": "time"}.get(endpoint)
    v = obj.get(key) if key else None
    return int(v) if isinstance(v, (int, float)) else None


def write_rest(writer: JsonlWriter, stream: str, res: RestResult) -> None:
    common = dict(venue=VENUE, stream=stream, recv_ns=res.end_ns)
    req = {"url": res.url, "params": res.params}
    if res.ok:
        writer.write(("raw", VENUE, stream), data_record(
            source="rest", raw=res.text or "", exch_ts_ms=rest_exch_ts(res.endpoint, res.text),
            req_start_ns=res.start_ns, request=req, status=res.status,
            used_weight_1m=res.used_weight, **common))
    else:
        writer.write(("raw", VENUE, stream), event_record(
            type="error", source="rest", req_start_ns=res.start_ns, request=req, status=res.status,
            error=res.error, attempts=res.attempts, raw=res.text, **common))


def subscribe(streams: list[str], req_id: int) -> list[dict[str, Any]]:
    return [{"method": "SUBSCRIBE", "params": list(streams), "id": req_id}]


class Binance:
    def __init__(self, cfg: BinanceConfig, backoff: BackoffConfig, session: aiohttp.ClientSession,
                 writer: JsonlWriter, latency: LatencyLog, *, timeout_s: float, max_retries: int):
        self.cfg = cfg
        self.writer = writer
        # Diff-depth sequence check (Binance USD-M local-book procedure). Raw frames are
        # written as received regardless; this only adds depth_synced / gap verdicts.
        prefix = f"{cfg.symbol.lower()}@depth"
        self.depth_stream = next((s for s in cfg.ws_public_streams if s.lower().startswith(prefix)), None)
        self.depth_sync = DepthSync()
        self._sync_epoch_ns: int | None = None  # set when the depth WS (re)subscribes
        self._resnap_task: asyncio.Task[None] | None = None
        self.funding_limiter = WindowLimiter(cfg.funding_rate_ip_limit, cfg.funding_rate_ip_window_s,
                                             name="binance-fundingRate")
        self.rest = RestClient(
            session, venue=VENUE, base_url=cfg.rest_base, latency=latency,
            limiter=WindowLimiter(cfg.weight_limit, cfg.weight_window_s, name="binance-weight"),
            timeout_s=timeout_s, max_retries=max_retries,
            backoff_base_s=backoff.base_s, backoff_cap_s=backoff.cap_s)
        connect_limiter = WindowLimiter(cfg.ws_connect_limit, cfg.ws_connect_window_s, name="binance-ws")
        common = dict(writer=writer, latency=latency, connect_limiter=connect_limiter,
                      ping_interval_s=cfg.ws_ping_interval_s, stale_timeout_s=cfg.ws_stale_timeout_s,
                      connect_timeout_s=timeout_s, exch_ts=ws_exch_ts)
        self.ws_public = WsConnection(
            session, venue=VENUE, name="public", url=cfg.ws_public_url, streams=cfg.ws_public_streams,
            subscribe_messages=lambda: subscribe(cfg.ws_public_streams, 1),
            backoff=Backoff(backoff.base_s, backoff.cap_s),
            on_subscribed=self._on_public_subscribed, on_frame=self._on_public_frame, **common)
        self.ws_market = WsConnection(
            session, venue=VENUE, name="market", url=cfg.ws_market_url, streams=cfg.ws_market_streams,
            subscribe_messages=lambda: subscribe(cfg.ws_market_streams, 2),
            backoff=Backoff(backoff.base_s, backoff.cap_s), **common)

    # ------------------------------------------------------------ depth sync
    async def _on_public_subscribed(self, is_reconnect: bool) -> None:
        # First connect and every reconnect: diffs are buffered (as ids) from now on, and a
        # snapshot taken right away lets the recorded stream rebuild the book from the start.
        self._sync_epoch_ns = now_ns()
        self.depth_sync.reset()
        await self.snapshot_depth()

    def _on_public_frame(self, stream: str, obj: Any, recv_ns: int) -> None:
        if stream != self.depth_stream:
            return
        d = obj["data"]
        self._handle_sync(self.depth_sync.on_diff(Diff(int(d["U"]), int(d["u"]), int(d["pu"]), recv_ns)))

    def _handle_sync(self, events: list) -> None:
        if self.depth_stream is None:
            return
        name = stream_dir(self.depth_stream)
        key = ("raw", VENUE, name)
        for e in events:
            if isinstance(e, Synced):
                self.writer.write(key, event_record(
                    type="depth_synced", venue=VENUE, stream=name, recv_ns=now_ns(),
                    last_update_id=e.last_update_id, snapshot_recv_ns=e.snapshot_recv_ns,
                    first_U=e.first.U, first_u=e.first.u, first_recv_ns=e.first.recv_ns,
                    applied=e.applied, dropped=e.dropped))
                log.info("binance depth synced at lastUpdateId=%d (dropped %d buffered)",
                         e.last_update_id, e.dropped)
            elif isinstance(e, Break):
                self.writer.write(key, gap_record(
                    venue=VENUE, stream=name, recv_ns=now_ns(), gap_start_ns=e.gap_start_ns,
                    gap_end_ns=e.gap_end_ns, reason=e.kind, **e.detail))
                log.warning("binance depth %s: %s", e.kind, e.detail)
            elif isinstance(e, NeedSnapshot):
                self._request_snapshot()

    def _request_snapshot(self) -> None:
        if self._resnap_task is not None and not self._resnap_task.done():
            return
        self._resnap_task = asyncio.create_task(self._resnapshot())

    async def _resnapshot(self) -> None:
        await asyncio.sleep(self.cfg.depth_resync_delay_s)
        self._resnap_task = None  # allow the next failure to request another
        await self.snapshot_depth()

    def _w(self, name: str) -> int:
        return int(self.cfg.weights[name])

    async def snapshot_depth(self) -> None:
        res = await self.rest.get("depth", "/fapi/v1/depth",
                                  {"symbol": self.cfg.symbol, "limit": self.cfg.depth_limit},
                                  weight=self._w("depth"))
        write_rest(self.writer, "rest_depth", res)
        # Feed only snapshots requested after the current subscription; an older one
        # (e.g. the scheduled poll at startup) could predate the buffered diffs.
        if not res.ok or self._sync_epoch_ns is None or res.start_ns < self._sync_epoch_ns:
            return
        try:
            last_update_id = int(json.loads(res.text or "")["lastUpdateId"])
        except (ValueError, KeyError, TypeError):
            log.error("binance depth snapshot without a usable lastUpdateId")
            return
        self._handle_sync(self.depth_sync.on_snapshot(last_update_id, res.end_ns))

    async def poll_premium_index(self) -> None:
        res = await self.rest.get("premiumIndex", "/fapi/v1/premiumIndex", {"symbol": self.cfg.symbol},
                                  weight=self._w("premiumIndex"))
        write_rest(self.writer, "rest_premium_index", res)

    async def poll_funding_rate(self) -> None:
        res = await self.rest.get("fundingRate", "/fapi/v1/fundingRate",
                                  {"symbol": self.cfg.symbol, "limit": self.cfg.funding_rate_limit},
                                  weight=self._w("fundingRate"), extra_limiters=(self.funding_limiter,))
        write_rest(self.writer, "rest_funding_rate", res)

    async def poll_klines(self) -> None:
        res = await self.rest.get("klines", "/fapi/v1/klines",
                                  {"symbol": self.cfg.symbol, "interval": self.cfg.klines_interval,
                                   "limit": self.cfg.klines_limit},
                                  weight=self._w("klines"))
        write_rest(self.writer, "rest_klines", res)
