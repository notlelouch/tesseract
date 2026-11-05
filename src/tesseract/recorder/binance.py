"""Binance USD-M futures public market data: REST pollers and WS streams.

Only public, unauthenticated market-data endpoints. No API key header is ever sent.
"""

from __future__ import annotations

import json
from typing import Any

import aiohttp

from ..config import BackoffConfig, BinanceConfig
from .backoff import Backoff
from .http import RestClient, RestResult
from .latency import LatencyLog
from .ratelimit import WindowLimiter
from .records import data_record, event_record
from .ws import WsConnection
from .writer import JsonlWriter

VENUE = "binance"


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
            on_subscribed=self._on_public_subscribed, **common)
        self.ws_market = WsConnection(
            session, venue=VENUE, name="market", url=cfg.ws_market_url, streams=cfg.ws_market_streams,
            subscribe_messages=lambda: subscribe(cfg.ws_market_streams, 2),
            backoff=Backoff(backoff.base_s, backoff.cap_s), **common)

    async def _on_public_subscribed(self, is_reconnect: bool) -> None:
        # The diff stream has a hole after a reconnect: take a fresh snapshot right away.
        if is_reconnect:
            await self.snapshot_depth()

    def _w(self, name: str) -> int:
        return int(self.cfg.weights[name])

    async def snapshot_depth(self) -> None:
        res = await self.rest.get("depth", "/fapi/v1/depth",
                                  {"symbol": self.cfg.symbol, "limit": self.cfg.depth_limit},
                                  weight=self._w("depth"))
        write_rest(self.writer, "rest_depth", res)

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
