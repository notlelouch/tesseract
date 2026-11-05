"""Mudrex public market data: REST kline / mark-kline polling and the linear WS.

Docs: https://docs.trade.mudrex.com/docs/market-data.md
      https://docs.trade.mudrex.com/docs/websocket-streams.md
No authentication is used or supported here.
"""

from __future__ import annotations

import time
from typing import Any

import aiohttp

from ..config import BackoffConfig, MudrexConfig
from .backoff import Backoff
from .http import RestClient, RestResult
from .latency import LatencyLog
from .ratelimit import WindowLimiter
from .records import data_record, event_record
from .ws import WsConnection
from .writer import JsonlWriter

VENUE = "mudrex"


def ws_exch_ts(obj: Any) -> int | None:
    """Mudrex WS frames carry no event timestamp. Kline `t` is the candle open
    time, not a send time, so it is left in `raw` and not reported here."""
    return None


def subscribe_messages(cfg: MudrexConfig) -> list[dict[str, Any]]:
    kline_streams = [s for s in cfg.ws_streams() if not s.startswith("ticker@")]
    msgs: list[dict[str, Any]] = []
    if kline_streams:
        msgs.append({"id": 1, "method": "SUBSCRIBE", "params": kline_streams})
    msgs.append({"id": 2, "method": "SUBSCRIBE", "params": [cfg.ticker_stream], "assets": list(cfg.ws_symbols)})
    return msgs


def write_rest(writer: JsonlWriter, stream: str, res: RestResult) -> None:
    common = dict(venue=VENUE, stream=stream, recv_ns=res.end_ns)
    req = {"url": res.url, "params": res.params}
    if res.ok:
        writer.write(("raw", VENUE, stream), data_record(
            source="rest", raw=res.text or "", exch_ts_ms=None,
            req_start_ns=res.start_ns, request=req, status=res.status, **common))
    else:
        writer.write(("raw", VENUE, stream), event_record(
            type="error", source="rest", req_start_ns=res.start_ns, request=req, status=res.status,
            error=res.error, attempts=res.attempts, raw=res.text, **common))


class Mudrex:
    def __init__(self, cfg: MudrexConfig, backoff: BackoffConfig, session: aiohttp.ClientSession,
                 writer: JsonlWriter, latency: LatencyLog, *, timeout_s: float, max_retries: int):
        self.cfg = cfg
        self.writer = writer
        self.rest = RestClient(
            session, venue=VENUE, base_url=cfg.rest_base, latency=latency,
            limiter=WindowLimiter(cfg.rest_rate_limit, cfg.rest_rate_window_s, name="mudrex-rest"),
            timeout_s=timeout_s, max_retries=max_retries,
            backoff_base_s=backoff.base_s, backoff_cap_s=backoff.cap_s)
        self.ws = WsConnection(
            session, venue=VENUE, name="linear", url=cfg.ws_url, streams=cfg.ws_streams(),
            subscribe_messages=lambda: subscribe_messages(cfg), exch_ts=ws_exch_ts,
            writer=writer, latency=latency,
            connect_limiter=WindowLimiter(cfg.ws_connect_limit, cfg.ws_connect_window_s, name="mudrex-ws"),
            backoff=Backoff(backoff.base_s, backoff.cap_s),
            ping_interval_s=cfg.ws_ping_interval_s, stale_timeout_s=cfg.ws_stale_timeout_s,
            connect_timeout_s=timeout_s)

    def _window(self) -> dict[str, Any]:
        end = int(time.time()) // 60 * 60 - 60  # open time of the last closed 1m candle
        return {
            "assets": ",".join(self.cfg.rest_assets),
            "aggregation": self.cfg.kline_interval if self.cfg.kline_interval != "1s" else "1m",
            "start_time": end - self.cfg.rest_lookback_s,
            "end_time": end,
        }

    async def poll_klines(self) -> None:
        params = self._window()
        res = await self.rest.get("kline", "/kline", params)
        write_rest(self.writer, "rest_kline", res)
        res = await self.rest.get("mark-kline", "/mark-kline", params)
        write_rest(self.writer, "rest_mark_kline", res)
