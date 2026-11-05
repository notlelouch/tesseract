"""Recorder entrypoint: `tesseract-record --config config.toml [--duration SECONDS]`.

Wires config -> writer, latency log, venue clients and the scheduler. On SIGINT
or SIGTERM it cancels all tasks, flushes and closes files, then closes sockets.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from collections.abc import Awaitable, Callable

import aiohttp

from ..config import Config, load_config
from .binance import Binance
from .latency import LatencyLog
from .mudrex import Mudrex
from .scheduler import every
from .writer import JsonlWriter, WriterOverflow

log = logging.getLogger("tesseract")


async def _flush_loop(writer: JsonlWriter, interval_s: float) -> None:
    while True:
        await asyncio.sleep(interval_s)
        writer.flush()


async def _status_loop(writer: JsonlWriter, conns: list, interval_s: float = 300) -> None:
    while True:
        await asyncio.sleep(interval_s)
        parts = [f"{c.venue}/{c.name}: msgs={c.messages} reconnects={c.reconnects}" for c in conns]
        log.info("status: written=%d pending=%d flush_errors=%d | %s",
                 writer.records_written, writer.pending, writer.flush_errors, "; ".join(parts))


async def _wall_clock_deadline(duration_s: float) -> None:
    """Sleep until wall-clock deadline. The monotonic clock asyncio uses stops
    while macOS sleeps, so a plain asyncio.sleep(duration) would overrun."""
    deadline = time.time() + duration_s
    while (left := deadline - time.time()) > 0:
        await asyncio.sleep(min(left, 5.0))


async def run(cfg: Config, duration_s: float | None = None) -> int:
    g = cfg.general
    writer = JsonlWriter(g.data_dir, fsync=g.fsync, max_buffer_records=g.max_buffer_records)
    latency = LatencyLog(writer)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: (log.info("received %s, shutting down", s.name), stop.set()))

    headers = {"User-Agent": g.user_agent}
    exit_code = 0
    async with aiohttp.ClientSession(headers=headers) as session:
        jobs: list[Awaitable[None]] = [_flush_loop(writer, g.flush_interval_s)]
        conns = []

        def sched(name: str, interval: float, fn: Callable[[], Awaitable[object]]) -> None:
            jobs.append(every(name, interval, fn))

        if cfg.mudrex.enabled:
            m = Mudrex(cfg.mudrex, cfg.backoff, session, writer, latency,
                       timeout_s=g.http_timeout_s, max_retries=g.http_max_retries)
            conns.append(m.ws)
            jobs.append(m.ws.run())
            sched("mudrex.klines", cfg.mudrex.rest_poll_interval_s, m.poll_klines)
        if cfg.binance.enabled:
            b = Binance(cfg.binance, cfg.backoff, session, writer, latency,
                        timeout_s=g.http_timeout_s, max_retries=g.http_max_retries)
            conns += [b.ws_public, b.ws_market]
            jobs += [b.ws_public.run(), b.ws_market.run()]
            sched("binance.depth", cfg.binance.depth_snapshot_interval_s, b.snapshot_depth)
            sched("binance.premiumIndex", cfg.binance.premium_index_interval_s, b.poll_premium_index)
            sched("binance.fundingRate", cfg.binance.funding_rate_interval_s, b.poll_funding_rate)
            sched("binance.klines", cfg.binance.klines_poll_interval_s, b.poll_klines)
        jobs.append(_status_loop(writer, conns))

        tasks = [asyncio.create_task(j) for j in jobs]
        stopper = asyncio.create_task(stop.wait())
        waiters = {stopper, *tasks}
        timer = None
        if duration_s:
            timer = asyncio.create_task(_wall_clock_deadline(duration_s))
            waiters.add(timer)
        log.info("recorder started: mudrex=%s binance=%s data_dir=%s",
                 cfg.mudrex.enabled, cfg.binance.enabled, g.data_dir)

        done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            if t in tasks and t.exception() is not None:
                exc = t.exception()
                log.error("task crashed: %r", exc, exc_info=exc)
                exit_code = 2 if isinstance(exc, WriterOverflow) else 1
        if timer is not None and timer in done:
            log.info("duration %.0fs reached, shutting down", duration_s)

        for t in [*tasks, stopper, *([timer] if timer else [])]:
            t.cancel()
        await asyncio.gather(*tasks, stopper, *([timer] if timer else []), return_exceptions=True)
        # Sockets are closed by the cancelled WS tasks; the session closes on exit.

    if not writer.close():
        log.error("shutdown: some records could not be persisted (pending=%d)", writer.pending)
        exit_code = exit_code or 3
    log.info("shutdown complete: %d records written", writer.records_written)
    return exit_code


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Tesseract v1 market data recorder")
    p.add_argument("--config", default="config.toml")
    p.add_argument("--duration", type=float, default=None, help="stop after N seconds (dev runs)")
    args = p.parse_args(argv)
    cfg = load_config(args.config)
    logging.basicConfig(
        level=getattr(logging, cfg.general.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.Formatter.converter = time.gmtime
    sys.exit(asyncio.run(run(cfg, args.duration)))


if __name__ == "__main__":
    main()
