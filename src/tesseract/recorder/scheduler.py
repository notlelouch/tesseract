"""Minimal periodic scheduler: fixed-rate ticks on the monotonic clock.

A job that raises is logged and retried on its next tick; it never kills the
scheduler. A job that overruns its interval skips missed ticks instead of
bursting (which would break rate limits).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

log = logging.getLogger(__name__)


async def every(
    name: str,
    interval_s: float,
    job: Callable[[], Awaitable[object]],
    *,
    run_immediately: bool = True,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    next_at = clock() if run_immediately else clock() + interval_s
    while True:
        delay = next_at - clock()
        if delay > 0:
            await sleep(delay)
        try:
            await job()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("scheduled job %s failed; will retry next tick", name)
        next_at += interval_s
        now = clock()
        if next_at < now:  # overran: skip missed ticks
            skipped = int((now - next_at) // interval_s) + 1
            next_at += skipped * interval_s
