"""REST client: rate-limited, timed, retried GETs against public endpoints only.

Every attempt is logged to the latency log (start/end ns). 429 and 418 honor
Retry-After (falling back to backoff) and pause the whole limiter, so other
requests to the same venue wait too.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp

from .backoff import Backoff
from .latency import LatencyLog
from .ratelimit import WindowLimiter, parse_retry_after
from .records import now_ns

log = logging.getLogger(__name__)

RETRY_STATUSES = {429, 418, 500, 502, 503, 504}


@dataclass
class RestResult:
    endpoint: str
    url: str
    params: dict[str, Any]
    status: int | None
    text: str | None
    start_ns: int
    end_ns: int
    attempts: int
    error: str | None = None
    used_weight: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == 200 and self.error is None


class RestClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        venue: str,
        base_url: str,
        latency: LatencyLog,
        limiter: WindowLimiter,
        timeout_s: float,
        max_retries: int,
        backoff_base_s: float,
        backoff_cap_s: float,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self.session = session
        self.venue = venue
        self.base_url = base_url.rstrip("/")
        self.latency = latency
        self.limiter = limiter
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)
        self.max_retries = max_retries
        self._backoff_args = (backoff_base_s, backoff_cap_s)
        self._sleep = sleep

    async def get(
        self,
        endpoint: str,
        path: str,
        params: dict[str, Any],
        *,
        weight: int = 1,
        extra_limiters: tuple[WindowLimiter, ...] = (),
    ) -> RestResult:
        url = f"{self.base_url}{path}"
        backoff = Backoff(*self._backoff_args)
        attempt = 0
        while True:
            attempt += 1
            await self.limiter.acquire(weight)
            for lim in extra_limiters:
                await lim.acquire(1)
            start = now_ns()
            status: int | None = None
            text: str | None = None
            error: str | None = None
            retry_after: float | None = None
            used_weight: str | None = None
            try:
                async with self.session.get(url, params=params, timeout=self.timeout) as resp:
                    status = resp.status
                    text = await resp.text()
                    used_weight = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                    retry_after = parse_retry_after(resp.headers.get("Retry-After"))
            except asyncio.TimeoutError:
                error = "timeout"
            except aiohttp.ClientError as e:
                error = f"{type(e).__name__}: {e}"
            end = now_ns()
            self.latency.rest(
                venue=self.venue, endpoint=endpoint, start_ns=start, end_ns=end,
                status=status, attempt=attempt, error=error, used_weight=used_weight,
            )
            result = RestResult(endpoint, url, params, status, text, start, end, attempt, error, used_weight)

            retryable = error is not None or status in RETRY_STATUSES
            if not retryable:
                if status != 200:
                    log.warning("%s %s -> HTTP %s: %.200s", self.venue, endpoint, status, text)
                return result
            delay = backoff.next_delay()
            if status in (429, 418):
                if retry_after is not None:
                    delay = retry_after
                # Pause everyone using this venue's limiter, not just this caller.
                self.limiter.block_for(delay)
                log.warning("%s %s -> HTTP %s, backing off %.1fs (Retry-After=%s)",
                            self.venue, endpoint, status, delay, retry_after)
            else:
                log.warning("%s %s attempt %d failed (%s), retrying in %.1fs",
                            self.venue, endpoint, attempt, error or f"HTTP {status}", delay)
            if attempt > self.max_retries:
                return result
            if status not in (429, 418):
                await self._sleep(delay)
            # For 429/418 the limiter block enforces the wait on the next acquire().

