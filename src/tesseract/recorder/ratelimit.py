"""Rate limiting.

- `WindowLimiter`: sliding-window budget of `limit` units per `window_s`
  (units = requests for Mudrex, request weight for Binance, connections for WS).
- `block_until()`: a server-imposed pause (HTTP 429/418 Retry-After) that holds
  every caller of the limiter, not just the one that got the 429.
- `parse_retry_after()`: Retry-After as delta-seconds or HTTP-date.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from email.utils import parsedate_to_datetime


def parse_retry_after(value: str | None, *, now: float | None = None) -> float | None:
    """Return seconds to wait, or None if absent/unparseable."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        secs = float(value)
        return max(0.0, secs)
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if dt is None:
        return None
    now = time.time() if now is None else now
    return max(0.0, dt.timestamp() - now)


class WindowLimiter:
    def __init__(
        self,
        limit: int,
        window_s: float,
        *,
        name: str = "",
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        if limit <= 0 or window_s <= 0:
            raise ValueError("limit and window_s must be > 0")
        self.limit = limit
        self.window_s = window_s
        self.name = name
        self._clock = clock
        self._sleep = sleep
        self._events: deque[tuple[float, int]] = deque()
        self._used = 0
        self._blocked_until = 0.0
        self._lock = asyncio.Lock()

    def _expire(self, now: float) -> None:
        while self._events and now - self._events[0][0] >= self.window_s:
            _, w = self._events.popleft()
            self._used -= w

    def block_until(self, monotonic_deadline: float) -> None:
        self._blocked_until = max(self._blocked_until, monotonic_deadline)

    def block_for(self, seconds: float) -> None:
        self.block_until(self._clock() + seconds)

    @property
    def used(self) -> int:
        self._expire(self._clock())
        return self._used

    def wait_time(self, weight: int = 1) -> float:
        """Seconds until `weight` units can be spent (0 if now)."""
        if weight > self.limit:
            raise ValueError(f"weight {weight} exceeds limiter capacity {self.limit}")
        now = self._clock()
        self._expire(now)
        wait = max(0.0, self._blocked_until - now)
        if self._used + weight > self.limit:
            # Find when enough old events expire.
            need = self._used + weight - self.limit
            freed = 0
            for ts, w in self._events:
                freed += w
                if freed >= need:
                    wait = max(wait, ts + self.window_s - now)
                    break
        return wait

    async def acquire(self, weight: int = 1) -> float:
        """Wait until allowed, then record the spend. Returns total seconds waited."""
        waited = 0.0
        async with self._lock:
            while True:
                w = self.wait_time(weight)
                if w <= 0:
                    break
                await self._sleep(w)
                waited += w
            self._events.append((self._clock(), weight))
            self._used += weight
        return waited
