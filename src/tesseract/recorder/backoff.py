"""Exponential backoff with jitter, capped (default cap 60s)."""

from __future__ import annotations

import random


class Backoff:
    def __init__(self, base_s: float = 1.0, cap_s: float = 60.0, rng: random.Random | None = None):
        if base_s <= 0 or cap_s <= 0:
            raise ValueError("base_s and cap_s must be > 0")
        self.base_s = base_s
        self.cap_s = cap_s
        self.attempt = 0
        self._rng = rng or random.Random()

    def ceiling(self, attempt: int) -> float:
        """Upper bound for the given attempt (0-based): min(cap, base * 2**attempt)."""
        # Clamp the exponent so huge attempt counts never overflow.
        return min(self.cap_s, self.base_s * (2 ** min(attempt, 32)))

    def next_delay(self) -> float:
        """Jitter: uniform in [ceiling/2, ceiling] so delays spread out but never collapse to ~0."""
        ceil = self.ceiling(self.attempt)
        self.attempt += 1
        return self._rng.uniform(ceil / 2, ceil)

    def reset(self) -> None:
        self.attempt = 0
