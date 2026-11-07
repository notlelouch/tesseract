"""Closed Mudrex candles from typed REST kline batches, and the reference price for a decision.

Source: typed `rest_kline` records (`kind: kline_batch`). Each poll returns the last few candles,
including the one still forming, so:
  - a candle is used only if the batch reporting it was received at or after its close time
    (open_time + interval);
  - when several batches report the same candle, the latest-received closed version wins.
WS kline updates are not used: a WS update cannot tell whether it is the candle's last one.

The reference price of a decision at time t is the close of the candle that closed most recently
at or before t. If that candle is missing (the series has a hole or ends before t) there is no
reference and NoReference is raised; an older close is never silently used instead.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

_UNIT_MS = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}


class NoReference(LookupError):
    pass


def interval_ms(interval: str) -> int:
    """'1m' -> 60000. Same notation as the recorder's kline_interval."""
    n, unit = interval[:-1], interval[-1:]
    if unit not in _UNIT_MS or not n.isdigit() or int(n) <= 0:
        raise ValueError(f"unsupported interval {interval!r}")
    return int(n) * _UNIT_MS[unit]


@dataclass(frozen=True)
class Candle:
    open_time_ms: int
    close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


class KlineSeries:
    def __init__(self, candles: Iterable[Candle], interval_ms: int):
        self.interval_ms = interval_ms
        self.candles = sorted(candles, key=lambda c: c.open_time_ms)
        self._closes = [c.close_time_ms for c in self.candles]

    @classmethod
    def from_typed(cls, records: Iterable[dict[str, Any]], interval_ms: int) -> "KlineSeries":
        best: dict[int, tuple[int, Candle]] = {}  # open_time -> (batch recv_ns, candle)
        for r in records:
            if r.get("type") != "data" or r.get("kind") != "kline_batch":
                continue
            recv_ms = r["recv_ns"] // 1_000_000
            for c in r["candles"]:
                close_ms = c["open_time_ms"] + interval_ms
                if recv_ms < close_ms:
                    continue  # still forming when this batch was received
                prev = best.get(c["open_time_ms"])
                if prev is None or r["recv_ns"] >= prev[0]:
                    best[c["open_time_ms"]] = (r["recv_ns"], Candle(
                        c["open_time_ms"], close_ms, Decimal(c["open"]), Decimal(c["high"]),
                        Decimal(c["low"]), Decimal(c["close"])))
        return cls((c for _, c in best.values()), interval_ms)

    def reference_at(self, t_ms: int) -> Candle:
        """The candle that closed most recently at or before t_ms. NoReference if it is missing."""
        i = bisect.bisect_right(self._closes, t_ms) - 1
        if i < 0 or self._closes[i] <= t_ms - self.interval_ms:
            raise NoReference(f"no closed candle in ({t_ms - self.interval_ms}, {t_ms}] ms")
        return self.candles[i]
