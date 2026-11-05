"""Record builders. Every record carries venue, stream, receive time (ns, UTC),
exchange time (ms, when the venue provides one) and the raw payload text exactly
as received. Parsing into typed fields happens in a separate step, not here."""

from __future__ import annotations

import time
from typing import Any

SCHEMA_VERSION = 1


def now_ns() -> int:
    return time.time_ns()


def data_record(
    *,
    venue: str,
    stream: str,
    source: str,
    recv_ns: int,
    raw: str,
    exch_ts_ms: int | None,
    **extra: Any,
) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "v": SCHEMA_VERSION,
        "type": "data",
        "venue": venue,
        "stream": stream,
        "source": source,
        "recv_ns": recv_ns,
        "exch_ts_ms": exch_ts_ms,
    }
    rec.update(extra)
    rec["raw"] = raw
    return rec


def gap_record(
    *,
    venue: str,
    stream: str,
    recv_ns: int,
    gap_start_ns: int,
    gap_end_ns: int,
    reason: str,
    **extra: Any,
) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "v": SCHEMA_VERSION,
        "type": "gap",
        "venue": venue,
        "stream": stream,
        "recv_ns": recv_ns,
        "gap_start_ns": gap_start_ns,
        "gap_end_ns": gap_end_ns,
        "gap_ms": round((gap_end_ns - gap_start_ns) / 1e6, 3),
        "reason": reason,
    }
    rec.update(extra)
    return rec


def event_record(*, venue: str, stream: str, recv_ns: int, type: str, **extra: Any) -> dict[str, Any]:
    """Non-data markers: session_start, error."""
    rec: dict[str, Any] = {
        "v": SCHEMA_VERSION,
        "type": type,
        "venue": venue,
        "stream": stream,
        "recv_ns": recv_ns,
    }
    rec.update(extra)
    return rec
