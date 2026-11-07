"""Latency log: REST request start/end and WS connect/disconnect times.

Written to <data_dir>/latency/<venue>/<YYYY-MM-DD>.jsonl, separate from raw data.
"""

from __future__ import annotations

from typing import Any

from .records import SCHEMA_VERSION, now_ns
from .writer import JsonlWriter


class LatencyLog:
    def __init__(self, writer: JsonlWriter):
        self._w = writer

    def _write(self, venue: str, rec: dict[str, Any]) -> None:
        rec = {"v": SCHEMA_VERSION, "venue": venue, **rec}
        if "recv_ns" not in rec:
            rec["recv_ns"] = now_ns()
        self._w.write(("latency", venue), rec)

    def rest(
        self,
        *,
        venue: str,
        endpoint: str,
        start_ns: int,
        end_ns: int,
        status: int | None,
        attempt: int,
        error: str | None = None,
        used_weight: str | None = None,
    ) -> None:
        self._write(
            venue,
            {
                "event": "rest",
                "endpoint": endpoint,
                "start_ns": start_ns,
                "end_ns": end_ns,
                "recv_ns": end_ns,
                "duration_ms": round((end_ns - start_ns) / 1e6, 3),
                "status": status,
                "attempt": attempt,
                "error": error,
                "used_weight_1m": used_weight,
            },
        )

    def ws_connect(self, *, venue: str, conn: str, conn_id: int, start_ns: int, end_ns: int,
                   ok: bool, error: str | None = None) -> None:
        self._write(
            venue,
            {
                "event": "ws_connect",
                "conn": conn,
                "conn_id": conn_id,
                "start_ns": start_ns,
                "end_ns": end_ns,
                "recv_ns": end_ns,
                "duration_ms": round((end_ns - start_ns) / 1e6, 3),
                "ok": ok,
                "error": error,
            },
        )

    def ws_disconnect(self, *, venue: str, conn: str, conn_id: int, at_ns: int, reason: str,
                      connected_s: float, messages: int) -> None:
        self._write(
            venue,
            {
                "event": "ws_disconnect",
                "conn": conn,
                "conn_id": conn_id,
                "recv_ns": at_ns,
                "reason": reason,
                "connected_s": round(connected_s, 3),
                "messages": messages,
            },
        )
