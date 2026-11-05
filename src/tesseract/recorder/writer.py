"""Append-only JSONL writer.

Layout: <root>/<key parts...>/<YYYY-MM-DD>.jsonl, date taken from the record's
`recv_ns` (UTC). Records are buffered in memory and written on `flush()`, which
the app calls on a timer and on shutdown.

Failure policy (never lose data silently):
- If a write fails, the records stay in the buffer and the error is logged.
- If the buffer grows past `max_buffer_records`, `write()` raises
  `WriterOverflow`, which stops the recorder loudly.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any

log = logging.getLogger(__name__)


class WriterOverflow(RuntimeError):
    pass


def utc_date(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")


def encode(record: dict[str, Any]) -> str:
    return json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"


class JsonlWriter:
    def __init__(self, root: str | Path, *, fsync: bool = True, max_buffer_records: int = 500_000):
        self.root = Path(root)
        self.fsync = fsync
        self.max_buffer_records = max_buffer_records
        self._buffers: dict[Path, list[str]] = {}
        self._handles: dict[Path, IO[str]] = {}
        self._pending = 0
        self.records_written = 0
        self.flush_errors = 0
        self._closed = False

    def path_for(self, key: tuple[str, ...], recv_ns: int) -> Path:
        return self.root.joinpath(*key, f"{utc_date(recv_ns)}.jsonl")

    def write(self, key: tuple[str, ...], record: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("writer is closed")
        if self._pending >= self.max_buffer_records:
            raise WriterOverflow(
                f"{self._pending} records buffered and disk writes are failing; refusing to drop data"
            )
        path = self.path_for(key, record["recv_ns"])
        self._buffers.setdefault(path, []).append(encode(record))
        self._pending += 1

    @property
    def pending(self) -> int:
        return self._pending

    def _open(self, path: Path) -> IO[str]:
        fh = self._handles.get(path)
        if fh is not None:
            return fh
        path.parent.mkdir(parents=True, exist_ok=True)
        # Repair a torn last line (e.g. after a crash) so the next record starts on its own line.
        needs_newline = False
        if path.exists() and path.stat().st_size > 0:
            with open(path, "rb") as rf:
                rf.seek(-1, os.SEEK_END)
                needs_newline = rf.read(1) != b"\n"
        fh = open(path, "a", encoding="utf-8")
        if needs_newline:
            log.warning("repairing torn last line in %s", path)
            fh.write("\n")
        self._handles[path] = fh
        return fh

    def flush(self) -> bool:
        """Write all buffered records. Returns True if everything was persisted."""
        ok = True
        for path in list(self._buffers):
            lines = self._buffers[path]
            if not lines:
                continue
            try:
                fh = self._open(path)
                fh.write("".join(lines))
                fh.flush()
                if self.fsync:
                    os.fsync(fh.fileno())
            except OSError as e:
                ok = False
                self.flush_errors += 1
                log.error("flush failed for %s (%d records kept in buffer): %s", path, len(lines), e)
                # Drop the handle; a partially written line is repaired on reopen.
                fh = self._handles.pop(path, None)
                if fh is not None:
                    try:
                        fh.close()
                    except OSError:
                        pass
                continue
            self.records_written += len(lines)
            self._pending -= len(lines)
            del self._buffers[path]
        self._close_stale_handles()
        return ok

    def _close_stale_handles(self) -> None:
        # Keep at most handles for paths written in this flush cycle or today's files.
        if len(self._handles) <= 64:
            return
        for path in list(self._handles):
            if path not in self._buffers:
                self._handles.pop(path).close()

    def close(self) -> bool:
        ok = self.flush()
        for fh in self._handles.values():
            try:
                fh.close()
            except OSError as e:
                ok = False
                log.error("close failed: %s", e)
        self._handles.clear()
        self._closed = True
        if self._pending:
            log.error("writer closed with %d unpersisted records", self._pending)
        return ok and self._pending == 0
