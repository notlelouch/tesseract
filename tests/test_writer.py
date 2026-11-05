import json
import os

import pytest

from tesseract.recorder.records import data_record, gap_record
from tesseract.recorder.writer import JsonlWriter, WriterOverflow, utc_date

from conftest import fixture_lines

# 2026-01-01T23:59:59.5Z and 2026-01-02T00:00:00.5Z
NS_DAY1 = 1767311999_500_000_000
NS_DAY2 = 1767312000_500_000_000


def rec(ns: int, raw: str = "{}", stream: str = "ws_ticker_1s") -> dict:
    return data_record(venue="mudrex", stream=stream, source="ws", recv_ns=ns, raw=raw, exch_ts_ms=None)


def read(path):
    return [json.loads(l) for l in path.read_text().splitlines()]


def test_utc_date():
    assert utc_date(NS_DAY1) == "2026-01-01"
    assert utc_date(NS_DAY2) == "2026-01-02"


def test_layout_and_raw_payload_preserved(tmp_path):
    w = JsonlWriter(tmp_path, fsync=False)
    frames = fixture_lines("mudrex_ws_frames.jsonl")
    for i, f in enumerate(frames):
        w.write(("raw", "mudrex", "ws_ticker_1s"), rec(NS_DAY1 + i, f))
    assert w.flush()
    path = tmp_path / "raw" / "mudrex" / "ws_ticker_1s" / "2026-01-01.jsonl"
    rows = read(path)
    assert [r["raw"] for r in rows] == frames  # byte-for-byte payload text
    assert all({"venue", "stream", "recv_ns", "exch_ts_ms", "raw"} <= r.keys() for r in rows)


def test_buffered_until_flush(tmp_path):
    w = JsonlWriter(tmp_path, fsync=False)
    w.write(("raw", "mudrex", "s"), rec(NS_DAY1))
    path = tmp_path / "raw" / "mudrex" / "s" / "2026-01-01.jsonl"
    assert not path.exists()
    assert w.pending == 1
    w.flush()
    assert len(read(path)) == 1 and w.pending == 0


def test_utc_date_rollover_splits_files(tmp_path):
    w = JsonlWriter(tmp_path, fsync=False)
    w.write(("raw", "binance", "s"), rec(NS_DAY1))
    w.write(("raw", "binance", "s"), rec(NS_DAY2))
    w.close()
    d = tmp_path / "raw" / "binance" / "s"
    assert sorted(p.name for p in d.iterdir()) == ["2026-01-01.jsonl", "2026-01-02.jsonl"]


def test_append_only_across_writer_instances(tmp_path):
    for i in range(2):
        w = JsonlWriter(tmp_path, fsync=False)
        w.write(("raw", "m", "s"), rec(NS_DAY1 + i))
        w.close()
    rows = read(tmp_path / "raw" / "m" / "s" / "2026-01-01.jsonl")
    assert [r["recv_ns"] for r in rows] == [NS_DAY1, NS_DAY1 + 1]


def test_torn_last_line_is_repaired(tmp_path):
    path = tmp_path / "raw" / "m" / "s" / "2026-01-01.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text('{"ok":1}\n{"torn":')  # crash mid-line
    w = JsonlWriter(tmp_path, fsync=False)
    w.write(("raw", "m", "s"), rec(NS_DAY1))
    w.close()
    lines = path.read_text().splitlines()
    assert lines[1] == '{"torn":'
    assert json.loads(lines[2])["recv_ns"] == NS_DAY1


def test_failed_flush_keeps_records_then_succeeds(tmp_path):
    w = JsonlWriter(tmp_path, fsync=False)
    blocker = tmp_path / "raw"
    blocker.write_text("not a dir")  # makes mkdir fail
    w.write(("raw", "m", "s"), rec(NS_DAY1))
    assert w.flush() is False
    assert w.pending == 1 and w.flush_errors == 1
    blocker.unlink()
    assert w.flush() is True
    assert w.pending == 0
    assert len(read(tmp_path / "raw" / "m" / "s" / "2026-01-01.jsonl")) == 1


def test_overflow_raises_instead_of_dropping(tmp_path):
    w = JsonlWriter(tmp_path, fsync=False, max_buffer_records=3)
    for i in range(3):
        w.write(("raw", "m", "s"), rec(NS_DAY1 + i))
    with pytest.raises(WriterOverflow):
        w.write(("raw", "m", "s"), rec(NS_DAY1 + 3))
    assert w.pending == 3


def test_close_flushes_and_reports(tmp_path):
    w = JsonlWriter(tmp_path, fsync=True)
    w.write(("raw", "m", "s"), gap_record(venue="m", stream="s", recv_ns=NS_DAY1,
                                          gap_start_ns=NS_DAY1 - 10**9, gap_end_ns=NS_DAY1, reason="reconnect"))
    assert w.close() is True
    rows = read(tmp_path / "raw" / "m" / "s" / "2026-01-01.jsonl")
    assert rows[0]["type"] == "gap" and rows[0]["gap_ms"] == 1000.0
    with pytest.raises(RuntimeError):
        w.write(("raw", "m", "s"), rec(NS_DAY1))
