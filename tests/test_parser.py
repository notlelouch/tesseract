"""Raw -> typed parser. Every raw record here is either a recorded fixture or written by the
recorder's own code from recorded fixture payloads:

- Mudrex WS: tests/fixtures/mudrex_ws_frames.jsonl replayed through a real WsConnection that is
  dropped once, so each frame is recorded twice and every stream gets a real `reconnect` gap.
- Mudrex REST: the recorded kline / mark-kline bodies written by mudrex.write_rest.
- Binance: tests/fixtures/binance_depth_session.jsonl copied verbatim, plus the `gap`
  (snapshot_stale) and `depth_synced` records the recorder's depth sync writes for that session.

Malformed and duplicate lines are derived from those real lines (truncated, repeated)."""

import hashlib
import json
import logging
from decimal import Decimal
from pathlib import Path

import pytest

from tesseract.parser import run as prun
from tesseract.parser.run import Gap, day_start_ns, gap_report, main, run, split_by_day
from tesseract.recorder import mudrex
from tesseract.recorder.http import RestResult
from tesseract.recorder.records import now_ns
from tesseract.recorder.writer import JsonlWriter

from conftest import fixture_lines, fixture_text
from test_binance_depth import DEPTH, FRAMES, GOOD, STALE, make
from test_gap import FakeVenue, run_until_reconnected

MUDREX_WS_STREAMS = ["ws_kline_1m_btcusdt", "ws_markkline_1m_btcusdt", "ws_ticker_1s"]


async def record_tree(tmp_path: Path) -> Path:
    rec = tmp_path / "rec"
    await run_until_reconnected(rec, FakeVenue(fixture_lines("mudrex_ws_frames.jsonl")))

    writer = JsonlWriter(rec, fsync=False)
    for stream, name, path in (("rest_kline", "mudrex_rest_kline.json", "kline"),
                               ("rest_mark_kline", "mudrex_rest_mark_kline.json", "mark-kline")):
        t = now_ns()
        mudrex.write_rest(writer, stream, RestResult(
            path, f"https://trade.mudrex.com/fapi/v1/price/{path}", {"assets": "BTC/USDT"}, 200,
            fixture_text(name), t - 10**6, t, 1))
    writer.close()

    for line in fixture_lines("binance_depth_session.jsonl"):
        r = json.loads(line)
        p = JsonlWriter(rec).path_for(("raw", r["venue"], r["stream"]), r["recv_ns"])
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    b, w, _ = make(rec, [STALE, GOOD])  # stale first snapshot -> gap, then resync
    await b._on_public_subscribed(False)
    b._on_public_frame(DEPTH, *FRAMES[0])
    await b._resnap_task
    for obj, recv in FRAMES[1:]:
        b._on_public_frame(DEPTH, obj, recv)
    w.close()
    return rec / "raw"


def parse(tmp_path: Path, raw: Path) -> dict:
    return run(raw, tmp_path / "typed", tmp_path / "reports")


def typed(tmp_path: Path, venue: str, stream: str) -> list[dict]:
    files = sorted((tmp_path / "typed" / venue / stream).glob("*.jsonl"))
    return [json.loads(l) for f in files for l in f.read_text(encoding="utf-8").splitlines()]


def raw_records(raw: Path, venue: str, stream: str) -> list[dict]:
    return [json.loads(l) for f in sorted((raw / venue / stream).glob("*.jsonl"))
            for l in f.read_text().splitlines()]


def first_file(raw: Path, venue: str, stream: str) -> Path:
    return sorted((raw / venue / stream).glob("*.jsonl"))[0]


# --------------------------------------------------------------------------- whole tree


async def test_one_typed_record_per_raw_record_same_layout(tmp_path):
    raw = await record_tree(tmp_path)
    s = parse(tmp_path, raw)
    assert s["totals"]["parse_failures"] == 0
    assert s["totals"]["duplicates_removed"] == 0
    assert s["totals"]["records_out"] == s["totals"]["lines_in"] > 0

    raw_files = sorted(p.relative_to(raw) for p in raw.rglob("*.jsonl"))
    typed_files = sorted(p.relative_to(tmp_path / "typed") for p in (tmp_path / "typed").rglob("*"))
    assert typed_files and [p for p in typed_files if p.suffix == ".jsonl"] == raw_files
    for rel in raw_files:
        out = (tmp_path / "typed" / rel).read_bytes()
        assert out.endswith(b"\n")
        lines = out.decode("utf-8").split("\n")[:-1]
        assert len(lines) == len((raw / rel).read_text().splitlines())
        for line in lines:
            r = json.loads(line)
            assert {"v", "type", "venue", "stream", "recv_ns"} <= r.keys()
            assert (r["venue"], r["stream"]) == (rel.parts[0], rel.parts[1])


async def test_raw_files_never_modified_and_overlap_refused(tmp_path):
    raw = await record_tree(tmp_path)

    def snapshot():
        # Every path under raw (so a new file or directory would show up), plus file contents.
        return {p: (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns) if p.is_file()
                else "dir" for p in sorted(raw.rglob("*"))}

    before = snapshot()
    parse(tmp_path, raw)
    parse(tmp_path, raw)  # rerun rebuilds typed output in place
    assert snapshot() == before
    with pytest.raises(SystemExit, match="overlaps the raw root"):
        run(raw, raw / "typed", tmp_path / "reports")
    with pytest.raises(SystemExit, match="overlaps the raw root"):
        run(raw, tmp_path / "typed", raw.parent)
    assert snapshot() == before


# --------------------------------------------------------------------------- values


async def test_mudrex_values_are_exact_decimals(tmp_path):
    raw = await record_tree(tmp_path)
    parse(tmp_path, raw)

    ticks = [r for r in typed(tmp_path, "mudrex", "ws_ticker_1s") if r["type"] == "data"]
    src = [json.loads(r["raw"], parse_float=Decimal)["data"][0]
           for r in raw_records(raw, "mudrex", "ws_ticker_1s") if r["type"] == "data"]
    assert len(ticks) == len(src) > 0
    for t, s in zip(ticks, src):
        (tk,) = t["tickers"]
        assert isinstance(tk["last_price"], str) and Decimal(tk["last_price"]) == s["p"]
        assert Decimal(tk["mark_price"]) == s["mp"]

    # Mark-kline has a 6-decimal value in the recording; it must survive verbatim.
    (mk,) = [r for r in typed(tmp_path, "mudrex", "rest_mark_kline")]
    assert mk["kind"] == "mark_kline_batch" and "75897.358966" in [c["high"] for c in mk["candles"]]
    assert all("volume" not in c for c in mk["candles"])
    (k,) = typed(tmp_path, "mudrex", "rest_kline")
    body = json.loads(fixture_text("mudrex_rest_kline.json"))["data"]["asset_ticks"]["btc/usdt"]
    assert [c["open_time_ms"] for c in k["candles"]] == [row[0] * 1000 for row in body]
    assert [c["volume"] for c in k["candles"]] == [str(row[5]) for row in body]


async def test_mudrex_null_exchange_time_stays_null(tmp_path):
    raw = await record_tree(tmp_path)
    parse(tmp_path, raw)
    n = 0
    for f in (tmp_path / "typed" / "mudrex").rglob("*.jsonl"):
        for line in f.read_text().splitlines():
            r = json.loads(line)
            if r["type"] != "data":
                continue
            n += 1
            assert "exch_ts_ms" in r and r["exch_ts_ms"] is None
            assert '"exch_ts_ms":null' in line
    assert n > 0


async def test_binance_depth_keeps_st_and_exact_levels(tmp_path):
    raw = await record_tree(tmp_path)
    parse(tmp_path, raw)
    stream = "ws_btcusdt_depth_100ms"
    diffs = [r for r in typed(tmp_path, "binance", stream) if r["type"] == "data"]
    src = [json.loads(r["raw"])["data"] for r in raw_records(raw, "binance", stream) if r["type"] == "data"]
    assert len(diffs) == len(src) == 21
    for d, s in zip(diffs, src):
        assert d["kind"] == "depth_diff"
        assert d["st"] == s["st"] == 1 and d["pair"] == s["ps"]
        assert (d["first_update_id"], d["final_update_id"], d["prev_final_update_id"]) == (s["U"], s["u"], s["pu"])
        assert d["bids"] == s["b"] and d["asks"] == s["a"]  # exact strings, side by key
        assert d["exch_ts_ms"] == d["event_time_ms"] == s["E"]
        assert d["extra"] == {}

    snaps = typed(tmp_path, "binance", "rest_depth")
    raw_snaps = [json.loads(r["raw"]) for r in raw_records(raw, "binance", "rest_depth")]
    assert [s["last_update_id"] for s in snaps] == [r["lastUpdateId"] for r in raw_snaps]
    assert snaps[0]["bids"] == raw_snaps[0]["bids"] and snaps[0]["asks"] == raw_snaps[0]["asks"]


# --------------------------------------------------------------------------- passthrough + gaps


async def test_gap_and_depth_synced_pass_through(tmp_path):
    raw = await record_tree(tmp_path)
    parse(tmp_path, raw)

    out = typed(tmp_path, "binance", "ws_btcusdt_depth_100ms")
    src = raw_records(raw, "binance", "ws_btcusdt_depth_100ms")
    for kind in ("gap", "depth_synced"):
        (t,) = [r for r in out if r["type"] == kind]
        (s,) = [r for r in src if r["type"] == kind]
        for k, v in s.items():
            if k != "v":
                assert t[k] == v, (kind, k)
    (g,) = [r for r in out if r["type"] == "gap"]
    assert g["reason"] == "snapshot_stale" and g["gap_ns"] == g["gap_end_ns"] - g["gap_start_ns"]

    for stream in MUDREX_WS_STREAMS:
        (g,) = [r for r in typed(tmp_path, "mudrex", stream) if r["type"] == "gap"]
        assert g["reason"] == "reconnect" and g["gap_ns"] == g["gap_end_ns"] - g["gap_start_ns"]
        assert [r["type"] for r in typed(tmp_path, "mudrex", stream)][0] == "session_start"


async def test_gap_report_matches_recorded_gaps(tmp_path):
    raw = await record_tree(tmp_path)
    s = parse(tmp_path, raw)

    expected: dict[str, dict[str, list[int]]] = {}
    for f in raw.rglob("*.jsonl"):
        for line in f.read_text().splitlines():
            r = json.loads(line)
            if r["type"] == "gap":
                day = prun.utc_day(r["gap_start_ns"])
                assert prun.utc_day(r["gap_end_ns"]) == day  # recorded gaps are seconds long
                expected.setdefault(day, {}).setdefault(f"{r['venue']}/{r['stream']}", []).append(
                    r["gap_end_ns"] - r["gap_start_ns"])
    assert sum(len(v) for d in expected.values() for v in d.values()) == 4  # 3 reconnect + 1 stale

    for day, streams in expected.items():
        report = json.loads((tmp_path / "reports" / f"{day}.json").read_text())
        assert report == s["days"][day]
        assert set(report["gaps"]) == set(streams)
        for key, durations in streams.items():
            g = report["gaps"][key]
            assert g["count"] == len(durations)
            assert g["total_ns"] == sum(durations)
            assert g["total_ms"] == sum(durations) / 1e6


def test_gap_report_arithmetic():
    d0 = day_start_ns("2026-11-05")
    d1 = d0 + prun.NS_PER_DAY
    sec = 10**9
    gaps = [
        # two overlapping gaps on one stream: sum counts the overlap twice, union once
        Gap("mudrex", "ws_ticker_1s", d0 + 100 * sec, d0 + 200 * sec, "reconnect"),
        Gap("mudrex", "ws_ticker_1s", d0 + 150 * sec, d0 + 250 * sec, "reconnect"),
        # one gap across UTC midnight: 60s on the first day, 30s on the second
        Gap("binance", "ws_btcusdt_depth_100ms", d1 - 60 * sec, d1 + 30 * sec, "reconnect"),
        # zero-length gap counts on its day with no duration
        Gap("binance", "ws_btcusdt_depth_100ms", d1 + 500 * sec, d1 + 500 * sec, "snapshot_stale"),
    ]
    r = gap_report(gaps)
    t = r["2026-11-05"]["mudrex/ws_ticker_1s"]
    assert (t["count"], t["total_ns"], t["covered_ns"]) == (2, 200 * sec, 150 * sec)
    assert (t["total_ms"], t["covered_ms"]) == (200_000.0, 150_000.0)

    a = r["2026-11-05"]["binance/ws_btcusdt_depth_100ms"]
    b = r["2026-11-06"]["binance/ws_btcusdt_depth_100ms"]
    assert (a["count"], a["total_ns"]) == (1, 60 * sec)
    assert (b["count"], b["total_ns"], b["covered_ns"]) == (2, 30 * sec, 30 * sec)
    assert b["by_reason"] == {"reconnect": {"count": 1, "total_ns": 30 * sec},
                              "snapshot_stale": {"count": 1, "total_ns": 0}}

    # A three-day gap is split into three clipped pieces that add back up.
    pieces = split_by_day(d0 + 10, d1 + prun.NS_PER_DAY + 20)
    assert [p[0] for p in pieces] == ["2026-11-05", "2026-11-06", "2026-11-07"]
    assert sum(e - s for _, s, e in pieces) == prun.NS_PER_DAY * 2 + 10


# --------------------------------------------------------------------------- dedupe


async def test_dedupe_removes_repeated_records(tmp_path):
    raw = await record_tree(tmp_path)
    clean = parse(tmp_path / "clean", raw)["totals"]

    # At-least-once writer retries repeat whole lines: one WS frame, one REST body, one gap.
    for venue, stream, pick in (("mudrex", "ws_ticker_1s", lambda r: r["type"] == "data"),
                                ("binance", "rest_depth", lambda r: True),
                                ("mudrex", "ws_kline_1m_btcusdt", lambda r: r["type"] == "gap")):
        f = first_file(raw, venue, stream)
        lines = f.read_text().splitlines(keepends=True)
        dup = next(l for l in lines if pick(json.loads(l)))
        f.write_text("".join(lines + [dup]))

    t = parse(tmp_path, raw)["totals"]
    assert t["duplicates_removed"] == 3
    assert t["lines_in"] == clean["lines_in"] + 3
    assert t["records_out"] == clean["records_out"]
    assert t["parse_failures"] == 0


async def test_dedupe_keeps_same_payload_received_at_different_times(tmp_path):
    # After the reconnect, the venue re-sends the same subscribe acks and frames; Mudrex payloads
    # have no timestamp, so the text repeats. Those are distinct observations and must be kept.
    raw = await record_tree(tmp_path)
    s = parse(tmp_path, raw)
    day = next(d for d in s["days"] if "mudrex/ws_control" in s["days"][d]["streams"])
    ctrl = s["days"][day]["streams"]["mudrex/ws_control"]
    assert ctrl == {**ctrl, "lines_in": 4, "records_out": 4, "duplicates_removed": 0,
                    "payload_repeats_kept": 2}
    acks = typed(tmp_path, "mudrex", "ws_control")
    assert sorted(a["id"] for a in acks) == [1, 1, 2, 2]
    assert len({a["recv_ns"] for a in acks}) == 4

    ticks = s["days"][day]["streams"]["mudrex/ws_ticker_1s"]
    n_frames = sum(1 for l in fixture_lines("mudrex_ws_frames.jsonl") if "ticker@1s" in l)
    assert ticks["payload_repeats_kept"] == n_frames and ticks["duplicates_removed"] == 0


# --------------------------------------------------------------------------- malformed lines


async def test_malformed_lines_logged_counted_and_skipped(tmp_path, caplog):
    raw = await record_tree(tmp_path)
    f = first_file(raw, "mudrex", "ws_ticker_1s")
    lines = f.read_bytes().splitlines(keepends=True)
    good = len(lines)
    real = lines[2]
    rec = json.loads(real)
    bad = [
        real[: len(real) // 2] + b"\n",  # torn write, as a crash leaves it before repair
        json.dumps({**rec, "raw": rec["raw"][:-3]}).encode() + b"\n",  # payload cut short
        real[:20] + b"\xff\xfe" + real[20:],  # not UTF-8
    ]
    # Insert after line 3, so the failures are lines 4, 5, 6 and good lines follow them.
    f.write_bytes(b"".join(lines[:3] + bad + lines[3:]))

    caplog.set_level(logging.ERROR, logger="tesseract.parser.run")
    rc = main(["--raw", str(raw), "--out", str(tmp_path / "typed"),
               "--reports", str(tmp_path / "reports")])
    assert rc == 1

    reports = [json.loads(p.read_text()) for p in (tmp_path / "reports").glob("*.json")]
    fails = [x for r in reports for x in r["failures"]]
    assert sum(r["totals"]["parse_failures"] for r in reports) == 3
    assert [x["line"] for x in fails] == [4, 5, 6]
    assert all(x["file"].startswith("mudrex/ws_ticker_1s/") for x in fails)
    assert "invalid JSON" in fails[0]["error"]
    assert "raw payload is not JSON" in fails[1]["error"]
    assert "UnicodeDecodeError" in fails[2]["error"]
    for n in (4, 5, 6):
        assert any(f"{f}:{n}:" in m for m in caplog.messages)

    # Every good line still came through, and nothing half-parsed was written.
    out = typed(tmp_path, "mudrex", "ws_ticker_1s")
    assert len(out) == good
    assert [r["raw_line"] for r in out] == [1, 2, 3] + list(range(7, good + 4))
