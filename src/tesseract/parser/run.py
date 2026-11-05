"""Parse data/raw/<venue>/<stream>/<YYYY-MM-DD>.jsonl into data/typed/ with the same layout,
and write one report per UTC day.

    python -m tesseract.parser.run --raw data/raw --out data/typed --reports data/reports/typed

- Raw files are opened read-only and never modified. Output roots inside the raw root are refused.
- Each typed file is written to a temp file and renamed into place, so a rerun rebuilds it whole.
- Dedupe: a record is dropped when it repeats an earlier record of the same file with the same
  venue, stream, recv_ns and payload (`raw` for data records, the whole record otherwise). That
  is what the writer's at-least-once retry produces. recv_ns is part of the key on purpose: Mudrex
  payloads carry no timestamp, so the same ticker or subscribe-ack text recurs legitimately.
  Records whose payload alone repeats are kept and counted as `payload_repeats_kept`.
- An unparseable line is logged (file, line, error), counted, and skipped; the run continues and
  exits with status 1 at the end.
- Gaps per UTC day: each `gap` record's [gap_start_ns, gap_end_ns) interval is split at UTC
  midnight. `count` is the number of gaps touching the day, `total_ms` the sum of their durations
  within the day, `covered_ms` the union (overlapping gaps counted once). A zero-length gap
  counts on the day it starts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .typed import ParseError, typed_record

log = logging.getLogger(__name__)

NS_PER_DAY = 86_400 * 10**9
MAX_FAILURES_LISTED = 1000


@dataclass
class FileResult:
    venue: str
    stream: str
    day: str
    lines_in: int = 0
    records_out: int = 0
    duplicates_removed: int = 0
    payload_repeats_kept: int = 0
    failures: list[tuple[int, str]] = field(default_factory=list)
    types: Counter = field(default_factory=Counter)
    gaps: list[tuple[int, int, str]] = field(default_factory=list)  # (start_ns, end_ns, reason)


@dataclass
class Gap:
    venue: str
    stream: str
    start_ns: int
    end_ns: int
    reason: str


def _is_within(p: Path, root: Path) -> bool:
    try:
        p.relative_to(root)
        return True
    except ValueError:
        return False


def _digest(*parts: str) -> bytes:
    h = hashlib.blake2b(digest_size=16)
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\x00")
    return h.digest()


def _encode(rec: dict[str, Any]) -> str:
    return json.dumps(rec, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n"


def parse_file(src: Path, dst: Path, venue: str, stream: str, day: str) -> FileResult:
    res = FileResult(venue, stream, day)
    seen: set[bytes] = set()
    seen_payload: set[bytes] = set()
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")

    def fail(lineno: int, err: str) -> None:
        log.error("%s:%d: %s", src, lineno, err)
        res.failures.append((lineno, err))

    with open(src, "rb") as fin, open(tmp, "w", encoding="utf-8", newline="\n") as fout:
        for lineno, bline in enumerate(fin, 1):
            res.lines_in += 1
            try:
                text = bline.decode("utf-8")
                if not text.strip():
                    raise ParseError("empty line")
                try:
                    rec = json.loads(text)
                except ValueError as e:
                    raise ParseError(f"invalid JSON: {e}") from None
                if not isinstance(rec, dict):
                    raise ParseError("record is not a JSON object")
                for k in ("type", "venue", "stream", "recv_ns"):
                    if k not in rec:
                        raise ParseError(f"record has no {k!r}")
                if (rec["venue"], rec["stream"]) != (venue, stream):
                    raise ParseError(f"record is {rec['venue']}/{rec['stream']}, file is {venue}/{stream}")

                recv = str(rec["recv_ns"])
                if rec["type"] == "data" and isinstance(rec.get("raw"), str):
                    payload = rec["raw"]
                else:
                    payload = json.dumps(rec, sort_keys=True, separators=(",", ":"))
                key = _digest(recv, payload)
                if key in seen:
                    res.duplicates_removed += 1
                    continue
                seen.add(key)
                pkey = _digest(payload)
                if pkey in seen_payload:
                    res.payload_repeats_kept += 1
                seen_payload.add(pkey)

                out = typed_record(rec, venue, stream, lineno)
                line = _encode(out)
            except (ParseError, KeyError, TypeError, ValueError, UnicodeDecodeError) as e:
                msg = f"{type(e).__name__}: {e}" if not isinstance(e, ParseError) else str(e)
                if isinstance(e, KeyError):
                    msg = f"missing field {e}"
                fail(lineno, msg)
                continue
            fout.write(line)
            res.records_out += 1
            res.types[out["type"]] += 1
            if out["type"] == "gap":
                res.gaps.append((out["gap_start_ns"], out["gap_end_ns"], str(out.get("reason"))))
    os.replace(tmp, dst)
    return res


def day_start_ns(day: str) -> int:
    d = date.fromisoformat(day)
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()) * 10**9


def utc_day(ns: int) -> str:
    return datetime.fromtimestamp(ns // 10**9, tz=timezone.utc).strftime("%Y-%m-%d")


def split_by_day(start: int, end: int) -> list[tuple[str, int, int]]:
    """[start, end) -> [(day, clipped_start, clipped_end)], one entry per UTC day touched."""
    if end <= start:
        return [(utc_day(start), start, start)]
    out = []
    day = utc_day(start)
    while True:
        lo = day_start_ns(day)
        hi = lo + NS_PER_DAY
        out.append((day, max(start, lo), min(end, hi)))
        if end <= hi:
            return out
        day = (date.fromisoformat(day) + timedelta(days=1)).isoformat()


def union_ns(intervals: list[tuple[int, int]]) -> int:
    total, cur_s, cur_e = 0, None, None
    for s, e in sorted(intervals):
        if cur_e is None or s > cur_e:
            if cur_e is not None:
                total += cur_e - cur_s
            cur_s, cur_e = s, e
        else:
            cur_e = max(cur_e, e)
    if cur_e is not None:
        total += cur_e - cur_s
    return total


def gap_report(gaps: list[Gap]) -> dict[str, dict[str, dict[str, Any]]]:
    """{day: {"venue/stream": {count, total_ms, covered_ms, by_reason}}}"""
    pieces: dict[tuple[str, str], list[tuple[int, int, str]]] = defaultdict(list)
    for g in gaps:
        for day, s, e in split_by_day(g.start_ns, g.end_ns):
            pieces[(day, f"{g.venue}/{g.stream}")].append((s, e, g.reason))
    out: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for (day, key), ps in sorted(pieces.items()):
        reasons: dict[str, dict[str, Any]] = {}
        for s, e, r in ps:
            rr = reasons.setdefault(r, {"count": 0, "total_ns": 0})
            rr["count"] += 1
            rr["total_ns"] += e - s
        total_ns = sum(e - s for s, e, _ in ps)
        out[day][key] = {
            "count": len(ps),
            "total_ns": total_ns,
            "total_ms": total_ns / 1e6,
            "covered_ns": union_ns([(s, e) for s, e, _ in ps]),
            "by_reason": reasons,
        }
        out[day][key]["covered_ms"] = out[day][key]["covered_ns"] / 1e6
    return dict(out)


def run(raw_root: Path, typed_root: Path, report_dir: Path) -> dict[str, Any]:
    raw_root, typed_root, report_dir = raw_root.resolve(), typed_root.resolve(), report_dir.resolve()
    if not raw_root.is_dir():
        raise SystemExit(f"raw root {raw_root} is not a directory")
    for p in (typed_root, report_dir):
        if _is_within(p, raw_root) or _is_within(raw_root, p):
            raise SystemExit(f"refusing to write {p}: overlaps the raw root {raw_root}")

    results: list[FileResult] = []
    for src in sorted(raw_root.glob("*/*/*.jsonl")):
        venue, stream, day = src.parent.parent.name, src.parent.name, src.stem
        try:
            date.fromisoformat(day)
        except ValueError:
            log.error("%s: file name is not a UTC date; skipped", src)
            continue
        results.append(parse_file(src, typed_root / venue / stream / src.name, venue, stream, day))

    gaps = [Gap(r.venue, r.stream, s, e, why) for r in results for s, e, why in r.gaps]
    gaps_by_day = gap_report(gaps)

    days = sorted({r.day for r in results} | set(gaps_by_day))
    summary: dict[str, Any] = {"days": {}}
    report_dir.mkdir(parents=True, exist_ok=True)
    for day in days:
        streams = {}
        failures = []
        for r in results:
            if r.day != day:
                continue
            key = f"{r.venue}/{r.stream}"
            streams[key] = {"lines_in": r.lines_in, "records_out": r.records_out,
                            "duplicates_removed": r.duplicates_removed,
                            "payload_repeats_kept": r.payload_repeats_kept,
                            "parse_failures": len(r.failures), "types": dict(sorted(r.types.items()))}
            failures += [{"file": f"{key}/{day}.jsonl", "line": n, "error": err} for n, err in r.failures]
        totals = {k: sum(s[k] for s in streams.values())
                  for k in ("lines_in", "records_out", "duplicates_removed", "payload_repeats_kept",
                            "parse_failures")}
        report = {"day": day, "totals": totals, "streams": streams,
                  "gaps": gaps_by_day.get(day, {}),
                  "failures": failures[:MAX_FAILURES_LISTED],
                  "failures_truncated": len(failures) > MAX_FAILURES_LISTED}
        tmp = report_dir / f"{day}.json.tmp"
        tmp.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(tmp, report_dir / f"{day}.json")
        summary["days"][day] = report
    summary["totals"] = {k: sum(d["totals"][k] for d in summary["days"].values())
                         for k in ("lines_in", "records_out", "duplicates_removed",
                                   "payload_repeats_kept", "parse_failures")}
    return summary


def render(summary: dict[str, Any]) -> str:
    out = []
    t = summary["totals"]
    out.append(f"lines in {t['lines_in']}, records out {t['records_out']}, "
               f"duplicates removed {t['duplicates_removed']}, parse failures {t['parse_failures']} "
               f"(payload repeats kept: {t['payload_repeats_kept']})")
    for day, rep in summary["days"].items():
        out.append(f"\n## {day}")
        out.append("| venue/stream | in | out | dupes | failures | gaps | gap total s | gap covered s |")
        out.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        keys = sorted(set(rep["streams"]) | set(rep["gaps"]))
        for k in keys:
            s = rep["streams"].get(k, {})
            g = rep["gaps"].get(k, {})
            out.append(f"| {k} | {s.get('lines_in', 0)} | {s.get('records_out', 0)} | "
                       f"{s.get('duplicates_removed', 0)} | {s.get('parse_failures', 0)} | "
                       f"{g.get('count', 0)} | {g.get('total_ns', 0) / 1e9:.3f} | "
                       f"{g.get('covered_ns', 0) / 1e9:.3f} |")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--raw", type=Path, default=Path("data/raw"))
    ap.add_argument("--out", type=Path, default=Path("data/typed"))
    ap.add_argument("--reports", type=Path, default=Path("data/reports/typed"))
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    summary = run(args.raw, args.out, args.reports)
    print(render(summary))
    n = summary["totals"]["parse_failures"]
    if n:
        print(f"\n{n} unparseable line(s); see the log above and the day reports", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
