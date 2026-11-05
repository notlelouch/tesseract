"""Summarize a data directory: records and bytes per stream (and per hour), gaps,
and REST latency percentiles per endpoint. Reads only; never modifies data.

    python -m tesseract.recorder.summary --data-dir data
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def pct(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = max(0, min(len(s) - 1, round(p / 100 * (len(s) - 1))))
    return s[k]


def summarize(data_dir: Path) -> str:
    out: list[str] = []
    raw = data_dir / "raw"
    out.append("## Streams")
    out.append("| venue/stream | records | data | gaps | errors | MB | span h | MB/hour |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for stream_dir in sorted(p for p in raw.glob("*/*") if p.is_dir()):
        n = data = gaps = errors = 0
        size = 0
        first = last = None
        for f in sorted(stream_dir.glob("*.jsonl")):
            size += f.stat().st_size
            for line in f.open():
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                n += 1
                t = r.get("type")
                data += t == "data"
                gaps += t == "gap"
                errors += t == "error"
                ns = r.get("recv_ns")
                if ns:
                    first = ns if first is None else min(first, ns)
                    last = ns if last is None else max(last, ns)
        span_h = ((last - first) / 3.6e12) if first and last and last > first else 0
        mb = size / 1e6
        rate = f"{mb / span_h:.3f}" if span_h >= 0.1 else "-"
        out.append(f"| {stream_dir.parent.name}/{stream_dir.name} | {n} | {data} | {gaps} | {errors} | "
                   f"{mb:.3f} | {span_h:.2f} | {rate} |")

    lat: dict[tuple[str, str], list[float]] = defaultdict(list)
    lat_err: dict[tuple[str, str], int] = defaultdict(int)
    ws_events: dict[tuple[str, str], list[str]] = defaultdict(list)
    for f in sorted((data_dir / "latency").rglob("*.jsonl")):
        for line in f.open():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("event") == "rest":
                key = (r["venue"], r["endpoint"])
                if r.get("status") == 200 and not r.get("error"):
                    lat[key].append(r["duration_ms"])
                else:
                    lat_err[key] += 1
            elif r.get("event") in ("ws_connect", "ws_disconnect"):
                tag = r["event"] if r.get("event") == "ws_disconnect" else ("ws_connect" if r.get("ok") else "ws_connect_failed")
                ws_events[(r["venue"], r["conn"])].append(tag)
    out.append("\n## REST latency (ms, successful requests)")
    out.append("| venue/endpoint | n | errors | p50 | p90 | p99 | max | mean |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for key in sorted(set(lat) | set(lat_err)):
        v = lat[key]
        mean = statistics.fmean(v) if v else float("nan")
        out.append(f"| {key[0]}/{key[1]} | {len(v)} | {lat_err[key]} | {pct(v, 50):.0f} | {pct(v, 90):.0f} | "
                   f"{pct(v, 99):.0f} | {max(v) if v else float('nan'):.0f} | {mean:.0f} |")
    out.append("\n## WebSocket connections")
    for key, evs in sorted(ws_events.items()):
        out.append(f"- {key[0]}/{key[1]}: connects={evs.count('ws_connect')} "
                   f"failed_connects={evs.count('ws_connect_failed')} disconnects={evs.count('ws_disconnect')}")
    return "\n".join(out)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", default="data")
    print(summarize(Path(p.parse_args().data_dir)))


if __name__ == "__main__":
    main()
