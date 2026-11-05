"""Turn one raw recorder record into one typed record.

Conventions in the typed output:
- Prices, sizes, rates and volumes are JSON strings holding the exact decimal text received
  (Binance sends strings; Mudrex JSON numbers are read as Decimal, never float). They are
  checked to be finite decimals, so a typed value always parses with Decimal().
- Times are integers: `*_ns` for local receive times, `*_ms` for exchange times.
- `exch_ts_ms` is `null` when the venue provides no exchange time (all of Mudrex, and Binance
  fundingRate/klines). It is never replaced with 0, and nothing is derived from it when null.
- Book levels are `[price, size]` pairs under `bids` / `asks`; the key is the side.
- Undocumented fields are kept: Binance depth `st` and `ps`, mark price `st` and `ap`, and any
  other key not mapped to a typed field goes into `extra` unchanged.

Every parser raises ParseError (or KeyError/TypeError/ValueError, which the caller treats the
same way) on input it cannot type, so a bad line is counted as a failure and never written
half-parsed.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

TYPED_VERSION = 1

# Record types the recorder writes (see README "Record schema").
DATA = "data"
PASSTHROUGH_TYPES = {"gap", "depth_synced", "session_start", "error"}


class ParseError(ValueError):
    pass


# --------------------------------------------------------------------------- value helpers


def loads(text: str) -> Any:
    """JSON with floats as Decimal, so no price is ever rounded through a binary float."""
    return json.loads(text, parse_float=Decimal)


def dec(v: Any, name: str) -> str:
    """A finite decimal as its exact string form. Accepts JSON strings, ints and Decimals."""
    if isinstance(v, bool) or not isinstance(v, (str, int, Decimal)):
        raise ParseError(f"{name}: expected a decimal, got {type(v).__name__} {v!r}")
    try:
        d = Decimal(v) if not isinstance(v, Decimal) else v
    except InvalidOperation:
        raise ParseError(f"{name}: not a decimal: {v!r}") from None
    if not d.is_finite():
        raise ParseError(f"{name}: not finite: {v!r}")
    return v if isinstance(v, str) else str(v)


def opt_dec(v: Any, name: str) -> str | None:
    """Binance sends "" for some absent values (e.g. old fundingRate markPrice)."""
    return None if v is None or v == "" else dec(v, name)


def integer(v: Any, name: str) -> int:
    if isinstance(v, bool) or not isinstance(v, int):
        raise ParseError(f"{name}: expected an integer, got {type(v).__name__} {v!r}")
    return v


def levels(v: Any, name: str) -> list[list[str]]:
    if not isinstance(v, list):
        raise ParseError(f"{name}: expected a list of levels")
    out = []
    for i, lvl in enumerate(v):
        if not isinstance(lvl, list) or len(lvl) != 2:
            raise ParseError(f"{name}[{i}]: expected [price, size], got {lvl!r}")
        out.append([dec(lvl[0], f"{name}[{i}].price"), dec(lvl[1], f"{name}[{i}].size")])
    return out


def plain(v: Any) -> Any:
    """Undo Decimal parsing for fields kept verbatim (st, ap, extra), keeping their JSON type."""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, list):
        return [plain(x) for x in v]
    if isinstance(v, dict):
        return {k: plain(x) for k, x in v.items()}
    return v


def obj(v: Any, name: str) -> dict[str, Any]:
    if not isinstance(v, dict):
        raise ParseError(f"{name}: expected an object, got {type(v).__name__}")
    return v


def extra(d: dict[str, Any], mapped: set[str]) -> dict[str, Any]:
    return {k: plain(v) for k, v in d.items() if k not in mapped}


def ws_envelope(payload: Any, stream: str) -> tuple[str, Any]:
    """{"stream": <name>, "data": ...}; checks the frame belongs in this stream's directory."""
    env = obj(payload, "frame")
    name = env.get("stream")
    if not isinstance(name, str) or "data" not in env:
        raise ParseError("frame has no stream/data envelope")
    if stream_dir(name) != stream:
        raise ParseError(f"frame stream {name!r} does not belong in {stream!r}")
    return name, env["data"]


def stream_dir(name: str) -> str:
    # Same mapping as the recorder (tesseract.recorder.ws.stream_dir), kept local so the
    # parser does not import aiohttp.
    return "ws_" + re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


# --------------------------------------------------------------------------- shared shapes


def control(payload: Any, stream: str) -> dict[str, Any]:
    """Subscribe acks and errors from either venue."""
    m = obj(payload, "control message")
    return {"kind": "control", "id": m.get("id"), "method": m.get("method"),
            "result": plain(m.get("result")), "error": plain(m.get("error")),
            "extra": extra(m, {"id", "method", "result", "error"})}


# --------------------------------------------------------------------------- Mudrex


def mudrex_ws(payload: Any, stream: str) -> dict[str, Any]:
    name, data = ws_envelope(payload, stream)
    kind = name.split("@", 1)[0]
    if kind == "ticker":
        if not isinstance(data, list):
            raise ParseError("ticker data: expected a list")
        return {"kind": "ticker", "channel": name, "tickers": [
            {"symbol": obj(t, "ticker")["s"], "last_price": dec(t["p"], "p"),
             "mark_price": dec(t["mp"], "mp"), "extra": extra(t, {"s", "p", "mp"})}
            for t in data]}
    if kind in ("kline", "markKline"):
        d = obj(data, "kline data")
        rec = {"kind": "kline" if kind == "kline" else "mark_kline", "channel": name,
               "symbol": d["s"], "interval": name.split("@")[1],
               # Mudrex `t` is the candle open time in epoch seconds; it is not an exchange send time.
               "open_time_ms": integer(d["t"], "t") * 1000,
               "open": dec(d["o"], "o"), "high": dec(d["h"], "h"),
               "low": dec(d["l"], "l"), "close": dec(d["c"], "c")}
        mapped = {"s", "t", "o", "h", "l", "c"}
        if kind == "kline":
            rec["volume"] = dec(d["v"], "v")
            mapped.add("v")
        rec["extra"] = extra(d, mapped)
        return rec
    raise ParseError(f"no Mudrex parser for WS channel {name!r}")


def mudrex_rest_kline(payload: Any, stream: str) -> dict[str, Any]:
    """{"success":true,"data":{"asset_ticks":{"btc/usdt":[[t,o,h,l,c(,v)],...]}}}; t in seconds.
    kline rows have 6 values, mark-kline rows have 5 (no volume)."""
    body = obj(payload, "response")
    width = 6 if stream == "rest_kline" else 5
    ticks = obj(obj(body["data"], "data")["asset_ticks"], "asset_ticks")
    candles = []
    for asset, rows in ticks.items():
        if not isinstance(rows, list):
            raise ParseError(f"asset_ticks[{asset!r}]: expected a list")
        for i, row in enumerate(rows):
            if not isinstance(row, list) or len(row) != width:
                raise ParseError(f"asset_ticks[{asset!r}][{i}]: expected {width} values, got {row!r}")
            c = {"asset": asset, "open_time_ms": integer(row[0], "t") * 1000,
                 "open": dec(row[1], "o"), "high": dec(row[2], "h"),
                 "low": dec(row[3], "l"), "close": dec(row[4], "c")}
            if width == 6:
                c["volume"] = dec(row[5], "v")
            candles.append(c)
    return {"kind": "kline_batch" if stream == "rest_kline" else "mark_kline_batch",
            "success": body.get("success"), "candles": candles,
            "extra": extra(body, {"success", "data"})}


# --------------------------------------------------------------------------- Binance


def binance_ws(payload: Any, stream: str) -> dict[str, Any]:
    name, data = ws_envelope(payload, stream)
    d = obj(data, "data")
    event = d.get("e")
    if event == "depthUpdate":
        return {"kind": "depth_diff", "channel": name, "symbol": d["s"], "pair": d.get("ps"),
                "event_time_ms": integer(d["E"], "E"), "transaction_time_ms": integer(d["T"], "T"),
                "first_update_id": integer(d["U"], "U"), "final_update_id": integer(d["u"], "u"),
                "prev_final_update_id": integer(d["pu"], "pu"),
                "bids": levels(d["b"], "b"), "asks": levels(d["a"], "a"),
                # Undocumented, seen live; kept verbatim. null means absent from the frame.
                "st": plain(d.get("st")),
                "extra": extra(d, {"e", "E", "T", "s", "ps", "U", "u", "pu", "b", "a", "st"})}
    if event == "markPriceUpdate":
        return {"kind": "mark_price", "channel": name, "symbol": d["s"],
                "event_time_ms": integer(d["E"], "E"),
                "mark_price": dec(d["p"], "p"), "index_price": opt_dec(d.get("i"), "i"),
                "estimated_settle_price": opt_dec(d.get("P"), "P"),
                "funding_rate": opt_dec(d.get("r"), "r"),
                "next_funding_time_ms": d.get("T") if d.get("T") is None else integer(d["T"], "T"),
                # Undocumented / newer fields, seen live; kept verbatim. null means absent.
                "ap": plain(d.get("ap")), "st": plain(d.get("st")),
                "extra": extra(d, {"e", "E", "s", "p", "i", "P", "r", "T", "ap", "st"})}
    raise ParseError(f"no Binance parser for event {event!r} on {name!r}")


def binance_rest_depth(payload: Any, stream: str) -> dict[str, Any]:
    b = obj(payload, "response")
    return {"kind": "depth_snapshot", "last_update_id": integer(b["lastUpdateId"], "lastUpdateId"),
            "event_time_ms": integer(b["E"], "E"), "transaction_time_ms": integer(b["T"], "T"),
            "bids": levels(b["bids"], "bids"), "asks": levels(b["asks"], "asks"),
            "extra": extra(b, {"lastUpdateId", "E", "T", "bids", "asks"})}


def binance_rest_premium_index(payload: Any, stream: str) -> dict[str, Any]:
    b = obj(payload, "response")
    return {"kind": "premium_index", "symbol": b["symbol"],
            "mark_price": dec(b["markPrice"], "markPrice"),
            "index_price": opt_dec(b.get("indexPrice"), "indexPrice"),
            "estimated_settle_price": opt_dec(b.get("estimatedSettlePrice"), "estimatedSettlePrice"),
            "funding_rate": opt_dec(b.get("lastFundingRate"), "lastFundingRate"),
            "interest_rate": opt_dec(b.get("interestRate"), "interestRate"),
            "next_funding_time_ms": integer(b["nextFundingTime"], "nextFundingTime"),
            "time_ms": integer(b["time"], "time"),
            "extra": extra(b, {"symbol", "markPrice", "indexPrice", "estimatedSettlePrice",
                               "lastFundingRate", "interestRate", "nextFundingTime", "time"})}


def binance_rest_funding_rate(payload: Any, stream: str) -> dict[str, Any]:
    if not isinstance(payload, list):
        raise ParseError("fundingRate: expected a list")
    rates = []
    for i, r in enumerate(payload):
        r = obj(r, f"fundingRate[{i}]")
        rates.append({"symbol": r["symbol"], "funding_rate": dec(r["fundingRate"], "fundingRate"),
                      "funding_time_ms": integer(r["fundingTime"], "fundingTime"),
                      "mark_price": opt_dec(r.get("markPrice"), "markPrice"),
                      "extra": extra(r, {"symbol", "fundingRate", "fundingTime", "markPrice"})})
    return {"kind": "funding_rate_history", "rates": rates}


def binance_rest_klines(payload: Any, stream: str) -> dict[str, Any]:
    if not isinstance(payload, list):
        raise ParseError("klines: expected a list")
    candles = []
    for i, k in enumerate(payload):
        if not isinstance(k, list) or len(k) < 11:
            raise ParseError(f"klines[{i}]: expected at least 11 values")
        candles.append({"open_time_ms": integer(k[0], "openTime"), "open": dec(k[1], "open"),
                        "high": dec(k[2], "high"), "low": dec(k[3], "low"),
                        "close": dec(k[4], "close"), "volume": dec(k[5], "volume"),
                        "close_time_ms": integer(k[6], "closeTime"),
                        "quote_volume": dec(k[7], "quoteVolume"), "trades": integer(k[8], "trades"),
                        "taker_buy_base_volume": dec(k[9], "takerBuyBase"),
                        "taker_buy_quote_volume": dec(k[10], "takerBuyQuote")})
    return {"kind": "kline_batch", "candles": candles}


# --------------------------------------------------------------------------- dispatch

Parser = Callable[[Any, str], dict[str, Any]]


def parser_for(venue: str, stream: str) -> Parser:
    if stream == "ws_control":
        return control
    if venue == "mudrex":
        if stream.startswith("ws_"):
            return mudrex_ws
        if stream in ("rest_kline", "rest_mark_kline"):
            return mudrex_rest_kline
    elif venue == "binance":
        if stream.startswith("ws_"):
            return binance_ws
        p = {"rest_depth": binance_rest_depth, "rest_premium_index": binance_rest_premium_index,
             "rest_funding_rate": binance_rest_funding_rate, "rest_klines": binance_rest_klines}.get(stream)
        if p:
            return p
    raise ParseError(f"no parser for {venue}/{stream}")


def exch_ts(rec: dict[str, Any]) -> int | None:
    """Recorder exchange time. Must be present on data records; null stays null, never 0."""
    if "exch_ts_ms" not in rec:
        raise ParseError("data record has no exch_ts_ms field")
    v = rec["exch_ts_ms"]
    if v is None:
        return None
    return integer(v, "exch_ts_ms")


def typed_record(rec: dict[str, Any], venue: str, stream: str, line: int) -> dict[str, Any]:
    """One raw recorder record (already JSON-decoded, venue/stream checked) -> one typed record."""
    rtype = rec["type"]
    recv_ns = integer(rec["recv_ns"], "recv_ns")
    head = {"v": TYPED_VERSION, "type": rtype, "venue": venue, "stream": stream,
            "recv_ns": recv_ns, "raw_line": line}
    if rtype == DATA:
        raw = rec.get("raw")
        if not isinstance(raw, str):
            raise ParseError("data record has no raw payload text")
        try:
            payload = loads(raw)
        except ValueError as e:
            raise ParseError(f"raw payload is not JSON: {e}") from None
        head["exch_ts_ms"] = exch_ts(rec)
        head["source"] = rec.get("source")
        head.update(parser_for(venue, stream)(payload, stream))
        return head
    if rtype in PASSTHROUGH_TYPES:
        body = {k: v for k, v in rec.items() if k not in ("v", "type", "venue", "stream", "recv_ns")}
        if rtype == "gap":
            start, end = integer(rec["gap_start_ns"], "gap_start_ns"), integer(rec["gap_end_ns"], "gap_end_ns")
            if end < start:
                raise ParseError(f"gap ends before it starts ({start} > {end})")
            body["gap_ns"] = end - start
        head.update(body)
        return head
    raise ParseError(f"unknown record type {rtype!r}")
