# tesseract

Tesseract v1: a market data recorder for BTC perpetuals. It records public Mudrex and Binance USD-M
market data to append-only JSONL with timestamps, so a later paper engine can replay exactly what
was received.

- It uses only public, unauthenticated market data endpoints. There are no API keys and no order
  endpoints, and the config loader rejects any key that looks like a secret.
- It stores raw payloads verbatim. Parsing into typed fields is a separate, later step, so a parser
  bug can be fixed and the data replayed.

## Setup and run

Requires Python 3.11+ (uses stdlib `tomllib`). The only runtime dependency is `aiohttp`.

```bash
make install                   # creates .venv (uv if available, else venv) and installs -e .[dev]
make test                      # unit tests; fixtures only, no network
make run                       # tesseract-record --config config.toml (runs until SIGINT/SIGTERM)
make run-dev DURATION=3600     # stops itself after DURATION seconds
make run CONFIG=config.dev.toml
make summary                   # records, MB/hour per stream, REST latency percentiles
make parse                     # data/raw -> data/typed + data/reports/typed/<day>.json (DATA=dir to override)
```

- `config.toml` is the full config with both venues enabled.
- `config.dev.toml` is the same, but with Binance disabled, for networks where Binance is blocked
  (see "Known doc mismatches and findings").

## File layout

```
data/
  raw/<venue>/<stream>/<YYYY-MM-DD>.jsonl     # one file per venue, stream and UTC date
  latency/<venue>/<YYYY-MM-DD>.jsonl          # REST start/end and WS connect/disconnect times
```

Streams currently produced:

| venue | stream dir | source |
|---|---|---|
| mudrex | `ws_ticker_1s` | WS `ticker@1s` (last `p` and mark `mp`) |
| mudrex | `ws_kline_1m_btcusdt` | WS `kline@1m@btcusdt` |
| mudrex | `ws_markkline_1m_btcusdt` | WS `markKline@1m@btcusdt` |
| mudrex | `ws_control` | WS subscribe acks and errors (anything without a `stream` field) |
| mudrex | `rest_kline`, `rest_mark_kline` | `GET /kline`, `GET /mark-kline` polled every 60s, 5-minute lookback |
| binance | `ws_btcusdt_depth_100ms` | WS diff depth |
| binance | `ws_btcusdt_markprice_1s` | WS mark price |
| binance | `ws_control` | WS subscribe acks and errors |
| binance | `rest_depth` | `GET /fapi/v1/depth?limit=1000` every 60s, plus immediately after every depth-WS connect (first and reconnects) and after a failed sequence check |
| binance | `rest_premium_index` | `GET /fapi/v1/premiumIndex` every 10s |
| binance | `rest_funding_rate` | `GET /fapi/v1/fundingRate` every 1h |
| binance | `rest_klines` | `GET /fapi/v1/klines` 1m, last 5, every 60s (cross-check) |

The example name in the brief was `ws_ticker`. Stream directories are named after the subscribed
stream (`ticker@1s` becomes `ws_ticker_1s`), so `ticker@5s` would not collide with it.

### Record schema (`v: 1`)

Every line is one JSON object. Common fields: `v`, `type`, `venue`, `stream`, `recv_ns` (local
receive time in ns since the epoch, UTC).

| type | meaning | extra fields |
|---|---|---|
| `data` | one WS frame or one REST response | `source` (`ws`/`rest`), `exch_ts_ms` (exchange time if the venue provides one, else `null`), `raw` (payload text exactly as received). WS: `conn`, `conn_id`. REST: `req_start_ns`, `request` {url, params}, `status`, `used_weight_1m` (Binance) |
| `session_start` | first successful connect of this recorder process for this stream | `conn`, `conn_id`, `url` |
| `gap` | `reason: reconnect` is written to every stream of a connection after a reconnect. Binance depth only: `reason: sequence_break` (a `pu` mismatch) or `snapshot_stale` (snapshot older than the buffered diffs) | `gap_start_ns`, `gap_end_ns`, `gap_ms`, `reason`. Reconnect: start is the last frame before the drop (or the drop time), end is when re-subscribe was sent. Depth reasons add `expected_pu`/`got_pu`/`U`/`u` or `last_update_id`/`first_U`/`first_u` |
| `depth_synced` | Binance depth only: the recorded diff stream can rebuild the book from this point | `last_update_id`, `snapshot_recv_ns`, `first_U`, `first_u`, `first_recv_ns` (first diff to apply), `applied`, `dropped` |
| `error` | a REST poll failed after all retries | `status`, `error`, `attempts`, `raw` (body if any) |

Where `exch_ts_ms` comes from:

| source | exch_ts_ms |
|---|---|
| Binance WS | `data.E` (event time) |
| Binance REST depth | `E` |
| Binance REST premiumIndex | `time` |
| Binance REST fundingRate, klines | `null` |
| Mudrex (all) | `null`: no Mudrex frame or response carries a send or event time. Kline `t` is the candle open time and stays in `raw`. |

For replay:

- Restarts show up as a new `session_start`.
- Disconnects within a run show up as `gap` records.
- Binance depth: start the book from the `rest_depth` record whose `lastUpdateId` matches the
  latest `depth_synced`, then apply diffs from `first_U`/`first_u` onward. Any `gap` record
  invalidates the book until the next `depth_synced`.
- The recorder checks the diff sequence while it records, following Binance's documented USD-M
  procedure:
  - On every WS connect it takes a snapshot right away and buffers diff IDs (the raw frames are
    still written immediately).
  - It drops diffs with `u < lastUpdateId`.
  - The first diff it applies must satisfy `U <= lastUpdateId <= u`.
  - After that, each diff's `pu` must equal the previous diff's `u`. Any failure writes a `gap` and
    triggers a new snapshot.
  - It tracks IDs only. Rebuilding price levels stays a separate replay step.
- Live check (2026-11-05): `depth_synced` was written 2.3s after the first subscribe, with no
  sequence breaks over 1,441 frames.

On macOS, `time.time_ns()` has microsecond resolution, so `recv_ns` ends in `000`.

### Durability

- The writer buffers in memory and flushes every `flush_interval_s` (1s), with `fsync`, and again on
  shutdown.
- If a disk write fails, the records stay buffered and the error is logged.
- If the buffer exceeds `max_buffer_records`, the recorder stops with exit code 2 rather than drop
  data.
- On reopen, a torn last line (left by a crash) is terminated so later lines stay parseable.
- Delivery is at-least-once: a write that partly succeeds and is then retried can duplicate lines.

## Typed parser (`tesseract.parser`)

`make parse` (or `tesseract-parse --raw data/raw --out data/typed --reports data/reports/typed`)
turns each `data/raw/<venue>/<stream>/<day>.jsonl` into `data/typed/<venue>/<stream>/<day>.jsonl`,
one typed record per raw record (UTF-8, one JSON object per line). Raw files are opened read-only.
Output paths that overlap the raw root are refused. Typed files are rebuilt whole on every run
(written to a temp file, then renamed into place).

Typed record: `v`, `type`, `venue`, `stream`, `recv_ns`, `raw_line` (line in the raw file), and for
`data`: `exch_ts_ms`, `source`, `kind` plus parsed fields:

| kind | from | fields |
|---|---|---|
| `ticker` | Mudrex `ticker@1s` | `tickers[]`: `symbol`, `last_price`, `mark_price` |
| `kline`, `mark_kline` | Mudrex WS klines | `symbol`, `interval`, `open_time_ms`, `open/high/low/close`, `volume` (kline only) |
| `kline_batch`, `mark_kline_batch` | Mudrex REST | `success`, `candles[]` with `asset`, `open_time_ms`, OHLC(V) |
| `control` | `ws_control` (both) | `id`, `method`, `result`, `error` |
| `depth_diff` | Binance depth WS | `symbol`, `pair` (`ps`), `event_time_ms`, `transaction_time_ms`, `first_update_id`/`final_update_id`/`prev_final_update_id`, `bids`, `asks`, `st` |
| `mark_price` | Binance markPrice WS | `mark_price`, `index_price`, `estimated_settle_price`, `funding_rate`, `next_funding_time_ms`, `ap`, `st` |
| `depth_snapshot` | Binance `rest_depth` | `last_update_id`, `event_time_ms`, `transaction_time_ms`, `bids`, `asks` |
| `premium_index`, `funding_rate_history`, `kline_batch` | Binance REST | documented fields, snake_cased |

- Prices, sizes, rates and volumes are strings holding the exact decimal text received; Mudrex
  JSON numbers are read as `Decimal`, never `float`. Book levels are `[price, size]` under
  `bids`/`asks`. Fields not mapped to a typed field go to `extra` unchanged.
- `exch_ts_ms` is `null` when the venue has no exchange time (all of Mudrex). It is never 0.
- `gap`, `depth_synced`, `session_start` and `error` records pass through with all their fields;
  `gap` adds an integer `gap_ns`.
- **Dedupe key: venue, stream, `recv_ns` and payload** (`raw` for data records, the whole record
  otherwise). This removes the writer's at-least-once retry duplicates. `recv_ns` is in the key
  because Mudrex payloads carry no timestamp, so identical text recurs legitimately. On
  2026-11-05 a payload-only key would have dropped 44 real ticker updates in `data/raw`, plus 16
  ticker updates and 6 reconnect subscribe acks in the sleep run. Such repeats are kept and
  reported as `payload_repeats_kept`.
- An unparseable line is logged as `file:line: error`, counted, listed in the day report and
  skipped. The run continues and exits with status 1.
- `data/reports/typed/<day>.json`: per stream `lines_in`, `records_out`, `duplicates_removed`,
  `payload_repeats_kept`, `parse_failures`, and `gaps` with `count`, `total_ns`/`total_ms`
  (sum) and `covered_ns`/`covered_ms` (union, overlaps counted once), split by reason. A gap
  crossing UTC midnight is split between the two days and counted on each.
- Not yet exercised by recorded data: the Binance `mark_price`, `premium_index`,
  `funding_rate_history` and `kline_batch` parsers. No recorded payloads for them exist in the repo
  (the premium-index fixture is synthetic). Their field types (`st`, `ap` included) are
  taken from the docs and the live notes above, not verified.

## Endpoints used

| venue | endpoint | params |
|---|---|---|
| Mudrex REST | `GET https://trade.mudrex.com/fapi/v1/price/kline` | `assets=BTC/USDT&aggregation=1m&start_time=<s>&end_time=<s>` |
| Mudrex REST | `GET https://trade.mudrex.com/fapi/v1/price/mark-kline` | same |
| Mudrex WS | `wss://trade.mudrex.com/fapi/v1/price/ws/linear` | `{"id":1,"method":"SUBSCRIBE","params":["kline@1m@btcusdt","markKline@1m@btcusdt"]}` and `{"id":2,"method":"SUBSCRIBE","params":["ticker@1s"],"assets":["btcusdt"]}` |
| Binance REST | `GET https://fapi.binance.com/fapi/v1/depth` | `symbol=BTCUSDT&limit=1000` |
| Binance REST | `GET /fapi/v1/premiumIndex` | `symbol=BTCUSDT` |
| Binance REST | `GET /fapi/v1/fundingRate` | `symbol=BTCUSDT&limit=100` |
| Binance REST | `GET /fapi/v1/klines` | `symbol=BTCUSDT&interval=1m&limit=5` |
| Binance WS | `wss://fstream.binance.com/public/stream` | `{"method":"SUBSCRIBE","params":["btcusdt@depth@100ms"],"id":1}` |
| Binance WS | `wss://fstream.binance.com/market/stream` | `{"method":"SUBSCRIBE","params":["btcusdt@markPrice@1s"],"id":2}` |

## Rate limits and reliability

| limit | value | how it is enforced |
|---|---|---|
| Mudrex REST | 300 req/min/IP (documented) | sliding-window limiter; the recorder uses about 2 req/min |
| Mudrex WS | 15 subscriptions per connection, 10 new connections/min/IP (documented) | config validation fails above 15; connect limiter at 10/60s |
| Mudrex idle close | 40s idle (documented) | protocol PING frame every 20s; reconnect if no data for 60s |
| Binance REST weight | 2400/min/IP (**unverified**, see below) | weight limiter set to 1200/60s (half). Weights: depth(1000)=20, others=1, about 50 weight/min in use |
| Binance fundingRate | shared 500 req / 5 min / IP (**unverified**) | separate limiter |
| Binance WS connections | per-IP connection rate (**unverified**) | 5 connects/s limiter; only 2 connections are used |

Reliability:

- Every REST call has a 10s timeout.
- HTTP 429 and 418 honor `Retry-After` (delta-seconds or HTTP-date) and pause every caller sharing
  that venue's limiter.
- With no `Retry-After`, and for 5xx and timeouts, retries use backoff. Other 4xx responses are not
  retried.
- After `http_max_retries`, the failure is written as an `error` record.
- WS reconnects use exponential backoff with jitter (`[c/2, c]`, `c = min(60, base·2^n)`), capped
  at 60s. The backoff resets once data flows again.
- A connection that answers pings but sends no data is recycled after `ws_stale_timeout_s`. This
  uses an outer deadline, because aiohttp's per-read timeout restarts on every pong.
- Host sleep (for example, a laptop lid or idle sleep) pauses the process, and the venue drops the
  socket. The recorder reconnects and writes a `gap` covering the sleep, as observed in dev.
  `--duration` uses the wall clock, because the macOS monotonic clock stops during sleep. For long
  runs on a Mac, use `caffeinate -ims make run`.
- SIGINT and SIGTERM cancel all tasks, log WS disconnects, flush and close files, and close the
  HTTP session.

## Known doc mismatches and findings

Checked on 2026-11-05. The Mudrex docs are at https://docs.trade.mudrex.com/docs/market-data.md
and https://docs.trade.mudrex.com/docs/websocket-streams.md (pages updated 2026-06-26).

### Mudrex (brief vs docs vs live behavior)

1. **REST parameter names.**
   - The docs use `assets` (comma-separated, max 25), `aggregation`, and required
     `start_time`/`end_time`, both inclusive and in epoch seconds.
   - There is no `symbol`, `interval` or `limit` parameter. Sending `symbol=` returns HTTP 400
     `assets are required` (verified live).
   - Responses key assets in lowercase (`btc/usdt`).
   - Candle arrays are `[t,o,h,l,c,v]` for kline and `[t,o,h,l,c]` for mark-kline.
2. **REST intervals.**
   - Documented as `1m, 3t, 5t, 10t, 15t, 30t, 1h, …`. The `t` suffix is not defined, and is
     probably minutes.
   - The WS supports only `1s` and `1m`.
3. **Mudrex PING.**
   - The docs say "send PING frames" without defining a payload.
   - Live: a text `PING` returns `invalid JSON`, and `{"method":"PING"}` returns `unknown method`.
   - **WebSocket protocol ping frames** work, and that is what the recorder sends.
4. **Ticker subscription.**
   - Needs a separate `assets` field (`"params":["ticker@1s"],"assets":["btcusdt"]`).
   - `data` is an array of `{s, p, mp}`. Only assets whose price changed are pushed.
5. **WS klines** push once per minute with the just-closed candle only. There are no in-progress
   updates (observed live).
6. **No exchange timestamps** on any Mudrex WS frame or REST response.
7. **Undocumented in the brief:** the REST limit of 300 req/min/IP. REST and WS limits are tracked
   separately.
8. **Path inconsistency in the docs.** The Market Data index lists `GET /price/kline`, while the
   per-endpoint curl examples use `/fapi/v1/price/kline`. The full path is used here and was
   verified live.

### Binance

1. **Reachability depends on the network.**
   - Earlier on 2026-11-05, `fapi.binance.com`, `fstream.binance.com` and
     `developers.binance.com` resolved to an ISP block address and timed out.
   - Later that day, on the system resolver, they resolved normally and every endpoint worked.
   - **No attempt was ever made to bypass the block** (no alternate DNS, VPN or proxy). Whether,
     and from where, to record Binance is a compliance decision.
2. **Book-sync rule: USD-M differs from spot.**
   - The official USD-M page "How to manage a local order book correctly" (read 2026-11-06)
     requires the first processed event to satisfy **`U <= lastUpdateId AND u >= lastUpdateId`**.
   - That is not the spot form `U <= lastUpdateId+1 <= u`. The recorder implements the documented
     USD-M rule.
   - The same page drops events with `u < lastUpdateId` and requires `pu` to equal the previous
     `u`.
   - It also names `wss://fstream.binance.com/public/stream` for depth, which confirms the split
     below.
3. **WS endpoint split (differs from the brief).**
   - Per secondary sources (see below), Binance split USD-M market streams on 2026-03-06:
     `wss://fstream.binance.com/public` for high-frequency data (`depth`, `bookTicker`) and
     `wss://fstream.binance.com/market` for the rest (`markPrice`, `kline`, `aggTrade`, …).
   - The **legacy `wss://fstream.binance.com/ws` was retired on 2026-04-23**.
   - The recorder uses `/public/stream` for `btcusdt@depth@100ms` and `/market/stream` for
     `btcusdt@markPrice@1s`.
   - Both URLs are in config. Both connected and streamed live on 2026-11-05. The `/public` path
     is also named on the official order-book page.
   - Sources:
     - [sieblyio/binance v3.5.0](https://github.com/sieblyio/binance/releases/tag/v3.5.0). The
       v3.6.11 package source routes `depth`/`bookTicker` to `/public/stream` and all other
       streams to `/market/stream`.
     - [ccxt/go-binance#809](https://github.com/ccxt/go-binance/pull/809).
     - [Binance derivatives change log](https://developers.binance.com/docs/derivatives/change-log).
4. **Extra fields seen live.**
   - Mark price frames carry `ap` (per a 2026-03-16 change log entry) and `st`.
   - Depth frames carry `ps` and `st`, which are not in the brief.
   - Raw recording keeps them all.
5. **Depth volume.** `btcusdt@depth@100ms` produced about 9.6 frames/s, roughly 90 MB/hour or
   2 GB/day uncompressed. Mudrex BTC is about 1 MB/hour.
6. **Unverified numbers.** These values come from prior knowledge, not from docs read today: REST
   weight limit 2400/min, depth weight 20 at `limit=1000`, the fundingRate 500/5min/IP limit, and
   the WS connection-rate and ping/pong rules. They are in config, set conservatively, and must be
   confirmed against the official docs. Live `X-MBX-USED-WEIGHT-1M` readings of 22–24 per minute
   are consistent with a depth weight of 20.
