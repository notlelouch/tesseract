# Test fixtures

- `mudrex_*`: recorded from the live public Mudrex endpoints on 2026-11-05 (unmodified payloads).
- `binance_depth_session.jsonl`: **recorded** from live Binance USD-M on 2026-11-05: two REST
  `/fapi/v1/depth?limit=1000` snapshot records and 21 consecutive `btcusdt@depth@100ms` frames,
  unmodified recorder records in receive order. The first snapshot was taken before the WS started
  (genuinely stale); the second is bridged by one of the frames.
- `binance_ws_frames.jsonl`, `binance_rest_depth.json`, `binance_rest_premium_index.json`:
  **synthetic**, hand-written to the documented shapes before Binance was reachable.
