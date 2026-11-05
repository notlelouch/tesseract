import json
from pathlib import Path

from tesseract.config import load_config
from tesseract.recorder import binance, mudrex

from conftest import fixture_lines, fixture_text

ROOT = Path(__file__).parent.parent


def test_mudrex_subscribe_messages():
    cfg = load_config(ROOT / "config.toml").mudrex
    msgs = mudrex.subscribe_messages(cfg)
    assert msgs[0] == {"id": 1, "method": "SUBSCRIBE", "params": ["kline@1m@btcusdt", "markKline@1m@btcusdt"]}
    assert msgs[1] == {"id": 2, "method": "SUBSCRIBE", "params": ["ticker@1s"], "assets": ["btcusdt"]}


def test_mudrex_fixtures_shape():
    k = json.loads(fixture_text("mudrex_rest_kline.json"))
    assert k["success"] and all(len(c) == 6 for c in k["data"]["asset_ticks"]["btc/usdt"])
    mk = json.loads(fixture_text("mudrex_rest_mark_kline.json"))
    assert all(len(c) == 5 for c in mk["data"]["asset_ticks"]["btc/usdt"])
    for f in fixture_lines("mudrex_ws_frames.jsonl"):
        assert mudrex.ws_exch_ts(json.loads(f)) is None


def test_binance_exchange_timestamps():
    frames = [json.loads(f) for f in fixture_lines("binance_ws_frames.jsonl")]
    assert binance.ws_exch_ts(frames[0]) is None  # subscribe ack
    assert binance.ws_exch_ts(frames[1]) == 1793894300123
    assert binance.rest_exch_ts("depth", fixture_text("binance_rest_depth.json")) == 1589436922972
    assert binance.rest_exch_ts("premiumIndex", fixture_text("binance_rest_premium_index.json")) == 1597370495002
    assert binance.rest_exch_ts("klines", "[[1,2]]") is None
    assert binance.rest_exch_ts("depth", "not json") is None
