from pathlib import Path

import pytest

from tesseract.config import ConfigError, load_config

ROOT = Path(__file__).parent.parent


def test_shipped_configs_load():
    for name in ("config.toml", "config.dev.toml"):
        cfg = load_config(ROOT / name)
        assert cfg.backoff.cap_s <= 60
        assert cfg.general.http_timeout_s == 10


def test_secret_like_keys_rejected(tmp_path):
    text = (ROOT / "config.toml").read_text().replace("[mudrex]\n", '[mudrex]\napi_key = "x"\n', 1)
    p = tmp_path / "c.toml"
    p.write_text(text)
    with pytest.raises(ConfigError):
        load_config(p)


def test_mudrex_subscription_limit(tmp_path):
    syms = ", ".join(f'"s{i}usdt"' for i in range(8))  # 16 kline streams + ticker > 15
    text = (ROOT / "config.toml").read_text().replace('ws_symbols = ["btcusdt"]', f"ws_symbols = [{syms}]")
    p = tmp_path / "c.toml"
    p.write_text(text)
    with pytest.raises(ConfigError, match="15"):
        load_config(p)
