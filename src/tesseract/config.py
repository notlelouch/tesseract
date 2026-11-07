"""Load and validate the recorder config (TOML). No secrets are read or accepted."""

from __future__ import annotations

import tomllib
from decimal import Decimal
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class GeneralConfig:
    data_dir: str
    log_level: str
    flush_interval_s: float
    fsync: bool
    max_buffer_records: int
    http_timeout_s: float
    http_max_retries: int
    user_agent: str


@dataclass(frozen=True)
class BackoffConfig:
    base_s: float
    cap_s: float


@dataclass(frozen=True)
class MudrexConfig:
    enabled: bool
    rest_base: str
    ws_url: str
    rest_assets: list[str]
    ws_symbols: list[str]
    kline_interval: str
    ticker_stream: str
    rest_poll_interval_s: float
    rest_lookback_s: int
    ws_ping_interval_s: float
    ws_stale_timeout_s: float
    rest_rate_limit: int
    rest_rate_window_s: float
    ws_connect_limit: int
    ws_connect_window_s: float
    ws_max_subscriptions: int

    def ws_streams(self) -> list[str]:
        streams: list[str] = []
        for sym in self.ws_symbols:
            streams.append(f"kline@{self.kline_interval}@{sym}")
            streams.append(f"markKline@{self.kline_interval}@{sym}")
        streams.append(self.ticker_stream)
        return streams


@dataclass(frozen=True)
class BinanceConfig:
    enabled: bool
    rest_base: str
    symbol: str
    ws_public_url: str
    ws_public_streams: list[str]
    ws_market_url: str
    ws_market_streams: list[str]
    ws_ping_interval_s: float
    ws_stale_timeout_s: float
    depth_snapshot_interval_s: float
    depth_resync_delay_s: float
    depth_limit: int
    premium_index_interval_s: float
    funding_rate_interval_s: float
    funding_rate_limit: int
    klines_interval: str
    klines_poll_interval_s: float
    klines_limit: int
    weight_limit: int
    weight_window_s: float
    funding_rate_ip_limit: int
    funding_rate_ip_window_s: float
    ws_connect_limit: int
    ws_connect_window_s: float
    weights: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class Config:
    general: GeneralConfig
    backoff: BackoffConfig
    mudrex: MudrexConfig
    binance: BinanceConfig


_SECRET_HINTS = ("key", "secret", "token", "password", "passphrase")


def _build(cls: type, section: str, raw: dict[str, Any]) -> Any:
    known = {f.name for f in fields(cls)}
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(f"[{section}] unknown keys: {sorted(unknown)}")
    for k in raw:
        if any(h in k.lower() for h in _SECRET_HINTS):
            raise ConfigError(f"[{section}] key {k!r} looks like a secret; the recorder takes none")
    missing = [f.name for f in fields(cls) if f.name not in raw and f.name != "weights"]
    if missing:
        raise ConfigError(f"[{section}] missing keys: {missing}")
    return cls(**raw)


def _positive(section: str, name: str, value: float) -> None:
    if value <= 0:
        raise ConfigError(f"[{section}] {name} must be > 0, got {value}")


def load_config(path: str | Path) -> Config:
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    for s in ("general", "backoff", "mudrex", "binance"):
        if s not in raw:
            raise ConfigError(f"missing section [{s}]")
    cfg = Config(
        general=_build(GeneralConfig, "general", raw["general"]),
        backoff=_build(BackoffConfig, "backoff", raw["backoff"]),
        mudrex=_build(MudrexConfig, "mudrex", raw["mudrex"]),
        binance=_build(BinanceConfig, "binance", raw["binance"]),
    )
    validate(cfg)
    return cfg


def validate(cfg: Config) -> None:
    g, m, b = cfg.general, cfg.mudrex, cfg.binance
    _positive("general", "flush_interval_s", g.flush_interval_s)
    _positive("general", "http_timeout_s", g.http_timeout_s)
    _positive("backoff", "base_s", cfg.backoff.base_s)
    _positive("backoff", "cap_s", cfg.backoff.cap_s)
    if cfg.backoff.cap_s > 60:
        raise ConfigError("[backoff] cap_s must be <= 60")
    if m.enabled:
        if len(m.rest_assets) > 25:
            raise ConfigError("[mudrex] rest_assets: max 25 assets per call")
        if len(m.ws_streams()) > m.ws_max_subscriptions:
            raise ConfigError(
                f"[mudrex] {len(m.ws_streams())} WS subscriptions exceeds "
                f"{m.ws_max_subscriptions} per connection"
            )
        if m.kline_interval not in ("1s", "1m"):
            raise ConfigError("[mudrex] kline_interval must be 1s or 1m (WS supports only these)")
        if m.rest_lookback_s // 60 + 1 > 1440:
            raise ConfigError("[mudrex] rest_lookback_s exceeds 1440 one-minute candles")
        if m.ws_ping_interval_s >= 40:
            raise ConfigError("[mudrex] ws_ping_interval_s must be < 40 (server idle timeout)")
        _positive("mudrex", "rest_poll_interval_s", m.rest_poll_interval_s)
    if b.enabled:
        for name in ("depth", "premiumIndex", "fundingRate", "klines"):
            if name not in b.weights:
                raise ConfigError(f"[binance.weights] missing weight for {name}")
        if b.depth_limit not in (5, 10, 20, 50, 100, 500, 1000):
            raise ConfigError("[binance] depth_limit must be one of 5,10,20,50,100,500,1000")
        _positive("binance", "depth_snapshot_interval_s", b.depth_snapshot_interval_s)


# --------------------------------------------------------------------------- paper fill model


@dataclass(frozen=True)
class PaperConfig:
    """[paper] section. Decimals are read from TOML as exact Decimal, never float."""

    taker_fee: Decimal  # fraction of filled notional, e.g. 0.0005 = 0.05%
    latency_ms: int  # decision -> fill delay
    fallback_spread_bps: Decimal  # full quoted spread assumed when no book covers the fill time
    fallback_slippage_bps: Decimal  # extra adverse move assumed on top of half the spread


def load_paper_config(path: str | Path) -> PaperConfig:
    with open(path, "rb") as f:
        raw = tomllib.load(f, parse_float=Decimal)
    if "paper" not in raw:
        raise ConfigError("missing section [paper]")
    p = raw["paper"]
    known = {f.name for f in fields(PaperConfig)}
    if set(p) - known:
        raise ConfigError(f"[paper] unknown keys: {sorted(set(p) - known)}")
    if known - set(p):
        raise ConfigError(f"[paper] missing keys: {sorted(known - set(p))}")

    def decimal(name: str) -> Decimal:
        v = p[name]
        if isinstance(v, bool) or not isinstance(v, (int, Decimal)):
            raise ConfigError(f"[paper] {name} must be a number, got {v!r}")
        d = Decimal(v)
        if not d.is_finite() or d < 0:
            raise ConfigError(f"[paper] {name} must be a finite number >= 0, got {v!r}")
        return d

    latency = p["latency_ms"]
    if isinstance(latency, bool) or not isinstance(latency, int) or latency < 0:
        raise ConfigError(f"[paper] latency_ms must be an integer >= 0, got {latency!r}")
    cfg = PaperConfig(taker_fee=decimal("taker_fee"), latency_ms=latency,
                      fallback_spread_bps=decimal("fallback_spread_bps"),
                      fallback_slippage_bps=decimal("fallback_slippage_bps"))
    if cfg.taker_fee >= 1:
        raise ConfigError(f"[paper] taker_fee is a fraction (0.0005 = 0.05%), got {cfg.taker_fee}")
    return cfg
