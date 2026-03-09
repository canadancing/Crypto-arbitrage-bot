from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


@dataclass
class Thresholds:
    min_negative_funding_rate_daily: float
    min_positive_funding_rate_daily: float
    min_net_edge_daily: float
    max_spread_percent: float


@dataclass
class PositionConfig:
    max_position_size_usd: float
    max_position_size_percent: float
    max_concurrent_positions: int
    leverage: int


@dataclass
class RiskConfig:
    daily_loss_limit_percent: float
    margin_ratio_threshold: float
    margin_ratio_target: float
    min_hold_time_seconds: int
    funding_interval_seconds: int = 28800
    max_funding_windows_to_profit: int = 6
    funding_exit_threshold: float = 0.02
    profit_buffer_usd: float = 0.30
    require_funding_fee_buffer: bool = False
    stale_recycle_enabled: bool = False
    target_roi_percent: float = 0.50
    cautious_mode_trigger_percent: float = 2.5
    cautious_max_position_size_percent: float = 15.0
    cautious_min_net_edge_daily: float = 15.0
    cautious_target_roi_percent: float = 1.0


@dataclass
class ScanConfig:
    top_coins_to_scan: int
    scan_interval_seconds: int
    position_check_interval_seconds: int
    max_attempts_per_cycle: int


@dataclass
class ExecutionConfig:
    use_limit_orders: bool
    limit_order_offset_percent: float
    order_timeout_seconds: int
    est_fee_percent: float = 0.08


@dataclass
class BorrowConfig:
    enable_reverse_carry: bool
    borrow_snipe_timeout_seconds: int
    borrow_poll_intervals_seconds: list[int]
    borrow_precheck_cooldown_seconds: int
    reverse_partial_fill_enabled: bool = False
    reverse_partial_fill_min_ratio: float = 0.35
    reverse_pause_after_no_inventory_count: int = 12
    reverse_pause_seconds: int = 3600
    prioritize_positive_carry: bool = True


@dataclass
class FilterConfig:
    quote_currency: str
    whitelist_enabled: bool = False
    whitelist: list[str] = field(default_factory=list)
    blacklist: list[str] = field(default_factory=list)


@dataclass
class ExchangeConfig:
    """Per-exchange configuration."""
    name: str
    enabled: bool
    api_key: str
    api_secret: str
    thresholds: Thresholds
    position: PositionConfig
    risk: RiskConfig
    scan: ScanConfig
    execution: ExecutionConfig
    borrow: BorrowConfig
    filters: FilterConfig
    unified_account: bool


@dataclass
class NotificationConfig:
    telegram_enabled: bool
    heartbeat_interval_minutes: int


@dataclass
class DashboardConfig:
    host: str
    port: int


@dataclass
class RuntimeConfig:
    dry_run: bool
    close_positions_on_shutdown: bool
    log_file: str
    paper_equity_usd: float


@dataclass
class AppConfig:
    exchanges: list[ExchangeConfig]
    notifications: NotificationConfig
    dashboard: DashboardConfig
    runtime: RuntimeConfig


def _build_exchange(raw: dict[str, Any]) -> ExchangeConfig:
    """Build an ExchangeConfig from a raw YAML dict, resolving env vars for credentials."""
    api_key = os.getenv(raw.get("env_key", ""), "") or ""
    api_secret = os.getenv(raw.get("env_secret", ""), "") or ""
    return ExchangeConfig(
        name=raw["name"],
        enabled=raw.get("enabled", True),
        api_key=api_key,
        api_secret=api_secret,
        thresholds=Thresholds(**raw["thresholds"]),
        position=PositionConfig(**raw["position"]),
        risk=RiskConfig(**raw["risk"]),
        scan=ScanConfig(**raw["scan"]),
        execution=ExecutionConfig(**raw["execution"]),
        borrow=BorrowConfig(**raw["borrow"]),
        filters=FilterConfig(**raw["filters"]),
        unified_account=raw.get("unified_account", False),
    )


def load_config(config_path: str = "config.yaml", env_path: str = ".env") -> AppConfig:
    load_dotenv(env_path)
    with Path(config_path).open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    exchanges = [_build_exchange(e) for e in raw.get("exchanges", [])]

    return AppConfig(
        exchanges=exchanges,
        notifications=NotificationConfig(**raw["notifications"]),
        dashboard=DashboardConfig(**raw["dashboard"]),
        runtime=RuntimeConfig(**raw["runtime"]),
    )


def as_dict(config: AppConfig) -> dict[str, Any]:
    d = dataclasses.asdict(config)
    # Strip credentials from serialized output
    for ex in d.get("exchanges", []):
        ex.pop("api_key", None)
        ex.pop("api_secret", None)
    return d
