"""Pre-trade safety validation checks with PnL tracking."""

from __future__ import annotations

import logging
from typing import Any

from gateio_funding_arb.config import ExchangeConfig


class SafetyChecker:
    """Validates trades and tracks daily PnL per exchange."""

    def __init__(self, config: ExchangeConfig, logger: logging.Logger | None = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(f"safety.{config.name}")
        self.daily_pnl: float = 0.0
        self.starting_equity: float = 0.0
        self._daily_loss_exceeded: bool = False
        self.active_positions: list[dict[str, Any]] = []

    def set_starting_equity(self, equity: float) -> None:
        self.starting_equity = equity
        self.daily_pnl = 0.0
        self._daily_loss_exceeded = False
        self.logger.info(f"Starting equity set to ${equity:,.2f}")

    def is_within_daily_loss_limit(self) -> bool:
        return not self._daily_loss_exceeded

    def is_cautious_mode(self) -> bool:
        if self.starting_equity <= 0:
            return False
        pnl_pct = (self.daily_pnl / self.starting_equity) * 100
        return pnl_pct <= -self.config.risk.cautious_mode_trigger_percent

    def update_pnl(self, pnl_change: float) -> bool:
        """Update daily PnL. Returns False if daily loss limit exceeded."""
        self.daily_pnl += pnl_change
        pnl_pct = (self.daily_pnl / self.starting_equity * 100) if self.starting_equity > 0 else 0

        if pnl_change != 0:
            self.logger.info(f"Daily PnL: ${self.daily_pnl:,.2f} ({pnl_pct:+.2f}%)")

        if pnl_pct <= -self.config.risk.daily_loss_limit_percent:
            self._daily_loss_exceeded = True
            self.logger.critical(
                f"⚠️ DAILY LOSS LIMIT EXCEEDED: {pnl_pct:.2f}% "
                f"(limit: -{self.config.risk.daily_loss_limit_percent}%)"
            )
            return False
        return True

    # ── trade validation ────────────────────────────────────────────

    def check_position_limits(self, symbol: str, size_usd: float) -> tuple[bool, str]:
        if size_usd > self.config.position.max_position_size_usd:
            return False, (
                f"Position size ${size_usd:.2f} exceeds max "
                f"${self.config.position.max_position_size_usd:.2f}"
            )
        if len(self.active_positions) >= self.config.position.max_concurrent_positions:
            return False, (
                f"Already at max concurrent positions "
                f"({self.config.position.max_concurrent_positions})"
            )
        if any(p["symbol"] == symbol for p in self.active_positions):
            return False, f"Already have an active position for {symbol}"
        if self.starting_equity > 0:
            pct = (size_usd / self.starting_equity) * 100
            
            max_pct = self.config.position.max_position_size_percent
            if self.is_cautious_mode():
                max_pct = self.config.risk.cautious_max_position_size_percent
                
            if pct > max_pct:
                return False, (
                    f"Position {pct:.1f}% of equity exceeds max {max_pct}% "
                    f"({'CAUTIOUS MODE' if self.is_cautious_mode() else 'normal'})"
                )
        return True, "Position size checks passed"

    def check_spread(self, spot_price: float, futures_price: float) -> tuple[bool, str]:
        spread_pct = abs(futures_price - spot_price) / spot_price * 100
        max_sp = self.config.thresholds.max_spread_percent
        if spread_pct > max_sp:
            return False, f"Spread too wide: {spread_pct:.4f}% (max: {max_sp}%)"
        return True, f"Spread check passed: {spread_pct:.4f}%"

    def check_blacklist(self, symbol: str) -> tuple[bool, str]:
        base = symbol.replace("USDT", "").replace("/USDT", "").replace("_USDT", "").replace(":USDT", "")
        for bl in self.config.filters.blacklist:
            bl_base = bl.replace("USDT", "").replace("/USDT", "").replace("_USDT", "").replace(":USDT", "")
            if base == bl_base:
                return False, f"{symbol} is blacklisted"
        return True, "Not blacklisted"

    def check_whitelist(self, symbol: str) -> tuple[bool, str]:
        if not self.config.filters.whitelist_enabled:
            return True, "Whitelist disabled"
        if symbol in self.config.filters.whitelist:
            return True, "Symbol in whitelist"
        return False, f"{symbol} not in whitelist"

    def validate_trade(
        self,
        symbol: str,
        size_usd: float,
        spot_price: float,
        futures_price: float,
    ) -> tuple[bool, list[str]]:
        """Run all safety checks. Returns (is_valid, messages)."""
        messages: list[str] = []
        is_valid = True

        if self._daily_loss_exceeded:
            return False, ["BLOCKED: Daily loss limit exceeded"]

        for check_fn, args in [
            (self.check_blacklist, (symbol,)),
            (self.check_whitelist, (symbol,)),
            (self.check_position_limits, (symbol, size_usd)),
            (self.check_spread, (spot_price, futures_price)),
        ]:
            valid, msg = check_fn(*args)
            if not valid:
                self.logger.warning(f"Safety reject {symbol}: {msg}")
                messages.append(msg)
                is_valid = False

        return is_valid, messages

    # ── position tracking ───────────────────────────────────────────

    def add_position(self, position: dict[str, Any]) -> None:
        self.active_positions.append(position)
        self.logger.info(
            f"Position added: {position['symbol']} "
            f"(Active: {len(self.active_positions)}/{self.config.position.max_concurrent_positions})"
        )

    def remove_position(self, symbol: str) -> None:
        self.active_positions = [p for p in self.active_positions if p["symbol"] != symbol]
        self.logger.info(f"Position removed: {symbol} (Active: {len(self.active_positions)})")
