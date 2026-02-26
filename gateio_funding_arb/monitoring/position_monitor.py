"""Position monitoring with PnL tracking and daily loss limit enforcement."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from gateio_funding_arb.utils.safety_checks import SafetyChecker


class PositionMonitor:
    """Continuously monitor open positions and trigger exits when needed."""

    def __init__(self, client, config, safety: SafetyChecker,
                 logger: logging.Logger | None = None) -> None:
        self.client = client
        self.config = config
        self.safety = safety
        self.exchange_name = config.name
        self.logger = logger or logging.getLogger(f"pos_mon.{self.exchange_name}")
        self.positions: list[dict[str, Any]] = []
        self.monitoring_active = False

    def add_position(self, position: dict[str, Any]) -> None:
        self.positions.append(position)
        self.logger.info(f"[{self.exchange_name}] Monitoring started for {position['symbol']}")

    def remove_position(self, symbol: str) -> None:
        self.positions = [p for p in self.positions if p["symbol"] != symbol]
        self.logger.info(f"[{self.exchange_name}] Monitoring stopped for {symbol}")

    async def check_funding_rate(self, position: dict[str, Any]) -> bool:
        """Returns True if funding rate has normalized (exit signal)."""
        symbol = position["symbol"]
        funding = await self.client.get_funding_rate(symbol)
        current_rate = abs(funding["daily_rate"])

        if current_rate < self.config.risk.funding_exit_threshold:
            self.logger.warning(
                f"[{self.exchange_name}] ⚠️ Funding normalized for {symbol}: "
                f"{funding['daily_rate']:.2f}% < {self.config.risk.funding_exit_threshold}%"
            )
            return True
        return False

    async def check_hold_time(self, position: dict[str, Any]) -> bool:
        elapsed = time.time() - position["entry_time"]
        return elapsed >= self.config.risk.min_hold_time_seconds

    async def calculate_pnl(self, position: dict[str, Any]) -> dict[str, float]:
        """Calculate current PnL for a position."""
        symbol = position["symbol"]
        
        # 1. Fetch live prices for unrealized mark PnL
        spot_price, futures_price = await asyncio.gather(
            self.client.get_spot_price(symbol),
            self.client.get_futures_price(symbol),
        )

        if position["strategy"] == "positive_carry":
            spot_pnl = (spot_price - position["spot_entry_price"]) * position["spot_qty"]
            # Mark-based futures PnL
            futures_mark_pnl = (position["futures_entry_price"] - futures_price) * position["futures_qty"]
        else:
            spot_pnl = (position["spot_entry_price"] - spot_price) * position["borrow_qty"]
            futures_mark_pnl = (futures_price - position["futures_entry_price"]) * position["futures_qty"]

        # 2. Fetch actual exchange position to get Funding and Fee PnL
        pnl_fund = 0.0
        pnl_fee = 0.0
        try:
            live_positions = await self.client.futures_exchange.fetch_positions([symbol])
            if live_positions:
                ex_pos = live_positions[0]
                # ccxt pulls from Gate.io raw response
                pnl_fund = float(ex_pos['info'].get('pnl_fund', 0))
                pnl_fee = float(ex_pos['info'].get('pnl_fee', 0))
        except Exception as e:
            self.logger.warning(f"[{self.exchange_name}] Failed to fetch live funding PnL for {symbol}: {e}")

        # Total PnL now includes actual exchange fees and funding payouts!
        total_pnl = spot_pnl + futures_mark_pnl + pnl_fund + pnl_fee
        pnl_pct = (total_pnl / position["size_usd"]) * 100 if position["size_usd"] > 0 else 0

        return {
            "spot_pnl": spot_pnl,
            "futures_pnl": futures_mark_pnl + pnl_fund + pnl_fee,  # Include it here for total calculations
            "funding_fee": pnl_fund,
            "trading_fee": pnl_fee,
            "total_pnl": total_pnl,
            "pnl_percent": pnl_pct,
        }

    async def monitor_position(self, position: dict[str, Any]) -> dict[str, Any]:
        """Monitor single position. Returns status dict."""
        pnl = await self.calculate_pnl(position)

        # Track incremental PnL for daily loss limit
        last_pnl = float(position.get("last_total_pnl", 0.0))
        pnl_delta = pnl["total_pnl"] - last_pnl
        position["last_total_pnl"] = pnl["total_pnl"]

        if not self.safety.update_pnl(pnl_delta):
            return {"should_close": True, "reason": "Daily loss limit exceeded", "pnl": pnl}

        if not await self.check_hold_time(position):
            return {"should_close": False, "reason": "Min hold time not met", "pnl": pnl}

        if await self.check_funding_rate(position):
            return {"should_close": True, "reason": "Funding rate normalized", "pnl": pnl}

        return {"should_close": False, "reason": "Position healthy", "pnl": pnl}

    async def monitoring_loop(self) -> None:
        """Main monitoring loop."""
        self.monitoring_active = True
        interval = self.config.scan.position_check_interval_seconds
        self.logger.info(f"[{self.exchange_name}] Position monitoring loop started")

        while self.monitoring_active:
            try:
                if not self.positions:
                    await asyncio.sleep(interval)
                    continue

                self.logger.info(
                    f"[{self.exchange_name}] Monitoring {len(self.positions)} positions..."
                )

                for position in self.positions[:]:
                    status = await self.monitor_position(position)
                    pnl = status["pnl"]
                    self.logger.info(
                        f"[{self.exchange_name}] {position['symbol']}: "
                        f"PnL=${pnl['total_pnl']:.2f} ({pnl['pnl_percent']:+.2f}%) "
                        f"Status: {status['reason']}"
                    )
                    position["last_status"] = status

                await asyncio.sleep(interval)

            except Exception as e:
                self.logger.error(f"[{self.exchange_name}] Monitoring error: {e}")
                await asyncio.sleep(interval)

    def stop(self) -> None:
        self.monitoring_active = False
        self.logger.info(f"[{self.exchange_name}] Position monitoring stopped")
