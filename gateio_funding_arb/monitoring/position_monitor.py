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

    def required_hold_seconds(self) -> int:
        return max(
            int(self.config.risk.min_hold_time_seconds),
            int(self.config.risk.funding_interval_seconds),
        )

    def _buffer_blockers(self, pnl_dict: dict[str, float]) -> list[str]:
        blockers: list[str] = []
        profit_buffer = float(self.config.risk.profit_buffer_usd)
        total_pnl = float(pnl_dict.get("total_pnl", 0.0))
        funding_fee = float(pnl_dict.get("funding_fee", 0.0))

        if total_pnl <= profit_buffer:
            blockers.append(f"net PnL ${total_pnl:.2f} <= ${profit_buffer:.2f}")
        if self.config.risk.require_funding_fee_buffer and funding_fee < profit_buffer:
            blockers.append(f"funding ${funding_fee:.4f} < ${profit_buffer:.2f}")
        return blockers

    def _stale_close_reason(self, position: dict[str, Any], pnl_dict: dict[str, float]) -> str | None:
        if not getattr(self.config.risk, "stale_recycle_enabled", False):
            return None

        blockers = self._buffer_blockers(pnl_dict)
        if not blockers:
            return None

        funding_interval_seconds = max(1, int(self.config.risk.funding_interval_seconds))
        max_windows = max(1, int(self.config.risk.max_funding_windows_to_profit))
        elapsed_seconds = max(0.0, time.time() - float(position["entry_time"]))
        elapsed_windows = elapsed_seconds / funding_interval_seconds
        if elapsed_windows < max_windows:
            return None

        profit_buffer = float(self.config.risk.profit_buffer_usd)
        return (
            f"Stale position recycled after {elapsed_windows:.1f} funding windows without clearing "
            f"${profit_buffer:.2f} buffer ({'; '.join(blockers)})"
        )

    async def check_funding_rate(self, position: dict[str, Any], pnl_dict: dict[str, float]) -> bool:
        """Returns True if funding rate has normalized AND position is profitable."""
        symbol = position["symbol"]
        funding = await self.client.get_funding_rate(symbol)
        current_rate = abs(funding["daily_rate"])

        if current_rate < self.config.risk.funding_exit_threshold:
            blockers = self._buffer_blockers(pnl_dict)
            if not blockers:
                profit_buffer = float(self.config.risk.profit_buffer_usd)
                self.logger.warning(
                    f"[{self.exchange_name}] ⚠️ Funding normalized for {symbol}: "
                    f"{current_rate:.2f}% < {self.config.risk.funding_exit_threshold}%, "
                    f"and position cleared the ${profit_buffer:.2f} profit buffer. Exiting."
                )
                return True
            self.logger.info(
                f"[{self.exchange_name}] Funding normalized for {symbol}: "
                f"{current_rate:.2f}% < {self.config.risk.funding_exit_threshold}%, "
                f"BUT funding-profit buffer not met ({'; '.join(blockers)}). Holding."
            )
        return False

    async def check_target_roi(self, position: dict[str, Any], pnl_dict: dict[str, float]) -> bool:
        target = self.config.risk.target_roi_percent
        if self.safety and self.safety.is_cautious_mode():
            target = self.config.risk.cautious_target_roi_percent

        pnl_pct = float(pnl_dict.get("pnl_percent", 0.0))
        if pnl_pct >= target:
            blockers = self._buffer_blockers(pnl_dict)
            if blockers:
                self.logger.info(
                    f"[{self.exchange_name}] {position['symbol']}: ROI +{pnl_pct:.2f}% reached "
                    f"but funding-profit buffer not met ({'; '.join(blockers)}). Holding."
                )
                return False
            self.logger.info(
                f"[{self.exchange_name}] {position['symbol']}: PnL=${pnl_dict.get('total_pnl', 0):.2f} "
                f"(+{pnl_pct:.2f}%) Status: Target ROI reached "
                f"{'[CAUTIOUS]' if self.safety and self.safety.is_cautious_mode() else ''}"
            )
            return True
        return False

    async def check_hold_time(self, position: dict[str, Any]) -> bool:
        elapsed = time.time() - position["entry_time"]
        return elapsed >= self.required_hold_seconds()

    async def calculate_pnl(self, position: dict[str, Any]) -> dict[str, float]:
        """Calculate current PnL for a position."""
        symbol = position["symbol"]
        
        # 1. Fetch live prices for realizable mark PnL
        spot_ticker, futures_ticker = await asyncio.gather(
            self.client.get_spot_ticker(symbol),
            self.client.get_futures_ticker(symbol),
        )

        if position["strategy"] == "positive_carry":
            # Exit positive carry: Sell spot (to bid), Buy futures (from ask)
            spot_price = float(spot_ticker.get("bid") or spot_ticker.get("last"))
            futures_price = float(futures_ticker.get("ask") or futures_ticker.get("last"))
            
            spot_pnl = (spot_price - position["spot_entry_price"]) * position["spot_qty"]
            # Mark-based futures PnL explicitly using base_qty
            futures_base_qty = position.get("futures_base_qty", position["spot_qty"])
            futures_mark_pnl = (position["futures_entry_price"] - futures_price) * futures_base_qty
        else:
            # Exit reverse carry: Buy spot (from ask), Sell futures (to bid)
            spot_price = float(spot_ticker.get("ask") or spot_ticker.get("last"))
            futures_price = float(futures_ticker.get("bid") or futures_ticker.get("last"))
            
            spot_pnl = (position["spot_entry_price"] - spot_price) * position["borrow_qty"]
            futures_base_qty = position.get("futures_base_qty", position["borrow_qty"])
            futures_mark_pnl = (futures_price - position["futures_entry_price"]) * futures_base_qty

        # 2. Fetch exchange-reported funding/fee income since entry.
        pnl_fund = 0.0
        pnl_fee = 0.0
        try:
            income = await self.client.get_position_income_summary(
                symbol,
                position.get("entry_time"),
            )
            pnl_fund = float(income.get("funding_fee", 0.0))
            pnl_fee = float(income.get("trading_fee", 0.0))
        except Exception as e:
            self.logger.warning(f"[{self.exchange_name}] Failed to fetch live funding PnL for {symbol}: {e}")

        # Total PnL now includes actual exchange fees and funding payouts.
        # Deduct round-trip fees (entry est_fees * 2) from live tracking so Target ROI strictly triggers on NET profit.
        round_trip_fees = position.get("est_fees", 0.0) * 2
        total_pnl = spot_pnl + futures_mark_pnl + pnl_fund + pnl_fee - round_trip_fees
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

        required_hold = self.required_hold_seconds()
        elapsed = time.time() - position["entry_time"]
        if elapsed < required_hold:
            return {
                "should_close": False,
                "reason": (
                    f"Waiting for funding window "
                    f"({elapsed / 3600:.2f}h < {required_hold / 3600:.2f}h)"
                ),
                "pnl": pnl,
            }

        stale_reason = self._stale_close_reason(position, pnl)
        if stale_reason:
            return {"should_close": True, "reason": stale_reason, "pnl": pnl}

        if await self.check_funding_rate(position, pnl):
            return {"should_close": True, "reason": "Funding rate normalized", "pnl": pnl}

        if await self.check_target_roi(position, pnl):
            self.logger.info(f"[{self.exchange_name}] 🎯 Target ROI reached for {position['symbol']}: +{pnl['pnl_percent']:.2f}%")
            return {"should_close": True, "reason": "Target ROI reached", "pnl": pnl}

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
