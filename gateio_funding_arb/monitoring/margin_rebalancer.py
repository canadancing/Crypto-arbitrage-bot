"""Auto-rebalance futures margin to prevent liquidation."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from gateio_funding_arb.config import ExchangeConfig


class MarginRebalancer:
    """Automatically transfer USDT from spot to futures when margin ratio is high."""

    def __init__(self, client, config: ExchangeConfig,
                 logger: logging.Logger | None = None) -> None:
        self.client = client
        self.config = config
        self.exchange_name = config.name
        self.logger = logger or logging.getLogger(f"margin.{self.exchange_name}")

    def _to_float(self, value: Any) -> float:
        try:
            return float(value or 0)
        except Exception:
            return 0.0

    def _account_numbers(self, futures_account: Any) -> tuple[float, float]:
        """Extract total margin balance and maintenance margin across payload shapes."""
        if isinstance(futures_account, list):
            # Some exchanges return an array of balance buckets.
            for row in futures_account:
                if not isinstance(row, dict):
                    continue
                ccy = str(row.get("currency") or row.get("asset") or row.get("ccy") or "").upper()
                if ccy and ccy != "USDT":
                    continue
                total = self._to_float(
                    row.get("total")
                    or row.get("equity")
                    or row.get("balance")
                    or row.get("available")
                )
                maint = self._to_float(
                    row.get("totalMaintMargin")
                    or row.get("totalMaintenanceMargin")
                    or row.get("maintMargin")
                )
                return total, maint
            return 0.0, 0.0

        if not isinstance(futures_account, dict):
            return 0.0, 0.0

        usdt_bucket = futures_account.get("USDT", {})
        if isinstance(usdt_bucket, list):
            usdt_bucket = usdt_bucket[0] if usdt_bucket else {}
        if not isinstance(usdt_bucket, dict):
            usdt_bucket = {}

        total_margin_balance = self._to_float(
            usdt_bucket.get("total")
            or futures_account.get("total", {}).get("USDT", 0)
            or usdt_bucket.get("equity")
        )

        info = futures_account.get("info", {})
        # If info is a list (like Gate.io), find the USDT entry
        if isinstance(info, list):
            for row in info:
                if isinstance(row, dict) and row.get("currency") in ("USDT", "USDC"):
                    info = row
                    break
            else:
                info = {}

        if not isinstance(info, dict):
            info = {}

        total_maint_margin = self._to_float(
            info.get("totalMaintMargin")
            or info.get("totalMaintenanceMargin")
            or info.get("maintenance_margin")
            or usdt_bucket.get("maintMargin")
        )
        return total_margin_balance, total_maint_margin

    async def check_and_rebalance(self, positions: list[dict[str, Any]]) -> bool:
        """Check futures margin ratio and rebalance if needed. Returns True if rebalanced."""
        try:
            if not positions:
                return False

            futures_account = await self.client.get_futures_account()
            total_margin_balance, total_maint_margin = self._account_numbers(futures_account)

            # Gate.io unified accounts put cross margin data differently; use true equity estimate
            if self.exchange_name == "gateio" and self.config.unified_account:
                total_margin_balance = await self.client.get_total_equity()

            if total_margin_balance == 0:
                return False

            margin_ratio = (total_maint_margin / total_margin_balance) * 100
            threshold = self.config.risk.margin_ratio_threshold

            self.logger.info(
                f"[{self.exchange_name}] Margin risk: {margin_ratio:.2f}% "
                f"(Maint: ${total_maint_margin:.2f} / Equity: ${total_margin_balance:.2f}) "
                f"| Threshold: {threshold}%"
            )

            if margin_ratio >= threshold:
                self.logger.warning(
                    f"[{self.exchange_name}] ⚠️ Margin risk {margin_ratio:.2f}% "
                    f"exceeds threshold {threshold}%"
                )

                target_ratio = self.config.risk.margin_ratio_target / 100
                target_balance = total_maint_margin / target_ratio if target_ratio > 0 else total_margin_balance * 2
                transfer_needed = target_balance - total_margin_balance

                if transfer_needed > 0:
                    spot_balance = await self.client.get_spot_balance()
                    available = spot_balance["free"]

                    if available >= transfer_needed:
                        self.logger.info(
                            f"[{self.exchange_name}] Transferring ${transfer_needed:.2f} "
                            f"USDT spot → futures"
                        )
                        await self.client.transfer_spot_to_futures("USDT", transfer_needed)
                        self.logger.info(
                            f"[{self.exchange_name}] ✅ Margin rebalanced: "
                            f"transferred ${transfer_needed:.2f}"
                        )
                        return True
                    else:
                        self.logger.error(
                            f"[{self.exchange_name}] ⚠️ INSUFFICIENT for rebalance! "
                            f"Need ${transfer_needed:.2f}, have ${available:.2f}. "
                            f"LIQUIDATION RISK!"
                        )
                        return False

            return False

        except Exception as e:
            self.logger.error(f"[{self.exchange_name}] Margin rebalance error: {e}")
            return False

    async def rebalancing_loop(self, get_positions: Callable[[], list[dict]]) -> None:
        """Background loop to check and rebalance margin every 30s."""
        self.logger.info(f"[{self.exchange_name}] Margin rebalancing loop started")
        while True:
            try:
                positions = get_positions()
                if positions:
                    await self.check_and_rebalance(positions)
                await asyncio.sleep(30)
            except Exception as e:
                self.logger.error(f"[{self.exchange_name}] Rebalancing loop error: {e}")
                await asyncio.sleep(30)
