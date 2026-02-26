"""Reverse carry strategy: borrow + sell spot + long futures when funding < 0."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from gateio_funding_arb.config import ExchangeConfig


class ReverseCarryStrategy:
    """
    Reverse Carry Trade:
    1.  Borrow the base asset on margin
    2.  Sell borrowed asset on spot
    3.  Long perpetual futures at equal notional
    4.  Collect negative funding payments (you receive when funding is negative)
    5.  Close: buy back spot, repay borrow, close futures long

    Delta-neutral: spot short (borrowed) + futures long.
    """

    def __init__(self, client, config: ExchangeConfig, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.config = config
        self.exchange_name = config.name
        self.logger = logger or logging.getLogger(f"rev_carry.{self.exchange_name}")

        # Borrow cooldown tracking
        self._borrow_cooldowns: dict[str, float] = {}
        self._no_inventory_counts: dict[str, int] = 0
        self._paused_until: float = 0.0
        self._transfer_block_until: float = 0.0

    def is_paused(self) -> bool:
        return time.time() < self._paused_until

    def is_on_cooldown(self, asset: str) -> bool:
        cooldown_end = self._borrow_cooldowns.get(asset, 0)
        return time.time() < cooldown_end

    async def borrow_snipe(self, asset: str, amount: float, timeout: int | None = None) -> bool:
        """Try to borrow an asset, polling for inventory within timeout.

        Returns True if borrow succeeded.
        """
        timeout = timeout or self.config.borrow.borrow_snipe_timeout_seconds
        intervals = self.config.borrow.borrow_poll_intervals_seconds
        elapsed = 0

        self.logger.info(f"[{self.exchange_name}] Sniping borrow for {amount} {asset} (timeout {timeout}s)")

        for poll_interval in intervals:
            if elapsed >= timeout:
                break

            try:
                available = await self.client.get_borrowable(asset)
                if available >= amount:
                    await self.client.borrow_margin(asset, amount)
                    self.logger.info(f"[{self.exchange_name}] ✅ Borrowed {amount} {asset}")
                    self._no_inventory_counts = 0
                    return True
                else:
                    self.logger.info(
                        f"[{self.exchange_name}] Borrow inventory {asset}: "
                        f"{available:.6f} < {amount:.6f}, polling..."
                    )
            except Exception as e:
                err_str = str(e).lower()
                if "3045" in err_str or "no inventory" in err_str or "insufficient" in err_str:
                    self.logger.info(f"[{self.exchange_name}] No inventory for {asset}")
                else:
                    self.logger.warning(f"[{self.exchange_name}] Borrow attempt error: {e}")

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        # Exhausted timeout
        self.logger.warning(f"[{self.exchange_name}] Borrow snipe timed out for {asset}")

        # Track consecutive failures
        self._no_inventory_counts += 1
        pause_after = self.config.borrow.reverse_pause_after_no_inventory_count
        if self._no_inventory_counts >= pause_after:
            pause_secs = self.config.borrow.reverse_pause_seconds
            self._paused_until = time.time() + pause_secs
            self._no_inventory_counts = 0
            self.logger.warning(
                f"[{self.exchange_name}] Reverse carry paused for {pause_secs}s "
                f"after {pause_after} consecutive no-inventory attempts"
            )

        # Set cooldown for this asset
        self._borrow_cooldowns[asset] = (
            time.time() + self.config.borrow.borrow_precheck_cooldown_seconds
        )

        return False

    async def execute(
        self,
        symbol: str,
        size_usd: float,
        equity_usd: float,
        dry_run: bool = False,
    ) -> dict[str, Any] | None:
        """Open a reverse carry position.

        Returns position dict on success, None on failure.
        """
        base_asset = self.client._base_asset(symbol)

        if self.is_paused():
            remaining = self._paused_until - time.time()
            self.logger.info(
                f"[{self.exchange_name}] Reverse carry paused, {remaining:.0f}s remaining"
            )
            return None
        if time.time() < self._transfer_block_until:
            return None

        if self.is_on_cooldown(base_asset):
            remaining = self._borrow_cooldowns[base_asset] - time.time()
            self.logger.debug(
                f"[{self.exchange_name}] {base_asset} on borrow cooldown, {remaining:.0f}s left"
            )
            return None

        try:
            spot_price = await self.client.get_spot_price(symbol)
            futures_price = await self.client.get_futures_price(symbol)
            contract_size = await self.client.get_contract_size(symbol)
            fut_symbol = self.client._futures_symbol(symbol)
            spot_symbol = self.client._spot_symbol(symbol)
            target_base_qty = size_usd / spot_price
            futures_qty = await self.client.round_qty(
                fut_symbol,
                target_base_qty / contract_size,
            )
            borrow_qty = await self.client.round_qty(
                spot_symbol,
                futures_qty * contract_size,
            )

            if futures_qty <= 0 or borrow_qty <= 0:
                self.logger.debug(f"[{self.exchange_name}] Qty rounded to 0 for {symbol}")
                return None

            size_usd = borrow_qty * spot_price
            est_fees = size_usd * 2 * (self.config.execution.est_fee_percent / 100)
            leverage = max(1, int(self.config.position.leverage))

            # Ensure futures has margin (skip internal transfer in unified mode).
            futures_balance = await self.client.get_futures_balance()
            margin_needed = (futures_qty * contract_size * futures_price) / leverage
            if not self.config.unified_account and futures_balance["free"] < margin_needed:
                spot_balance = await self.client.get_spot_balance()
                if spot_balance["free"] + futures_balance["free"] < margin_needed:
                    self.logger.warning(
                        f"[{self.exchange_name}] Insufficient margin for {symbol}"
                    )
                    return None
                transfer = max(0.0, margin_needed - futures_balance["free"] + 0.2)
                moved = await self.client.safe_transfer_spot_to_futures("USDT", transfer)
                if moved <= 0:
                    self.logger.warning(
                        f"[{self.exchange_name}] Cannot rebalance funds spot→futures for {symbol}, skipping"
                    )
                    self._transfer_block_until = time.time() + 300
                    return None

            # Re-pull futures balance and shrink qty if needed.
            futures_balance = await self.client.get_futures_balance()
            if self.config.unified_account:
                spot_balance = await self.client.get_spot_balance()
                futures_margin_free = spot_balance["free"]
            else:
                futures_margin_free = futures_balance["free"]

            max_fut_qty = (
                (futures_margin_free * leverage) / (futures_price * contract_size)
                if futures_price > 0 and contract_size > 0
                else 0
            )
            futures_qty = await self.client.round_qty(fut_symbol, min(futures_qty, max_fut_qty))
            borrow_qty = await self.client.round_qty(spot_symbol, futures_qty * contract_size)
            if futures_qty <= 0 or borrow_qty <= 0:
                self.logger.warning(f"[{self.exchange_name}] Post-transfer affordable qty is 0 for {symbol}")
                return None
            size_usd = borrow_qty * spot_price
            est_fees = size_usd * 2 * (self.config.execution.est_fee_percent / 100)

            # Snipe borrow
            can_borrow = await self.borrow_snipe(
                base_asset, borrow_qty, self.config.borrow.borrow_snipe_timeout_seconds,
            )
            if not can_borrow:
                # Check partial fill
                if self.config.borrow.reverse_partial_fill_enabled:
                    available = await self.client.get_borrowable(base_asset)
                    min_qty = borrow_qty * self.config.borrow.reverse_partial_fill_min_ratio
                    if available >= min_qty:
                        borrow_qty = await self.client.round_qty(
                            spot_symbol, available
                        )
                        futures_qty = await self.client.round_qty(
                            fut_symbol,
                            borrow_qty / contract_size,
                        )
                        borrow_qty = await self.client.round_qty(
                            spot_symbol,
                            futures_qty * contract_size,
                        )
                        size_usd = borrow_qty * spot_price
                        self.logger.info(
                            f"[{self.exchange_name}] Partial fill: {borrow_qty} {base_asset} "
                            f"(${size_usd:.2f})"
                        )
                        await self.client.borrow_margin(base_asset, borrow_qty)
                    else:
                        return None
                else:
                    return None

            await self.client.set_leverage(symbol, self.config.position.leverage)

            self.logger.info(
                f"[{self.exchange_name}] Executing reverse carry for {symbol}: "
                f"borrow_qty={borrow_qty}, fut_qty={futures_qty}, spot=${spot_price:.4f}, futures=${futures_price:.4f}"
            )

            # Sell borrowed asset on spot
            spot_order = await self.client.sell_spot(
                symbol, borrow_qty, use_limit=self.config.execution.use_limit_orders,
            )

            try:
                # Long futures
                futures_order = await self.client.open_long_futures(
                    symbol, futures_qty, use_limit=self.config.execution.use_limit_orders,
                )
            except Exception as e:
                # Rollback: buy back spot and repay borrow
                self.logger.error(
                    f"[{self.exchange_name}] Futures long failed for {symbol}, "
                    f"rolling back: {e}"
                )
                try:
                    await self.client.buy_spot(symbol, borrow_qty)
                    await self.client.repay_margin(base_asset, borrow_qty)
                except Exception as rb_err:
                    self.logger.critical(
                        f"[{self.exchange_name}] ROLLBACK FAILED for {symbol}: {rb_err}"
                    )
                return None

            position = {
                "symbol": symbol,
                "exchange": self.exchange_name,
                "strategy": "reverse_carry",
                "spot_entry_price": spot_price,
                "futures_entry_price": futures_price,
                "borrow_qty": borrow_qty,
                "futures_qty": futures_qty,
                "futures_base_qty": borrow_qty,
                "borrow_asset": base_asset,
                "size_usd": size_usd,
                "entry_time": time.time(),
                "est_fees": est_fees,
            }

            self.logger.info(
                f"[{self.exchange_name}] ✅ Reverse carry opened for {symbol} "
                f"(${size_usd:.2f}, fees ~${est_fees:.2f})"
            )
            return position

        except Exception as e:
            self.logger.error(f"[{self.exchange_name}] Reverse carry error for {symbol}: {e}")
            return None

    async def close(self, position: dict[str, Any]) -> dict[str, Any] | None:
        """Close a reverse carry position."""
        symbol = position["symbol"]
        base_asset = position["borrow_asset"]
        try:
            borrow_qty = position["borrow_qty"]
            futures_qty = position["futures_qty"]
            futures_base_qty = position.get("futures_base_qty", futures_qty)

            self.logger.info(f"[{self.exchange_name}] Closing reverse carry for {symbol}")

            # Close futures long first.
            # Gate.io may return "empty position" if the futures leg was already closed
            # externally (e.g. liquidation). Treat this as already-gone and proceed.
            try:
                await self.client.close_long_futures(symbol, futures_qty)
            except Exception as fut_err:
                err_str = str(fut_err).lower()
                futures_gone_signals = [
                    "empty position",       # Gate.io
                    "increase_position",    # Gate.io alternate
                    "reduceonly",           # Binance — ReduceOnly Order is rejected
                ]
                if any(sig in err_str for sig in futures_gone_signals):
                    self.logger.warning(
                        f"[{self.exchange_name}] Futures leg already gone for {symbol} "
                        f"({fut_err}). Skipping futures close, buying back spot only."
                    )
                else:
                    raise  # unexpected — let outer handler log and return None
            # Buy back spot to repay borrow
            await self.client.buy_spot(symbol, borrow_qty)
            # Repay
            await self.client.repay_margin(base_asset, borrow_qty)

            spot_price = await self.client.get_spot_price(symbol)
            futures_price = await self.client.get_futures_price(symbol)

            spot_pnl = (position["spot_entry_price"] - spot_price) * borrow_qty
            futures_pnl = (futures_price - position["futures_entry_price"]) * futures_base_qty
            total_pnl = spot_pnl + futures_pnl - position.get("est_fees", 0)

            self.logger.info(
                f"[{self.exchange_name}] ✅ Closed {symbol}: "
                f"PnL=${total_pnl:.2f} (spot=${spot_pnl:.2f}, fut=${futures_pnl:.2f})"
            )

            return {"symbol": symbol, "pnl": total_pnl, "spot_pnl": spot_pnl, "futures_pnl": futures_pnl}

        except Exception as e:
            self.logger.error(f"[{self.exchange_name}] Close error for {symbol}: {e}")
            return None
