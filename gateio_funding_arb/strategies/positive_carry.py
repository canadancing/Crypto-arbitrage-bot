"""Positive carry strategy: buy spot + short futures when funding > 0."""

from __future__ import annotations

import logging
import time
from typing import Any

from gateio_funding_arb.config import ExchangeConfig


class PositiveCarryStrategy:
    """
    Positive Carry Trade:
    1.  Buy spot asset
    2.  Short perpetual futures at roughly equal notional
    3.  Collect positive funding payments as income
    4.  Close when funding normalizes

    Portfolio is delta-neutral: spot long offsets futures short.
    """

    def __init__(self, client, config: ExchangeConfig, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.config = config
        self.exchange_name = config.name
        self.logger = logger or logging.getLogger(f"pos_carry.{self.exchange_name}")
        self._transfer_block_until: float = 0.0
        self._capital_block_until: float = 0.0
        self._capital_block_seconds: int = 300
        self._capital_block_logged_until: float = 0.0

    def is_paused(self) -> bool:
        """Returns True if strategy is temporarily blocked from entering new trades."""
        return time.time() < max(self._capital_block_until, self._transfer_block_until)

    async def execute(
        self,
        symbol: str,
        size_usd: float,
        equity_usd: float,
        dry_run: bool = False,
    ) -> dict[str, Any] | None:
        """Open a positive carry position.

        Returns position dict on success, None on failure.
        """
        if time.time() < self._transfer_block_until:
            return None
        if time.time() < self._capital_block_until:
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
            spot_qty = await self.client.round_qty(
                spot_symbol,
                futures_qty * contract_size,
            )

            if futures_qty <= 0 or spot_qty <= 0:
                self.logger.debug(f"[{self.exchange_name}] Qty rounded to 0 for {symbol}")
                return None

            # Rough fee estimate
            size_usd = spot_qty * spot_price
            est_fees = size_usd * 2 * (self.config.execution.est_fee_percent / 100)
            leverage = max(1, int(self.config.position.leverage))

            # Ensure we have enough capital across spot & futures
            spot_balance = await self.client.get_spot_balance()
            futures_balance = await self.client.get_futures_balance()
            spot_needed = spot_qty * spot_price
            futures_margin = (futures_qty * contract_size * futures_price) / leverage
            if self.config.unified_account:
                # In a unified account the spot asset backs the futures short,
                # so we only need enough free USDT for the spot buy + fees.
                # The futures margin is NOT a separate cash outlay.
                total_available = spot_balance["free"]
                total_required = spot_needed + est_fees
            else:
                total_available = spot_balance["free"] + futures_balance["free"]
                total_required = spot_needed + futures_margin + est_fees

            if total_available < total_required:
                self._capital_block_until = time.time() + self._capital_block_seconds
                if time.time() >= self._capital_block_logged_until:
                    self.logger.warning(
                        f"[{self.exchange_name}] Insufficient capital for {symbol}: "
                        f"need ${total_required:.2f}, have ${total_available:.2f}. "
                        f"Pausing new entries for {self._capital_block_seconds}s"
                    )
                    self._capital_block_logged_until = self._capital_block_until
                return None

            # Unified accounts share margin across products; internal transfer is unnecessary.
            if not self.config.unified_account:
                if spot_balance["free"] < spot_needed and futures_balance["free"] > futures_margin:
                    transfer_amt = max(0.0, spot_needed - spot_balance["free"] + 0.2)
                    moved = await self.client.safe_transfer_futures_to_spot("USDT", transfer_amt)
                    if moved <= 0:
                        self.logger.warning(
                            f"[{self.exchange_name}] Cannot rebalance funds futures→spot for {symbol}, skipping"
                        )
                        self._transfer_block_until = time.time() + 300
                        return None
                elif futures_balance["free"] < futures_margin and spot_balance["free"] > spot_needed:
                    transfer_needed = max(0.0, futures_margin - futures_balance["free"] + 0.2)
                    # Keep enough spot USDT for the upcoming spot buy + fees.
                    spot_spare = max(0.0, spot_balance["free"] - spot_needed - est_fees)
                    transfer_amt = min(transfer_needed, spot_spare)
                    if transfer_amt <= 0:
                        self.logger.warning(
                            f"[{self.exchange_name}] No spare spot USDT to move for {symbol}, skipping"
                        )
                        self._transfer_block_until = time.time() + 300
                        return None
                    moved = await self.client.safe_transfer_spot_to_futures("USDT", transfer_amt)
                    if moved <= 0:
                        self.logger.warning(
                            f"[{self.exchange_name}] Cannot rebalance funds spot→futures for {symbol}, skipping"
                        )
                        self._transfer_block_until = time.time() + 300
                        return None

            # Re-pull balances and shrink qty to what is actually affordable after transfer.
            spot_balance = await self.client.get_spot_balance()
            futures_balance = await self.client.get_futures_balance()
            max_spot_base_qty = spot_balance["free"] / spot_price if spot_price > 0 else 0
            max_fut_qty_by_spot = max_spot_base_qty / contract_size
            futures_margin_free = spot_balance["free"] if self.config.unified_account else futures_balance["free"]
            max_fut_qty_by_margin = (
                (futures_margin_free * leverage) / (futures_price * contract_size)
                if futures_price > 0 and contract_size > 0
                else 0
            )
            affordable_fut_qty = min(futures_qty, max_fut_qty_by_spot, max_fut_qty_by_margin)
            futures_qty = await self.client.round_qty(fut_symbol, affordable_fut_qty)
            spot_qty = await self.client.round_qty(spot_symbol, futures_qty * contract_size)
            if futures_qty <= 0 or spot_qty <= 0:
                self.logger.warning(f"[{self.exchange_name}] Post-transfer affordable qty is 0 for {symbol}")
                return None
            # Keep requested notional aligned to executed qty for downstream status/history.
            size_usd = spot_qty * spot_price
            est_fees = size_usd * 2 * (self.config.execution.est_fee_percent / 100)

            # Set leverage
            await self.client.set_leverage(symbol, self.config.position.leverage)

            # Execute: buy spot, then short futures
            self.logger.info(
                f"[{self.exchange_name}] Executing positive carry for {symbol}: "
                f"spot_qty={spot_qty}, fut_qty={futures_qty}, spot=${spot_price:.4f}, futures=${futures_price:.4f}"
            )

            spot_order = await self.client.buy_spot(
                symbol, spot_qty, use_limit=self.config.execution.use_limit_orders,
            )

            try:
                futures_order = await self.client.open_short_futures(
                    symbol, futures_qty, use_limit=self.config.execution.use_limit_orders,
                )
            except Exception as e:
                # Rollback: sell back the spot we just bought
                self.logger.error(
                    f"[{self.exchange_name}] Futures short failed for {symbol}, "
                    f"rolling back spot buy: {e}"
                )
                try:
                    await self.client.sell_spot(symbol, spot_qty)
                except Exception as rb_err:
                    self.logger.critical(
                        f"[{self.exchange_name}] ROLLBACK FAILED for {symbol}: {rb_err}"
                    )
                return None

            actual_spot_price = spot_price
            if spot_order and isinstance(spot_order, dict):
                actual_spot_price = spot_order.get("average") or spot_order.get("price") or spot_price

            actual_futures_price = futures_price
            if futures_order and isinstance(futures_order, dict):
                actual_futures_price = futures_order.get("average") or futures_order.get("price") or futures_price

            position = {
                "symbol": symbol,
                "exchange": self.exchange_name,
                "strategy": "positive_carry",
                "spot_entry_price": float(actual_spot_price),
                "futures_entry_price": float(actual_futures_price),
                "spot_qty": spot_qty,
                "futures_qty": futures_qty,
                "futures_base_qty": spot_qty,
                "size_usd": size_usd,
                "entry_time": time.time(),
                "est_fees": est_fees,
            }

            self.logger.info(
                f"[{self.exchange_name}] ✅ Positive carry opened for {symbol} "
                f"(${size_usd:.2f}, fees ~${est_fees:.2f})"
            )
            return position

        except Exception as e:
            self.logger.error(f"[{self.exchange_name}] Positive carry error for {symbol}: {e}")
            return None

    async def close(self, position: dict[str, Any]) -> dict[str, Any] | None:
        """Close a positive carry position."""
        symbol = position["symbol"]
        try:
            spot_qty = position["spot_qty"]
            futures_qty = position["futures_qty"]
            futures_base_qty = position.get("futures_base_qty", futures_qty)

            self.logger.info(f"[{self.exchange_name}] Closing positive carry for {symbol}")

            # Close futures short first, then sell spot.
            # Gate.io may return "empty position" if the futures leg was already closed
            # externally (e.g. liquidation). Treat this as already-gone and proceed.
            try:
                await self.client.close_short_futures(symbol, futures_qty)
            except Exception as fut_err:
                err_str = str(fut_err).lower()
                # Futures leg may already be closed (previous partial close,
                # liquidation, or manual intervention).  Recognise the
                # exchange-specific error strings and proceed to sell spot.
                futures_gone_signals = [
                    "empty position",       # Gate.io
                    "increase_position",    # Gate.io alternate
                    "reduceonly",           # Binance — ReduceOnly Order is rejected
                ]
                if any(sig in err_str for sig in futures_gone_signals):
                    self.logger.warning(
                        f"[{self.exchange_name}] Futures leg already gone for {symbol} "
                        f"({fut_err}). Skipping futures close, selling spot only."
                    )
                else:
                    raise  # unexpected — let outer handler log and return None
            # Sell back spot. Fees on purchase might have reduced the base asset quantity slightly.
            base_asset = self.client._base_asset(symbol)
            spot_symbol = self.client._spot_symbol(symbol)
            spot_balance = await self.client.get_spot_balance(base_asset)
            actual_spot_qty = min(float(spot_qty), float(spot_balance.get("free", 0)))
            actual_spot_qty = await self.client.round_qty(spot_symbol, actual_spot_qty)

            if actual_spot_qty > 0:
                await self.client.sell_spot(symbol, actual_spot_qty)
            else:
                self.logger.warning(
                    f"[{self.exchange_name}] Spot balance for {symbol} is {actual_spot_qty} "
                    f"(requested {spot_qty}). Skipping sell."
                )

            spot_price = await self.client.get_spot_price(symbol)
            futures_price = await self.client.get_futures_price(symbol)

            spot_pnl = (spot_price - position["spot_entry_price"]) * spot_qty
            futures_pnl = (position["futures_entry_price"] - futures_price) * futures_base_qty
            
            # Deduct standard round-trip fees (bid-ask spread and exchange API fees)
            round_trip_estimate = position.get("est_fees", 0.0) * 2
            
            # Add actual funding payments and trading fees accrued during the position's lifetime
            last_pnl_status = position.get("last_status", {}).get("pnl", {})
            pnl_fund = last_pnl_status.get("funding_fee", 0.0)
            pnl_fee = last_pnl_status.get("trading_fee", 0.0)

            total_pnl = spot_pnl + futures_pnl + pnl_fund + pnl_fee - round_trip_estimate

            self.logger.info(
                f"[{self.exchange_name}] ✅ Closed {symbol}: "
                f"PnL=${total_pnl:.2f} (spot=${spot_pnl:.2f}, fut=${futures_pnl:.2f}, "
                f"funding=${pnl_fund:.4f}, fees=${pnl_fee:.4f})"
            )

            return {
                "symbol": symbol, 
                "pnl": total_pnl, 
                "spot_pnl": spot_pnl, 
                "futures_pnl": futures_pnl + pnl_fund + pnl_fee,
                "funding_fee": pnl_fund,
                "trading_fee": pnl_fee,
            }

        except Exception as e:
            self.logger.error(f"[{self.exchange_name}] Close error for {symbol}: {e}")
            return None
