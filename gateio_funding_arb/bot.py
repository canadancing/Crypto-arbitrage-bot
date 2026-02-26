"""Per-exchange funding arbitrage bot."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from gateio_funding_arb.config import ExchangeConfig
from gateio_funding_arb.clients.exchange_client import ExchangeClient, DryRunClient
from gateio_funding_arb.strategies.positive_carry import PositiveCarryStrategy
from gateio_funding_arb.strategies.reverse_carry import ReverseCarryStrategy
from gateio_funding_arb.monitoring.position_monitor import PositionMonitor
from gateio_funding_arb.monitoring.margin_rebalancer import MarginRebalancer
from gateio_funding_arb.utils.history import HistoryStore
from gateio_funding_arb.utils.safety_checks import SafetyChecker
from gateio_funding_arb.utils.notifications import TelegramNotifier


class ExchangeArbBot:
    """Funding arbitrage bot for a single exchange.

    The MultiExchangeBot creates one of these per enabled exchange.
    """

    def __init__(
        self,
        config: ExchangeConfig,
        notifier: TelegramNotifier,
        dry_run: bool = False,
        paper_equity: float = 1000.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config
        self.name = config.name
        self.logger = logger or logging.getLogger(f"bot.{self.name}")
        self.notifier = notifier
        self.dry_run = dry_run
        self.running = False

        # Client
        if dry_run:
            self.client = DryRunClient(config, paper_equity=paper_equity, logger=self.logger)
        else:
            self.client = ExchangeClient(config, logger=self.logger)

        # Safety
        self.safety = SafetyChecker(config, logger=self.logger)

        # Strategies
        self.positive_carry = PositiveCarryStrategy(self.client, config, logger=self.logger)
        self.reverse_carry = ReverseCarryStrategy(self.client, config, logger=self.logger)

        # Monitoring
        self.position_monitor = PositionMonitor(self.client, config, self.safety, logger=self.logger)
        self.margin_rebalancer = MarginRebalancer(self.client, config, logger=self.logger)

        # State
        self.positions: list[dict[str, Any]] = []
        self._scan_attempt_count = 0
        self._last_cycle_heartbeat = 0.0
        self._last_live_refresh = 0.0
        self._status_equity = 0.0
        self._status_spot_equity = 0.0
        self._status_futures_equity = 0.0
        self._exchange_open_positions: list[dict[str, Any]] = []
        self.history = HistoryStore()

    async def initialize(self) -> None:
        """Initialize equity and leverage defaults."""
        equity = await self.client.get_total_equity()
        self._status_equity = equity
        self.safety.set_starting_equity(equity)
        self.logger.info(
            f"[{self.name}] Initialized — equity: ${equity:,.2f}, "
            f"dry_run: {self.dry_run}"
        )
        await self.notifier.send(
            f"🚀 <b>[{self.name.capitalize()}]</b> Bot started\n"
            f"Equity: ${equity:,.2f} | Dry run: {self.dry_run}"
        )
        await self._refresh_live_status(force=True)
        await self._adopt_existing_positions()
        await self._cleanup_orphan_spots()

    async def _adopt_existing_positions(self) -> None:
        """Detect and adopt pre-existing exchange positions for monitoring.

        At startup the bot may find futures positions that were opened by a
        previous session (or manually).  This method converts them into tracked
        positions so the position monitor can auto-close them when funding
        rates drop.
        """
        if self.dry_run:
            return

        try:
            live_positions = await self.client.get_futures_positions()
        except Exception as e:
            self.logger.warning(f"[{self.name}] Could not fetch positions for adoption: {e}")
            return

        # Fetch spot balances once for matching
        try:
            spot_bal = await self.client.spot_exchange.fetch_balance()
            spot_totals = spot_bal.get("total", {})
        except Exception:
            spot_totals = {}

        adopted = 0
        for p in live_positions:
            contracts = float(
                p.get("contracts")
                or p.get("positionAmt")
                or p.get("info", {}).get("size")
                or p.get("info", {}).get("positionAmt")
                or 0
            )
            if abs(contracts) <= 0:
                continue

            symbol = p.get("symbol") or p.get("info", {}).get("contract") or "unknown"
            symbol = self.client._futures_symbol(symbol)  # normalize

            # Skip if already tracked
            if any(pos["symbol"] == symbol for pos in self.positions):
                continue

            side = p.get("side", "").lower()
            notional = abs(float(
                p.get("notional")
                or p.get("info", {}).get("value")
                or p.get("info", {}).get("notional")
                or 0
            ))

            # Determine strategy from position direction
            if side == "short":
                strategy = "positive_carry"
            elif side == "long":
                strategy = "reverse_carry"
            else:
                self.logger.info(f"[{self.name}] Skipping adoption of {symbol}: unknown side '{side}'")
                continue

            # Get current prices as approximate entry prices
            try:
                spot_price = await self.client.get_spot_price(symbol)
                futures_price = await self.client.get_futures_price(symbol)
                contract_size = await self.client.get_contract_size(symbol)
            except Exception as e:
                self.logger.warning(f"[{self.name}] Cannot adopt {symbol}: price fetch failed: {e}")
                continue

            base_asset = self.client._base_asset(symbol)
            futures_base_qty = abs(contracts) * contract_size

            if strategy == "positive_carry":
                # Match with spot holdings
                spot_qty = float(spot_totals.get(base_asset, 0) or 0)
                if spot_qty <= 0:
                    self.logger.info(
                        f"[{self.name}] Skipping adoption of {symbol}: "
                        f"short futures found but no spot {base_asset} holdings"
                    )
                    continue

                position = {
                    "symbol": symbol,
                    "strategy": "positive_carry",
                    "size_usd": notional,
                    "entry_time": time.time(),
                    "spot_entry_price": spot_price,
                    "futures_entry_price": futures_price,
                    "spot_qty": spot_qty,
                    "futures_qty": abs(contracts),
                    "futures_base_qty": futures_base_qty,
                    "est_fees": 0,
                    "_adopted": True,
                }
            else:
                # Reverse carry: long futures + borrowed spot sold
                position = {
                    "symbol": symbol,
                    "strategy": "reverse_carry",
                    "size_usd": notional,
                    "entry_time": time.time(),
                    "spot_entry_price": spot_price,
                    "futures_entry_price": futures_price,
                    "borrow_qty": futures_base_qty,
                    "borrow_asset": base_asset,
                    "futures_qty": abs(contracts),
                    "futures_base_qty": futures_base_qty,
                    "est_fees": 0,
                    "_adopted": True,
                }

            position_id = f"{self.name}:{symbol}:adopted:{time.time()}"
            position["position_id"] = position_id
            self.positions.append(position)
            self.safety.add_position(position)
            self.position_monitor.add_position(position)

            adopted += 1
            self.logger.info(
                f"[{self.name}] ✅ Adopted existing position: {symbol} "
                f"({strategy}, ${notional:.2f})"
            )

        if adopted > 0:
            await self.notifier.send(
                f"📌 <b>[{self.name.capitalize()}]</b> Adopted {adopted} existing position(s)"
            )

    async def _cleanup_orphan_spots(self) -> None:
        """Sell spot tokens that have no matching futures position.

        This handles the case where a previous close partially succeeded
        (futures closed but spot sell failed), leaving orphaned spot tokens.
        """
        if self.dry_run:
            return

        try:
            spot_bal = await self.client.spot_exchange.fetch_balance()
            spot_totals = spot_bal.get("total", {})
        except Exception as e:
            self.logger.warning(f"[{self.name}] Could not fetch spot balances for orphan cleanup: {e}")
            return

        # Build set of base assets that have active tracked positions
        tracked_assets = set()
        for pos in self.positions:
            tracked_assets.add(self.client._base_asset(pos["symbol"]))

        cleaned = 0
        for asset, amount in spot_totals.items():
            if not isinstance(amount, (int, float)) or amount <= 0:
                continue
            # Skip stablecoins and very small dust
            if asset in ("USDT", "USDC", "BUSD", "USD", "BNB"):
                continue

            # Check if this asset has a tracked position — if so, it's fine
            if asset in tracked_assets:
                continue

            # Check dollar value — ignore dust below $5 (Binance min notional is ~$5)
            try:
                symbol = f"{asset}/USDT"
                if symbol not in self.client.spot_exchange.markets:
                    await self.client.spot_exchange.load_markets()
                if symbol not in self.client.spot_exchange.markets:
                    continue
                ticker = await self.client.spot_exchange.fetch_ticker(symbol)
                price = float(ticker.get("last") or ticker.get("close") or 0)
                value_usd = amount * price
                if value_usd < 5.0:
                    continue
            except Exception:
                continue

            # This is an orphaned spot holding — sell it
            self.logger.warning(
                f"[{self.name}] 🧹 Orphan detected: {amount:.6f} {asset} "
                f"(~${value_usd:.2f}) with no futures hedge. Selling..."
            )
            try:
                qty = await self.client.round_qty(symbol, amount)
                if qty > 0:
                    await self.client.sell_spot(f"{asset}/USDT:USDT", qty)
                    self.logger.info(
                        f"[{self.name}] ✅ Orphan sold: {qty} {asset} (~${value_usd:.2f})"
                    )
                    cleaned += 1
            except Exception as e:
                self.logger.error(
                    f"[{self.name}] Failed to sell orphan {asset}: {e}"
                )

        if cleaned > 0:
            await self.notifier.send(
                f"🧹 <b>[{self.name.capitalize()}]</b> Cleaned up {cleaned} orphaned spot holding(s)"
            )

    async def _refresh_live_status(self, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self._last_live_refresh) < 20:
            return
        self._last_live_refresh = now
        try:
            self._status_equity = await self.client.get_total_equity()
            breakdown = await self.client.get_equity_breakdown()
            self._status_spot_equity = breakdown["spot"]
            self._status_futures_equity = breakdown["futures"]
        except Exception:
            pass
        try:
            live = await self.client.get_futures_positions()
            mapped: list[dict[str, Any]] = []
            for p in live:
                contracts = float(
                    p.get("contracts")
                    or p.get("positionAmt")
                    or p.get("info", {}).get("size")
                    or p.get("info", {}).get("positionAmt")
                    or 0
                )
                if abs(contracts) <= 0:
                    continue
                symbol = p.get("symbol") or p.get("info", {}).get("contract") or "unknown"
                notional = float(
                    p.get("notional")
                    or p.get("info", {}).get("value")
                    or p.get("info", {}).get("notional")
                    or 0
                )
                mapped.append(
                    {
                        "symbol": symbol,
                        "strategy": "live_exchange",
                        "size_usd": abs(notional),
                        "entry_time": now,
                        "pnl": {"total_pnl": float(p.get("unrealizedPnl") or p.get("info", {}).get("unrealised_pnl") or 0)},
                    }
                )
            self._exchange_open_positions = mapped
        except Exception:
            pass

    async def scan_and_trade(self) -> dict[str, Any]:
        """One scan cycle: find opportunities and execute trades."""
        if not self.safety.is_within_daily_loss_limit():
            self.logger.warning(f"[{self.name}] Daily loss limit exceeded, skipping scan")
            return {"opportunities": 0, "attempted": 0, "opened": 0, "skipped_loss_limit": True}

        opportunities = await self.client.scan_funding_rates()
        if not opportunities:
            self.logger.info(f"[{self.name}] No opportunities found")
            return {"opportunities": 0, "attempted": 0, "opened": 0, "skipped_loss_limit": False}

        self._scan_attempt_count = 0
        max_attempts = self.config.scan.max_attempts_per_cycle
        equity = await self.client.get_total_equity()
        opened = 0

        for opp in opportunities:
            if self._scan_attempt_count >= max_attempts:
                break

            symbol = opp["symbol"]
            daily_rate = opp["daily_rate"]
            fee_cost = self.config.execution.est_fee_percent * 2
            net_edge = abs(daily_rate) - fee_cost

            # Determine strategy
            is_positive = daily_rate > 0
            if is_positive:
                min_rate = self.config.thresholds.min_positive_funding_rate_daily
                strategy_name = "positive_carry"
            else:
                min_rate = self.config.thresholds.min_negative_funding_rate_daily
                strategy_name = "reverse_carry"

            if abs(daily_rate) < min_rate:
                continue

            net_threshold = self.config.thresholds.min_net_edge_daily
            if net_edge < net_threshold:
                continue

            # Skip if reverse carry disabled or paused
            if not is_positive:
                if not self.config.borrow.enable_reverse_carry:
                    continue
                if self.reverse_carry.is_paused():
                    continue
                base = self.client._base_asset(symbol)
                if self.reverse_carry.is_on_cooldown(base):
                    continue
            else:
                if self.positive_carry.is_paused():
                    continue

            # Prioritize positive carry
            if self.config.borrow.prioritize_positive_carry and not is_positive:
                # Check if there are still positive carry opportunities ahead
                remaining_positive = any(
                    o["daily_rate"] > self.config.thresholds.min_positive_funding_rate_daily
                    for o in opportunities[opportunities.index(opp)+1:]
                    if o["daily_rate"] > 0
                )
                if remaining_positive:
                    continue

            # Skip if we already hold this position, to avoid log spam and API calls
            if any(p["symbol"] == symbol for p in self.positions):
                continue

            # Validate trade
            try:
                spot_price = await self.client.get_spot_price(symbol)
                futures_price = await self.client.get_futures_price(symbol)
            except Exception as e:
                self.logger.warning(f"[{self.name}] Price fetch failed for {symbol}: {e}")
                continue

            # Dynamic position sizing: target 80% of equity spread evenly across
            # all position slots so that size scales up automatically as balance grows.
            # max_position_size_usd in config acts as a hard safety cap.
            max_slots = self.config.position.max_concurrent_positions
            size_usd = (equity * 0.80) / max(max_slots, 1)
            size_usd = min(size_usd, self.config.position.max_position_size_usd)
            size_usd = max(size_usd, 5.0)  # never open a position smaller than $5
            is_valid, messages = self.safety.validate_trade(
                symbol, size_usd, spot_price, futures_price,
            )
            if not is_valid:
                for msg in messages:
                    self.logger.info(f"[{self.name}] {symbol} rejected: {msg}")
                continue

            self._scan_attempt_count += 1

            self.logger.info(
                f"[{self.name}] 🎯 Opportunity: {symbol} "
                f"{daily_rate:+.2f}%/day (net ~{net_edge:+.2f}%) → {strategy_name}"
            )

            # Execute
            position = None
            if is_positive:
                position = await self.positive_carry.execute(
                    symbol, size_usd, equity, dry_run=self.dry_run,
                )
            else:
                position = await self.reverse_carry.execute(
                    symbol, size_usd, equity, dry_run=self.dry_run,
                )

            if position:
                opened += 1
                position_id = f"{self.name}:{symbol}:{position.get('entry_time', time.time())}"
                position["position_id"] = position_id
                self.positions.append(position)
                self.safety.add_position(position)
                self.position_monitor.add_position(position)
                self.history.append({
                    "event": "OPEN",
                    "position_id": position_id,
                    "exchange": self.name,
                    "symbol": symbol,
                    "strategy": strategy_name,
                    "size_usd": position.get("size_usd", size_usd),
                    "entry_time": position.get("entry_time"),
                    "spot_entry_price": position.get("spot_entry_price"),
                    "futures_entry_price": position.get("futures_entry_price"),
                    "spot_qty": position.get("spot_qty"),
                    "futures_qty": position.get("futures_qty"),
                    "borrow_qty": position.get("borrow_qty"),
                    "dry_run": self.dry_run,
                })
                await self.notifier.send_trade_alert(
                    self.name, symbol, "OPEN", strategy_name,
                    {"price": spot_price, "size": position.get("spot_qty", position.get("borrow_qty", 0)),
                     "notional": size_usd},
                )
        return {
            "opportunities": len(opportunities),
            "attempted": self._scan_attempt_count,
            "opened": opened,
            "skipped_loss_limit": False,
        }

    async def check_and_close_positions(self) -> None:
        """Check monitored positions and close those that need closing."""
        for position in self.positions[:]:
            status = position.get("last_status", {})
            if not status.get("should_close", False):
                continue

            symbol = position["symbol"]
            strategy = position["strategy"]
            self.logger.info(
                f"[{self.name}] Closing {symbol}: {status.get('reason', 'unknown')}"
            )

            result = None
            if strategy == "positive_carry":
                result = await self.positive_carry.close(position)
            else:
                result = await self.reverse_carry.close(position)

            if result:
                # Finalize PnL correctly by applying the difference
                # between the exact closure PnL and the last unrealized snapshot
                final_unrealized = position.get("last_total_pnl", 0.0)
                final_delta = result.get("pnl", 0.0) - final_unrealized
                self.safety.update_pnl(final_delta)

                self.positions.remove(position)
                self.safety.remove_position(symbol)
                self.position_monitor.remove_position(symbol)
                entry_ts = float(position.get("entry_time", time.time()))
                close_ts = time.time()
                self.history.append({
                    "event": "CLOSE",
                    "position_id": position.get("position_id", f"{self.name}:{symbol}:{entry_ts}"),
                    "exchange": self.name,
                    "symbol": symbol,
                    "strategy": strategy,
                    "size_usd": position.get("size_usd", 0),
                    "entry_time": entry_ts,
                    "close_time": close_ts,
                    "hold_seconds": max(0, close_ts - entry_ts),
                    "pnl": result.get("pnl", 0),
                    "spot_pnl": result.get("spot_pnl", 0),
                    "futures_pnl": result.get("futures_pnl", 0),
                    "dry_run": self.dry_run,
                    "close_reason": status.get("reason", "unknown"),
                })
                await self.notifier.send_trade_alert(
                    self.name, symbol, "CLOSE", strategy,
                    {"price": 0, "size": 0, "notional": result.get("pnl", 0)},
                )
            else:
                # Close failed — track consecutive failures.
                # After 3 attempts, force-remove from local tracking so we don't
                # loop forever. Manual review of the exchange position is advised.
                position["_close_attempts"] = position.get("_close_attempts", 0) + 1
                if position["_close_attempts"] >= 3:
                    self.logger.error(
                        f"[{self.name}] {symbol} failed to close after "
                        f"{position['_close_attempts']} attempts — force-removing from "
                        f"tracking. Please verify the position manually on the exchange."
                    )
                    self.positions.remove(position)
                    self.safety.remove_position(symbol)
                    self.position_monitor.remove_position(symbol)
                    self.history.append({
                        "event": "CLOSE_FORCED",
                        "position_id": position.get("position_id", f"{self.name}:{symbol}"),
                        "exchange": self.name,
                        "symbol": symbol,
                        "strategy": strategy,
                        "size_usd": position.get("size_usd", 0),
                        "entry_time": float(position.get("entry_time", time.time())),
                        "close_time": time.time(),
                        "pnl": position.get("last_total_pnl", 0),
                        "dry_run": self.dry_run,
                        "close_reason": "force_removed_after_repeated_failures",
                    })

    async def main_loop(self) -> None:
        """Main trading loop for this exchange."""
        self.running = True
        await self.initialize()

        # Start background tasks
        monitor_task = asyncio.create_task(self.position_monitor.monitoring_loop())
        rebalancer_task = asyncio.create_task(
            self.margin_rebalancer.rebalancing_loop(lambda: self.positions)
        )

        scan_interval = self.config.scan.scan_interval_seconds

        try:
            while self.running:
                try:
                    cycle = await self.scan_and_trade()
                    await self.check_and_close_positions()
                    await self._refresh_live_status()
                    now = time.time()
                    if now - self._last_cycle_heartbeat >= 120:
                        self.logger.info(
                            f"[{self.name}] Heartbeat — opportunities={cycle.get('opportunities', 0)}, "
                            f"attempted={cycle.get('attempted', 0)}, opened={cycle.get('opened', 0)}, "
                            f"positions={len(self.positions)}"
                        )
                        self._last_cycle_heartbeat = now
                except Exception as e:
                    self.logger.error(f"[{self.name}] Main loop error: {e}", exc_info=True)
                    await self.notifier.send_error(self.name, str(e))

                await asyncio.sleep(scan_interval)

        finally:
            self.position_monitor.stop()
            monitor_task.cancel()
            rebalancer_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
            try:
                await rebalancer_task
            except asyncio.CancelledError:
                pass

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        self.running = False
        self.logger.info(f"[{self.name}] Shutting down...")
        await self.notifier.send(f"🛑 <b>[{self.name.capitalize()}]</b> Bot stopped")
        await self.client.close()

    def get_status(self) -> dict[str, Any]:
        """Return current status for dashboard."""
        tracked_positions = [
            {
                "symbol": p["symbol"],
                "strategy": p["strategy"],
                "size_usd": p["size_usd"],
                "entry_time": p["entry_time"],
                "pnl": p.get("last_status", {}).get("pnl", {}),
            }
            for p in self.positions
        ]
        if tracked_positions:
            open_positions = tracked_positions
            positions_count = len(tracked_positions)
        else:
            open_positions = self._exchange_open_positions
            positions_count = len(self._exchange_open_positions)

        return {
            "exchange": self.name,
            "running": self.running,
            "dry_run": self.dry_run,
            "positions": positions_count,
            "max_positions": self.config.position.max_concurrent_positions,
            "daily_pnl": self.safety.daily_pnl,
            "starting_equity": self._status_equity or self.safety.starting_equity,
            "spot_equity": self._status_spot_equity,
            "futures_equity": self._status_futures_equity,
            "loss_limit_exceeded": not self.safety.is_within_daily_loss_limit(),
            "open_positions": open_positions,
        }

    async def get_status_async(self) -> dict[str, Any]:
        """Refresh live exchange snapshot before returning status."""
        await self._refresh_live_status(force=True)
        return self.get_status()
