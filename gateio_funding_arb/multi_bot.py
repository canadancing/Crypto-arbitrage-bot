"""Multi-exchange orchestrator — runs one ExchangeArbBot per enabled exchange."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from pathlib import Path
from typing import Any

from gateio_funding_arb.config import AppConfig, load_config
from gateio_funding_arb.bot import ExchangeArbBot
from gateio_funding_arb.utils.notifications import TelegramNotifier
from gateio_funding_arb.utils.logger import build_logger


class MultiExchangeBot:
    """Runs funding arb bots for all enabled exchanges concurrently."""

    def __init__(self, config_path: str = "config.yaml") -> None:
        self.config = load_config(config_path)
        self._status_path = Path(self.config.runtime.log_file).parent / "status.json"
        self.logger = build_logger(
            "multi_bot",
            log_file=self.config.runtime.log_file,
        )

        # Shared Telegram notifier
        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        telegram_chat = os.getenv("TELEGRAM_CHAT_ID", "")
        self.notifier = TelegramNotifier(
            token=telegram_token,
            chat_id=telegram_chat,
            enabled=self.config.notifications.telegram_enabled,
        )
        self._log_startup_credential_status(telegram_token, telegram_chat)

        # Create one bot per enabled exchange
        self.bots: list[ExchangeArbBot] = []
        for ex_config in self.config.exchanges:
            if not ex_config.enabled:
                self.logger.info(f"Skipping disabled exchange: {ex_config.name}")
                continue
            if not ex_config.api_key or ex_config.api_key.startswith("your_"):
                if not self.config.runtime.dry_run:
                    self.logger.warning(
                        f"Skipping {ex_config.name}: missing API credentials "
                        f"(set {ex_config.name.upper()}_API_KEY in .env)"
                    )
                    continue

            bot = ExchangeArbBot(
                config=ex_config,
                notifier=self.notifier,
                dry_run=self.config.runtime.dry_run,
                paper_equity=self.config.runtime.paper_equity_usd,
                logger=build_logger(
                    f"bot.{ex_config.name}",
                    log_file=self.config.runtime.log_file,
                ),
            )
            self.bots.append(bot)

        self.logger.info(
            f"MultiExchangeBot initialized with {len(self.bots)} exchange(s): "
            f"{[b.name for b in self.bots]}"
        )

        # Telegram command handling
        self._tg_offset: int | None = None
        self._heartbeat_interval = self.config.notifications.heartbeat_interval_minutes * 60
        self._last_heartbeat = 0.0

    def _cred_status(self, value: str) -> str:
        if not value:
            return "missing"
        if value.startswith("your_"):
            return "placeholder"
        return "set"

    def _log_startup_credential_status(self, telegram_token: str, telegram_chat: str) -> None:
        self.logger.info(
            "Credential check | telegram_token=%s(len=%d) telegram_chat_id=%s",
            self._cred_status(telegram_token),
            len(telegram_token),
            self._cred_status(telegram_chat),
        )
        for ex in self.config.exchanges:
            self.logger.info(
                "Credential check | exchange=%s enabled=%s api_key=%s(len=%d) api_secret=%s(len=%d)",
                ex.name,
                ex.enabled,
                self._cred_status(ex.api_key),
                len(ex.api_key),
                self._cred_status(ex.api_secret),
                len(ex.api_secret),
            )

    # ── lifecycle ───────────────────────────────────────────────────

    def run(self) -> None:
        """Synchronous entry point — runs the async event loop."""
        try:
            asyncio.run(self._run_all())
        except KeyboardInterrupt:
            self.logger.info("Bot stopped by user (Ctrl+C)")
        except Exception as e:
            self.logger.critical(f"Fatal error: {e}", exc_info=True)

    async def _run_all(self) -> None:
        """Launch all exchange bots + shared services concurrently."""
        if not self.bots:
            self.logger.error("No exchange bots configured. Check config.yaml and .env")
            return

        # Register signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self._shutdown()))

        try:
            await asyncio.wait_for(
                self.notifier.send(
                    f"🚀 <b>Multi-Exchange Arb Bot Started</b>\n"
                    f"Exchanges: {', '.join(b.name.capitalize() for b in self.bots)}\n"
                    f"Dry run: {self.config.runtime.dry_run}"
                ),
                timeout=5,
            )
        except Exception as e:
            self.logger.warning(f"Startup notification skipped: {e}")

        tasks = []
        for bot in self.bots:
            tasks.append(asyncio.create_task(bot.main_loop(), name=f"bot_{bot.name}"))

        # Shared services
        tasks.append(asyncio.create_task(self._telegram_loop(), name="telegram"))
        tasks.append(asyncio.create_task(self._heartbeat_loop(), name="heartbeat"))
        tasks.append(asyncio.create_task(self._status_dump_loop(), name="status_dump"))

        # Run indefinitely; log and continue on transient loop failures.
        while True:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                name = t.get_name()
                exc = t.exception()
                if exc:
                    self.logger.error(f"Task {name} crashed: {exc}", exc_info=exc)
                if name.startswith("bot_"):
                    self.logger.error(f"{name} exited unexpectedly; restarting in 3s")
                    await asyncio.sleep(3)
                    bot_name = name.replace("bot_", "", 1)
                    bot = next((b for b in self.bots if b.name == bot_name), None)
                    if bot:
                        tasks.remove(t)
                        tasks.append(asyncio.create_task(bot.main_loop(), name=name))
                elif name == "telegram":
                    self.logger.warning("telegram loop exited; restarting in 3s")
                    await asyncio.sleep(3)
                    tasks.remove(t)
                    tasks.append(asyncio.create_task(self._telegram_loop(), name=name))
                elif name == "heartbeat":
                    self.logger.warning("heartbeat loop exited; restarting in 3s")
                    await asyncio.sleep(3)
                    tasks.remove(t)
                    tasks.append(asyncio.create_task(self._heartbeat_loop(), name=name))
                elif name == "status_dump":
                    self.logger.warning("status dump loop exited; restarting in 3s")
                    await asyncio.sleep(3)
                    tasks.remove(t)
                    tasks.append(asyncio.create_task(self._status_dump_loop(), name=name))

    async def _shutdown(self) -> None:
        self.logger.info("Shutting down all exchange bots...")
        for bot in self.bots:
            await bot.shutdown()

    # ── telegram ────────────────────────────────────────────────────

    async def _telegram_loop(self) -> None:
        """Poll Telegram for commands."""
        while True:
            try:
                updates = await self.notifier.get_updates(self._tg_offset)
                for update in updates:
                    self._tg_offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    text = msg.get("text", "").strip()

                    try:
                        if text.startswith("/"):
                            await self._handle_command(text)
                    except Exception as handle_err:
                        self.logger.error(f"Error handling command '{text}': {handle_err}")

                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"Telegram loop error: {e}")
                await asyncio.sleep(10)

    async def _handle_command(self, text: str) -> None:
        cmd = text.split()[0].lower()

        if cmd == "/status":
            lines = []
            for bot in self.bots:
                s = await bot.get_status_async()
                lines.append(
                    f"<b>{s['exchange'].capitalize()}</b>\n"
                    f"  Positions: {s['positions']}/{s['max_positions']}\n"
                    f"  Daily PnL: ${s['daily_pnl']:+.2f}\n"
                    f"  Equity: ${s['starting_equity']:,.2f}\n"
                    f"  Loss limit: {'⚠️ EXCEEDED' if s['loss_limit_exceeded'] else '✅ OK'}"
                )
            await self.notifier.send("📊 <b>Status</b>\n\n" + "\n\n".join(lines))

        elif cmd == "/positions":
            lines = []
            for bot in self.bots:
                for p in bot.positions:
                    pnl = p.get("last_status", {}).get("pnl", {})
                    lines.append(
                        f"<b>{bot.name.capitalize()}</b> {p['symbol']}\n"
                        f"  Strategy: {p['strategy']}\n"
                        f"  Size: ${p['size_usd']:.2f}\n"
                        f"  PnL: ${pnl.get('total_pnl', 0):.2f}\n"
                        f"  Funding Earned: ${pnl.get('funding_fee', 0):.2f}"
                    )
            if lines:
                await self.notifier.send("📋 <b>Open Positions</b>\n\n" + "\n\n".join(lines))
            else:
                await self.notifier.send("📋 No open positions")

        elif cmd == "/help":
            await self.notifier.send(
                "🤖 <b>Commands</b>\n\n"
                "/status — Show status for all exchanges\n"
                "/positions — List open positions\n"
                "/help — Show this message"
            )

        else:
            await self.notifier.send(f"Unknown command: {cmd}. Try /help")

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat messages."""
        # Grace period: wait 2 minutes after startup before sending any heartbeat
        # so the bots have time to load live positions from the exchange.
        await asyncio.sleep(120)
        while True:
            try:
                now = time.time()
                if now - self._last_heartbeat >= self._heartbeat_interval:
                    lines = []
                    for bot in self.bots:
                        # Use async status which reads live exchange positions,
                        # not just the in-memory list (which is empty after restart).
                        s = await bot.get_status_async()
                        n_positions = int(s.get("positions", 0))
                        max_pos = int(s.get("max_positions", 0))
                        daily_pnl = float(s.get("daily_pnl", 0) or 0)
                        equity = float(s.get("starting_equity", 0) or 0)
                        lines.append(
                            f"[{bot.name.capitalize()}] {n_positions}/{max_pos} positions | "
                            f"PnL ${daily_pnl:+.2f} | Equity ${equity:,.0f}"
                        )
                    await self.notifier.send_heartbeat(lines)
                    self._last_heartbeat = now

                await asyncio.sleep(60)
            except Exception as e:
                self.logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(60)

    async def _status_dump_loop(self) -> None:
        """Persist live status for the dashboard process."""
        while True:
            try:
                exchanges = await asyncio.gather(*(b.get_status_async() for b in self.bots))
                payload = {
                    "dry_run": self.config.runtime.dry_run,
                    "exchanges": exchanges,
                    "total_positions": sum(int(e.get("positions", 0)) for e in exchanges),
                    "total_daily_pnl": sum(float(e.get("daily_pnl", 0) or 0) for e in exchanges),
                }
                payload["updated_at"] = time.time()
                self._status_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = self._status_path.with_suffix(".tmp")
                with open(tmp, "w", encoding="utf-8") as f:
                    # Write to file (instead of Path.write_text which might be less explicit)
                    f.write(json.dumps(payload, default=str))

                try:
                    os.replace(tmp, self._status_path)
                except OSError as write_err:
                    # Dashboard or something else might be holding a lock transiently
                    self.logger.debug(f"Transient status file replace error: {write_err}")
                await asyncio.sleep(2)
            except Exception as e:
                self.logger.error(f"Status dump error: {e}")
                await asyncio.sleep(2)

    # ── dashboard helpers ───────────────────────────────────────────

    def get_all_status(self) -> dict[str, Any]:
        """Combined status for dashboard API."""
        return {
            "dry_run": self.config.runtime.dry_run,
            "exchanges": [bot.get_status() for bot in self.bots],
            "total_positions": sum(len(b.positions) for b in self.bots),
            "total_daily_pnl": sum(b.safety.daily_pnl for b in self.bots),
        }
