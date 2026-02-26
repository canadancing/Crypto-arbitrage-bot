"""Telegram notifications with trade alerts and exchange prefixing."""

from __future__ import annotations

import logging
from typing import Optional

import aiohttp


class TelegramNotifier:
    """Send notifications via Telegram Bot API."""

    def __init__(self, token: str, chat_id: str, enabled: bool = True) -> None:
        self.token = token
        self.chat_id = chat_id
        self.enabled = enabled
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.logger = logging.getLogger("telegram")
        self._send_timeout = aiohttp.ClientTimeout(total=8)
        self._updates_timeout = aiohttp.ClientTimeout(total=15)

    async def send(self, message: str) -> None:
        """Send a text message."""
        if not self.enabled or not self.token or not self.chat_id:
            return

        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML",
        }

        try:
            async with aiohttp.ClientSession(timeout=self._send_timeout) as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        self.logger.error(f"Telegram send failed: {await resp.text()}")
        except Exception as e:
            self.logger.error(f"Telegram error: {e}")

    async def send_trade_alert(
        self,
        exchange: str,
        symbol: str,
        side: str,
        strategy: str,
        details: dict,
    ) -> None:
        """Send a formatted trade execution alert with exchange context."""
        emoji = "🟢" if side == "OPEN" else "🔴"
        msg = (
            f"{emoji} <b>Trade Executed: {side}</b>\n\n"
            f"<b>Exchange:</b> {exchange.capitalize()}\n"
            f"<b>Symbol:</b> {symbol}\n"
            f"<b>Strategy:</b> {strategy}\n"
            f"<b>Price:</b> ${details.get('price', 0):.4f}\n"
            f"<b>Size:</b> {details.get('size', 0):.4f}\n"
            f"<b>Notional:</b> ${details.get('notional', 0):.2f}\n"
        )
        await self.send(msg)

    async def send_error(self, exchange: str, error_msg: str) -> None:
        await self.send(f"⚠️ <b>[{exchange.capitalize()}] Error</b>\n\n{error_msg}")

    async def send_heartbeat(self, status_lines: list[str]) -> None:
        header = "💓 <b>Heartbeat</b>\n\n"
        await self.send(header + "\n".join(status_lines))

    async def get_updates(self, offset: Optional[int] = None) -> list:
        """Fetch new messages from the bot."""
        if not self.enabled or not self.token:
            return []

        url = f"{self.base_url}/getUpdates"
        params: dict = {"timeout": 10}
        if offset:
            params["offset"] = offset

        try:
            async with aiohttp.ClientSession(timeout=self._updates_timeout) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("result", [])
                    else:
                        self.logger.error(f"Telegram updates failed: {await resp.text()}")
                        return []
        except Exception as e:
            self.logger.error(f"Telegram updates error: {e}")
            return []
