"""Unified exchange client for Gate.io and Binance via ccxt."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Any

import ccxt.async_support as ccxt

from gateio_funding_arb.config import ExchangeConfig


class ExchangeClient:
    """
    Unified async exchange client using ccxt.

    Supports Gate.io and Binance with common interface for:
    - Market data (spot/futures prices, funding rates)
    - Order execution (spot, futures, borrow/repay)
    - Account queries (balances, positions, transfers)
    - Symbol precision/rounding
    """

    # Maps our config exchange name → ccxt exchange id
    EXCHANGE_MAP = {"binance": "binance", "gateio": "gateio"}

    def __init__(self, config: ExchangeConfig, logger: logging.Logger | None = None) -> None:
        self.config = config
        self.name = config.name
        self.logger = logger or logging.getLogger(f"client.{self.name}")
        self._precision_cache: dict[str, dict[str, Any]] = {}
        self._request_count = 0
        self._last_request_time = 0.0
        self._equity_cache_value: float | None = None
        self._equity_cache_ts: float = 0.0
        self._request_timeout_seconds = 20

        exchange_cls = getattr(ccxt, self.EXCHANGE_MAP[self.name])
        common_opts: dict[str, Any] = {
            "apiKey": config.api_key,
            "secret": config.api_secret,
            "enableRateLimit": True,
        }

        if self.name == "binance":
            futures_options: dict[str, Any] = {
                "defaultType": "future",
                "adjustForTimeDifference": True,
            }
            spot_options: dict[str, Any] = {
                "defaultType": "spot",
                "adjustForTimeDifference": True,
            }
        elif self.name == "gateio":
            futures_options = {"defaultType": "swap"}
            spot_options = {"defaultType": "spot"}
        else:
            futures_options = {}
            spot_options = {}

        futures_opts = dict(common_opts)
        futures_opts["options"] = futures_options
        spot_opts = dict(common_opts)
        spot_opts["options"] = spot_options

        self.futures_exchange: ccxt.Exchange = exchange_cls(futures_opts)
        self.spot_exchange: ccxt.Exchange = exchange_cls(spot_opts)
        # Backward compatibility for existing call sites.
        self.exchange: ccxt.Exchange = self.futures_exchange

    async def close(self) -> None:
        await self.futures_exchange.close()
        if self.spot_exchange is not self.futures_exchange:
            await self.spot_exchange.close()

    # ── helpers ──────────────────────────────────────────────────────

    async def _retry(self, fn, *args, retries: int = 3, **kwargs) -> Any:
        """Retry an exchange call with exponential back-off."""
        for attempt in range(retries):
            try:
                return await asyncio.wait_for(
                    fn(*args, **kwargs),
                    timeout=self._request_timeout_seconds,
                )
            except asyncio.TimeoutError:
                wait = 2 ** attempt
                self.logger.warning(
                    f"[{self.name}] Timeout calling {getattr(fn, '__name__', 'exchange_api')} "
                    f"({self._request_timeout_seconds}s), retry {attempt+1}/{retries} in {wait}s"
                )
                await asyncio.sleep(wait)
            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as e:
                wait = 2 ** attempt
                self.logger.warning(f"[{self.name}] Retry {attempt+1}/{retries} after {e}, sleeping {wait}s")
                await asyncio.sleep(wait)
            except ccxt.ExchangeError as e:
                self.logger.error(f"[{self.name}] Exchange error: {e}")
                raise
        raise RuntimeError(f"[{self.name}] All {retries} retries exhausted")

    def _is_balance_related_error(self, exc: Exception) -> bool:
        s = str(exc).upper()
        return (
            "BALANCE_NOT_ENOUGH" in s
            or "INSUFFICIENT_AVAILABLE" in s
            or "INSUFFICIENT" in s
            or "NOT ENOUGH" in s
        )

    def _base_asset(self, symbol: str) -> str:
        """Extract the base asset from any symbol format (BTC/USDT:USDT, BTC/USDT, BTC_USDT, BTCUSDT)."""
        # Strip settlement suffix first
        s = symbol.split(":")[0]   # BTC/USDT:USDT → BTC/USDT
        if "/" in s:
            return s.split("/")[0]  # BTC/USDT → BTC
        if "_" in s:
            return s.split("_")[0]  # BTC_USDT → BTC
        # Fallback: raw like BTCUSDT
        return s.replace("USDT", "")

    def _futures_symbol(self, symbol: str) -> str:
        """Normalize to the ccxt linear perpetual symbol (always BASE/USDT:USDT)."""
        base = self._base_asset(symbol)
        return f"{base}/USDT:USDT"

    def _spot_symbol(self, symbol: str) -> str:
        base = self._base_asset(symbol)
        return f"{base}/USDT"

    def _symbol_norm(self, symbol: str) -> str:
        """Normalize symbol text for robust blacklist matching across formats."""
        s = str(symbol or "").upper()
        return "".join(ch for ch in s if ch.isalnum())

    async def _load_precision(self, symbol: str) -> dict[str, Any]:
        """Cache market info for precision rounding."""
        if symbol not in self._precision_cache:
            exch = self.futures_exchange if ":" in symbol else self.spot_exchange
            await exch.load_markets()
            if symbol in exch.markets:
                mkt = exch.markets[symbol]
                self._precision_cache[symbol] = mkt
        return self._precision_cache.get(symbol, {})

    async def round_qty(self, symbol: str, qty: float) -> float:
        mkt = await self._load_precision(symbol)
        precision = mkt.get("precision", {}).get("amount")
        if precision is None:
            return round(qty, 8)
        if isinstance(precision, int):
            return round(qty, precision)
        # Some exchanges give step size as a float
        step = float(precision)
        return math.floor(qty / step) * step

    async def round_price(self, symbol: str, price: float) -> float:
        mkt = await self._load_precision(symbol)
        precision = mkt.get("precision", {}).get("price")
        if precision is None:
            return round(price, 8)
        if isinstance(precision, int):
            return round(price, precision)
        step = float(precision)
        return math.floor(price / step) * step

    async def get_contract_size(self, symbol: str) -> float:
        """Return futures contract size in base units (1.0 if unavailable)."""
        fut_sym = self._futures_symbol(symbol)
        mkt = await self._load_precision(fut_sym)
        try:
            contract_size = float(mkt.get("contractSize") or 1.0)
        except Exception:
            contract_size = 1.0
        return max(contract_size, 1e-12)

    # ── market data ─────────────────────────────────────────────────

    async def get_spot_price(self, symbol: str) -> float:
        spot_sym = self._spot_symbol(symbol)
        ticker = await self._retry(self.spot_exchange.fetch_ticker, spot_sym)
        return float(ticker["last"])

    async def get_futures_price(self, symbol: str) -> float:
        fut_sym = self._futures_symbol(symbol)
        ticker = await self._retry(self.futures_exchange.fetch_ticker, fut_sym)
        return float(ticker["last"])

    async def get_funding_rate(self, symbol: str) -> dict[str, float]:
        """Get current funding rate for a symbol.

        Returns:
            Dict with 'rate' (per-period), 'daily_rate' (annualized to daily %).
        """
        fut_sym = self._futures_symbol(symbol)
        info = await self._retry(self.futures_exchange.fetch_funding_rate, fut_sym)
        rate = float(info.get("fundingRate", 0))

        # Gate.io funds every 8h, Binance every 8h.  Daily = rate * 3.
        daily_rate = rate * 3 * 100  # as percentage
        return {"rate": rate, "daily_rate": daily_rate}

    async def scan_funding_rates(self) -> list[dict[str, Any]]:
        """Scan all perpetual markets for funding rates.

        Returns a list sorted by |daily_rate| descending.
        """
        await asyncio.wait_for(
            self.futures_exchange.load_markets(),
            timeout=self._request_timeout_seconds,
        )
        await asyncio.wait_for(
            self.spot_exchange.load_markets(),
            timeout=self._request_timeout_seconds,
        )
        results: list[dict[str, Any]] = []

        try:
            all_rates = await self._retry(self.futures_exchange.fetch_funding_rates)
        except Exception as e:
            self.logger.warning(f"[{self.name}] Bulk funding rate fetch failed, falling back: {e}")
            return results

        quote = self.config.filters.quote_currency
        blacklist_norm = {self._symbol_norm(s) for s in self.config.filters.blacklist}
        blacklist_base = {self._base_asset(s).upper() for s in self.config.filters.blacklist}
        stablecoin_base = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDE", "DAI", "USDP", "USDD"}

        for sym, info in all_rates.items():
            # Filter to linear USDT perpetuals
            mkt = self.futures_exchange.markets.get(sym)
            if not mkt or not mkt.get("linear"):
                continue
            if mkt.get("quote", "") != quote:
                continue

            raw_symbol = mkt.get("symbol", sym)
            market_id = mkt.get("id", "")
            if (
                self._symbol_norm(raw_symbol) in blacklist_norm
                or self._symbol_norm(sym) in blacklist_norm
                or self._symbol_norm(market_id) in blacklist_norm
            ):
                continue
            # This strategy needs both futures and spot markets; skip futures-only listings.
            base = mkt.get("base", "")
            if base.upper() in stablecoin_base:
                continue
            if base.upper() in blacklist_base:
                continue
            spot_symbol = f"{base}/{quote}"
            if spot_symbol not in self.spot_exchange.markets:
                continue

            rate = float(info.get("fundingRate", 0))
            daily = rate * 3 * 100

            results.append({
                "symbol": raw_symbol,
                "base": base,
                "rate": rate,
                "daily_rate": daily,
            })

        results.sort(key=lambda r: abs(r["daily_rate"]), reverse=True)
        return results[: self.config.scan.top_coins_to_scan]

    # ── spot orders ─────────────────────────────────────────────────

    async def buy_spot(self, symbol: str, qty: float, use_limit: bool = False) -> dict[str, Any]:
        spot_sym = self._spot_symbol(symbol)
        qty = await self.round_qty(spot_sym, qty)
        self.logger.info(f"[{self.name}] BUY spot {qty} {spot_sym}")
        gate_params = {"unifiedAccount": True} if (self.name == "gateio" and self.config.unified_account) else {}
        if use_limit:
            price = await self.get_spot_price(symbol)
            price *= 1 + self.config.execution.limit_order_offset_percent / 100
            price = await self.round_price(spot_sym, price)
            return await self._retry(
                self.spot_exchange.create_limit_buy_order, spot_sym, qty, price, gate_params,
            )
        if self.name == "gateio":
            # Gate expects market buy in quote-cost terms; use cost route first.
            spot_px = await self.get_spot_price(symbol)
            cost = max(0.01, round(qty * spot_px, 6))
            try:
                return await self._retry(
                    self.spot_exchange.create_market_buy_order_with_cost, spot_sym, cost, gate_params
                )
            except Exception:
                # Fallback: aggressive near-market IOC-like limit buy.
                price = await self.round_price(spot_sym, spot_px * 1.005)
                return await self._retry(
                    self.spot_exchange.create_limit_buy_order, spot_sym, qty, price, gate_params,
                )
        return await self._retry(
            self.spot_exchange.create_market_buy_order, spot_sym, qty, gate_params,
        )

    async def sell_spot(self, symbol: str, qty: float, use_limit: bool = False) -> dict[str, Any]:
        spot_sym = self._spot_symbol(symbol)
        qty = await self.round_qty(spot_sym, qty)
        self.logger.info(f"[{self.name}] SELL spot {qty} {spot_sym}")
        gate_params = {"unifiedAccount": True} if (self.name == "gateio" and self.config.unified_account) else {}
        if use_limit:
            price = await self.get_spot_price(symbol)
            price *= 1 - self.config.execution.limit_order_offset_percent / 100
            price = await self.round_price(spot_sym, price)
            return await self._retry(
                self.spot_exchange.create_limit_sell_order, spot_sym, qty, price, gate_params,
            )
        return await self._retry(
            self.spot_exchange.create_market_sell_order, spot_sym, qty, gate_params,
        )

    # ── futures orders ──────────────────────────────────────────────

    async def open_short_futures(self, symbol: str, qty: float, use_limit: bool = False) -> dict[str, Any]:
        fut_sym = self._futures_symbol(symbol)
        qty = await self.round_qty(fut_sym, qty)
        self.logger.info(f"[{self.name}] SHORT futures {qty} {fut_sym}")
        params: dict[str, Any] = {}
        # Binance one-way mode (default) does not use positionSide — sending it
        # causes error -4061. Use reduceOnly for closes instead.
        if self.name == "gateio" and self.config.unified_account:
            params["unifiedAccount"] = True
        if use_limit:
            price = await self.get_futures_price(symbol)
            price *= 1 - self.config.execution.limit_order_offset_percent / 100
            price = await self.round_price(fut_sym, price)
            return await self._retry(
                self.futures_exchange.create_limit_sell_order, fut_sym, qty, price, params=params
            )
        return await self._retry(
            self.futures_exchange.create_market_sell_order, fut_sym, qty, params=params
        )

    async def close_short_futures(self, symbol: str, qty: float) -> dict[str, Any]:
        fut_sym = self._futures_symbol(symbol)
        qty = await self.round_qty(fut_sym, qty)
        self.logger.info(f"[{self.name}] CLOSE short futures {qty} {fut_sym}")
        params: dict[str, Any] = {"reduceOnly": True}
        if self.name == "gateio" and self.config.unified_account:
            params["unifiedAccount"] = True
        return await self._retry(
            self.futures_exchange.create_market_buy_order, fut_sym, qty, params=params
        )

    async def open_long_futures(self, symbol: str, qty: float, use_limit: bool = False) -> dict[str, Any]:
        fut_sym = self._futures_symbol(symbol)
        qty = await self.round_qty(fut_sym, qty)
        self.logger.info(f"[{self.name}] LONG futures {qty} {fut_sym}")
        params: dict[str, Any] = {}
        if self.name == "gateio" and self.config.unified_account:
            params["unifiedAccount"] = True
        if use_limit:
            price = await self.get_futures_price(symbol)
            price *= 1 + self.config.execution.limit_order_offset_percent / 100
            price = await self.round_price(fut_sym, price)
            return await self._retry(
                self.futures_exchange.create_limit_buy_order, fut_sym, qty, price, params=params
            )
        return await self._retry(
            self.futures_exchange.create_market_buy_order, fut_sym, qty, params=params
        )

    async def close_long_futures(self, symbol: str, qty: float) -> dict[str, Any]:
        fut_sym = self._futures_symbol(symbol)
        qty = await self.round_qty(fut_sym, qty)
        self.logger.info(f"[{self.name}] CLOSE long futures {qty} {fut_sym}")
        params: dict[str, Any] = {"reduceOnly": True}
        if self.name == "gateio" and self.config.unified_account:
            params["unifiedAccount"] = True
        return await self._retry(
            self.futures_exchange.create_market_sell_order, fut_sym, qty, params=params
        )

    # ── margin / borrow ─────────────────────────────────────────────

    async def borrow_margin(self, asset: str, amount: float) -> dict[str, Any]:
        self.logger.info(f"[{self.name}] BORROW margin {amount} {asset}")
        # Binance margin borrow is via SAPI (spot side), Gate.io is via futures/swap.
        exchange = self.spot_exchange if self.name == "binance" else self.futures_exchange
        return await self._retry(exchange.borrow_margin, asset, amount)

    async def repay_margin(self, asset: str, amount: float) -> dict[str, Any]:
        self.logger.info(f"[{self.name}] REPAY margin {amount} {asset}")
        # Binance margin repay is via SAPI (spot side), Gate.io is via futures/swap.
        exchange = self.spot_exchange if self.name == "binance" else self.futures_exchange
        return await self._retry(exchange.repay_margin, asset, amount)

    async def get_borrowable(self, asset: str) -> float:
        """Get max borrowable amount for an asset.

        Binance: uses SAPI /margin/maxBorrowable (cross-margin).
        Gate.io: uses fetch_cross_borrow_rate which includes the 'available' field.
        """
        if self.name == "binance":
            try:
                # Binance SAPI cross-margin max borrowable endpoint.
                # Must use the spot exchange instance (SAPI endpoints are on the spot/margin API).
                resp = await self._retry(
                    self.spot_exchange.sapiGetMarginMaxBorrowable,
                    {"asset": asset},
                )
                # Response shape: {"amount": "123.45", "borrowLimit": "999"}
                amount = float(resp.get("amount", 0) or 0)
                return amount
            except Exception as e:
                self.logger.warning(f"[{self.name}] Could not fetch borrowable for {asset}: {e}")
                return 0.0
        else:
            # Gate.io: fetch_cross_borrow_rate returns available borrowable in the 'available' key.
            try:
                info = await self._retry(
                    self.spot_exchange.fetch_cross_borrow_rate, asset
                )
                return float(info.get("available", 0) or 0)
            except Exception as e:
                self.logger.warning(f"[{self.name}] Could not fetch borrowable for {asset}: {e}")
                return 0.0

    # ── balances & account ──────────────────────────────────────────

    async def get_spot_balance(self, asset: str = "USDT") -> dict[str, float]:
        # For Gate.io unified accounts the spot and futures share one balance pool.
        # The legacy spot endpoint (type=spot) returns only the classic spot sub-account
        # (~empty), and omitting type causes timeouts.  Use the futures/swap endpoint
        # (which works and already has unifiedAccount=True support) as the source of truth.
        if self.name == "gateio" and self.config.unified_account:
            return await self.get_futures_balance(asset)
        params: dict[str, Any] = {}
        if self.name == "gateio":
            params["type"] = "spot"
        balance = await self._retry(self.spot_exchange.fetch_balance, params=params)
        free = float(balance.get(asset, {}).get("free", 0))
        total = float(balance.get(asset, {}).get("total", 0))
        return {"free": free, "total": total}

    async def get_futures_balance(self, asset: str = "USDT") -> dict[str, float]:
        balance_type = "future" if self.name == "binance" else "swap"
        params: dict[str, Any] = {"type": balance_type}
        # Gate.io's fetch_balance automatically fetches unified margins if the API
        # credentials are for a unified account. Explicitly sending unifiedAccount=True
        # breaks CCXT's dict parsing (missing USDT key).
        balance = await self._retry(self.futures_exchange.fetch_balance, params=params)
        free = float(balance.get(asset, {}).get("free", 0))
        total = float(balance.get(asset, {}).get("total", 0))
        return {"free": free, "total": total}

    async def get_total_equity(self) -> float:
        """Estimated total equity in USDT with short-lived caching."""
        now = time.time()
        if self._equity_cache_value is not None and (now - self._equity_cache_ts) < 30:
            return self._equity_cache_value

        # Gate.io provides a direct total-balance endpoint in USDT.
        if self.name == "gateio":
            try:
                raw = await self._retry(self.futures_exchange.privateWalletGetTotalBalance)
                amount = float(raw.get("total", {}).get("amount", 0) or 0)
                pnl = float(raw.get("total", {}).get("unrealised_pnl", 0) or 0)
                total = amount + pnl
                self._equity_cache_value = total
                self._equity_cache_ts = now
                return total
            except Exception as e:
                self.logger.warning(f"[{self.name}] Total balance API failed, fallback estimation: {e}")

        # Fallback for Binance and any API failures: spot marked-to-USDT + futures USDT.
        spot_bal = await self._retry(self.spot_exchange.fetch_balance)
        spot_total = await self._estimate_spot_total_usdt(spot_bal)

        futures = await self.get_futures_balance()
        total = spot_total + futures["total"]
        self._equity_cache_value = total
        self._equity_cache_ts = now
        return total

    async def get_equity_breakdown(self) -> dict[str, float]:
        """Get equity breakdown into spot and futures."""
        if self.name == "gateio":
            try:
                raw = await self._retry(self.futures_exchange.privateWalletGetTotalBalance)
                total_amt = float(raw.get("total", {}).get("amount", 0) or 0)
                total_pnl = float(raw.get("total", {}).get("unrealised_pnl", 0) or 0)
                total_eq = total_amt + total_pnl
                
                details = raw.get("details", {})
                futures_amt = float(details.get("futures", {}).get("amount", 0) or 0)
                futures_pnl = float(details.get("futures", {}).get("unrealised_pnl", 0) or 0)
                futures_eq = futures_amt + futures_pnl
                
                # Gate.io has 'finance', 'delivery', etc. We roll everything non-futures into 'spot'
                # to ensure Spot + Futures = Total
                spot = max(0.0, total_eq - futures_eq)
                
                # The user wants "Futures" replaced with "Free USDT" for Gate.io in the UI.
                # We fetch spot available USDT and pass it under the "futures" key,
                # letting the UI rename the label based on the exchange.
                spot_balance = await self._retry(self.spot_exchange.fetch_balance)
                free_usdt = float(spot_balance.get("USDT", {}).get("free", 0) or 0)
                
                return {"spot": spot, "futures": free_usdt}
            except Exception:
                pass

        # Fallback for Binance and Gate.io errors
        try:
            spot_bal = await self._retry(self.spot_exchange.fetch_balance)
            spot_total = await self._estimate_spot_total_usdt(spot_bal)
            futures = await self.get_futures_balance()
            futures_total = futures["total"]
            return {"spot": spot_total, "futures": futures_total}
        except Exception:
            return {"spot": 0.0, "futures": 0.0}

    async def _estimate_spot_total_usdt(self, balance: dict[str, Any]) -> float:
        """Mark non-USDT spot holdings to USDT using spot tickers."""
        totals = balance.get("total", {})
        if not isinstance(totals, dict):
            return float(balance.get("USDT", {}).get("total", 0) or 0)

        await self.spot_exchange.load_markets()
        total_usdt = 0.0
        for asset, amount in totals.items():
            if not isinstance(amount, (int, float)) or abs(amount) < 1e-12:
                continue
            if asset == "USDT":
                total_usdt += float(amount)
                continue

            symbol = f"{asset}/USDT"
            if symbol not in self.spot_exchange.markets:
                continue

            try:
                # Spot valuation can include delisted/unsupported symbols in wallet dust.
                # Skip quietly instead of producing noisy exchange errors.
                ticker = await self.spot_exchange.fetch_ticker(symbol)
                price = float(ticker.get("last") or ticker.get("close") or 0)
                if price > 0:
                    total_usdt += float(amount) * price
            except Exception:
                continue
        return total_usdt

    async def get_futures_account(self) -> dict[str, Any]:
        """Get raw futures account info for margin calculations."""
        balance_type = "future" if self.name == "binance" else "swap"
        return await self._retry(
            self.futures_exchange.fetch_balance, params={"type": balance_type}
        )

    async def get_futures_positions(self) -> list[dict[str, Any]]:
        """Get open futures positions."""
        try:
            return await self._retry(self.futures_exchange.fetch_positions)
        except Exception as e:
            self.logger.warning(f"[{self.name}] Could not fetch futures positions: {e}")
            return []

    # ── transfers ───────────────────────────────────────────────────

    async def transfer_spot_to_futures(self, asset: str, amount: float) -> Any:
        self.logger.info(f"[{self.name}] Transfer {amount} {asset} spot → futures")
        return await self._retry(
            self.futures_exchange.transfer, asset, amount, "spot", "future"
        )

    async def transfer_futures_to_spot(self, asset: str, amount: float) -> Any:
        self.logger.info(f"[{self.name}] Transfer {amount} {asset} futures → spot")
        return await self._retry(
            self.futures_exchange.transfer, asset, amount, "future", "spot"
        )

    async def safe_transfer_spot_to_futures(
        self,
        asset: str,
        amount: float,
        min_amount: float = 0.2,
        reduction: float = 0.7,
        max_attempts: int = 4,
    ) -> float:
        """Try transferring with size backoff. Returns transferred amount, 0 on failure."""
        amt = max(0.0, float(amount))
        if amt <= 0:
            return 0.0
        for attempt in range(max_attempts):
            if amt < min_amount:
                break
            try:
                self.logger.info(f"[{self.name}] Transfer {amt} {asset} spot → futures")
                await self.futures_exchange.transfer(asset, amt, "spot", "future")
                return amt
            except Exception as e:
                if self._is_balance_related_error(e):
                    self.logger.warning(
                        f"[{self.name}] spot→futures transfer insufficient for ${amt:.2f}; "
                        f"retrying smaller amount"
                    )
                    amt *= reduction
                    continue
                raise
        return 0.0

    async def safe_transfer_futures_to_spot(
        self,
        asset: str,
        amount: float,
        min_amount: float = 0.2,
        reduction: float = 0.7,
        max_attempts: int = 4,
    ) -> float:
        """Try transferring with size backoff. Returns transferred amount, 0 on failure."""
        amt = max(0.0, float(amount))
        if amt <= 0:
            return 0.0
        for attempt in range(max_attempts):
            if amt < min_amount:
                break
            try:
                self.logger.info(f"[{self.name}] Transfer {amt} {asset} futures → spot")
                await self.futures_exchange.transfer(asset, amt, "future", "spot")
                return amt
            except Exception as e:
                if self._is_balance_related_error(e):
                    self.logger.warning(
                        f"[{self.name}] futures→spot transfer insufficient for ${amt:.2f}; "
                        f"retrying smaller amount"
                    )
                    amt *= reduction
                    continue
                raise
        return 0.0

    # ── leverage ────────────────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        fut_sym = self._futures_symbol(symbol)
        try:
            await self._retry(self.futures_exchange.set_leverage, leverage, fut_sym)
            self.logger.info(f"[{self.name}] Set leverage {leverage}x on {fut_sym}")
        except Exception as e:
            self.logger.warning(f"[{self.name}] Could not set leverage on {fut_sym}: {e}")


class DryRunClient(ExchangeClient):
    """Paper-trading wrapper — logs trades but never hits the exchange."""

    def __init__(self, config: ExchangeConfig, paper_equity: float = 1000.0,
                 logger: logging.Logger | None = None) -> None:
        # Don't call super().__init__ — we don't want a real ccxt exchange
        self.config = config
        self.name = config.name
        self.logger = logger or logging.getLogger(f"dry.{self.name}")
        self._precision_cache: dict[str, dict[str, Any]] = {}
        self._request_count = 0
        self._last_request_time = 0.0
        self._paper_equity = paper_equity
        self._sim_positions: dict[str, dict] = {}

        # Still need a real exchange object for market data only
        exchange_cls = getattr(ccxt, ExchangeClient.EXCHANGE_MAP[self.name])
        self.futures_exchange = exchange_cls({"enableRateLimit": True})
        self.spot_exchange = self.futures_exchange
        self.exchange = self.futures_exchange

    # Override all mutating methods to no-op
    async def buy_spot(self, symbol: str, qty: float, use_limit: bool = False) -> dict[str, Any]:
        price = await self.get_spot_price(symbol)
        self.logger.info(f"[DRY {self.name}] BUY spot {qty} @ ${price:.4f}")
        return {"id": "dry-run", "symbol": symbol, "side": "buy", "amount": qty, "price": price, "status": "filled"}

    async def sell_spot(self, symbol: str, qty: float, use_limit: bool = False) -> dict[str, Any]:
        price = await self.get_spot_price(symbol)
        self.logger.info(f"[DRY {self.name}] SELL spot {qty} @ ${price:.4f}")
        return {"id": "dry-run", "symbol": symbol, "side": "sell", "amount": qty, "price": price, "status": "filled"}

    async def open_short_futures(self, symbol: str, qty: float, use_limit: bool = False) -> dict[str, Any]:
        price = await self.get_futures_price(symbol)
        self.logger.info(f"[DRY {self.name}] SHORT futures {qty} @ ${price:.4f}")
        return {"id": "dry-run", "symbol": symbol, "side": "sell", "amount": qty, "price": price, "status": "filled"}

    async def close_short_futures(self, symbol: str, qty: float) -> dict[str, Any]:
        price = await self.get_futures_price(symbol)
        self.logger.info(f"[DRY {self.name}] CLOSE SHORT {qty} @ ${price:.4f}")
        return {"id": "dry-run", "symbol": symbol, "side": "buy", "amount": qty, "price": price, "status": "filled"}

    async def open_long_futures(self, symbol: str, qty: float, use_limit: bool = False) -> dict[str, Any]:
        price = await self.get_futures_price(symbol)
        self.logger.info(f"[DRY {self.name}] LONG futures {qty} @ ${price:.4f}")
        return {"id": "dry-run", "symbol": symbol, "side": "buy", "amount": qty, "price": price, "status": "filled"}

    async def close_long_futures(self, symbol: str, qty: float) -> dict[str, Any]:
        price = await self.get_futures_price(symbol)
        self.logger.info(f"[DRY {self.name}] CLOSE LONG {qty} @ ${price:.4f}")
        return {"id": "dry-run", "symbol": symbol, "side": "sell", "amount": qty, "price": price, "status": "filled"}

    async def borrow_margin(self, asset: str, amount: float) -> dict[str, Any]:
        self.logger.info(f"[DRY {self.name}] BORROW {amount} {asset}")
        return {"id": "dry-run"}

    async def repay_margin(self, asset: str, amount: float) -> dict[str, Any]:
        self.logger.info(f"[DRY {self.name}] REPAY {amount} {asset}")
        return {"id": "dry-run"}

    async def get_borrowable(self, asset: str) -> float:
        return 999_999.0

    async def get_spot_balance(self, asset: str = "USDT") -> dict[str, float]:
        half = self._paper_equity / 2
        return {"free": half, "total": half}

    async def get_futures_balance(self, asset: str = "USDT") -> dict[str, float]:
        half = self._paper_equity / 2
        return {"free": half, "total": half}

    async def get_total_equity(self) -> float:
        return self._paper_equity

    async def get_futures_account(self) -> dict[str, Any]:
        # Keep margin monitor fully offline in dry-run mode.
        half = self._paper_equity / 2
        return {
            "USDT": {"free": half, "total": half},
            "total": {"USDT": half},
            "info": {"totalMaintMargin": 0},
        }

    async def get_futures_positions(self) -> list[dict[str, Any]]:
        return []

    async def transfer_spot_to_futures(self, asset: str, amount: float) -> Any:
        self.logger.info(f"[DRY {self.name}] Transfer {amount} spot → futures")
        return {}

    async def transfer_futures_to_spot(self, asset: str, amount: float) -> Any:
        self.logger.info(f"[DRY {self.name}] Transfer {amount} futures → spot")
        return {}

    async def safe_transfer_spot_to_futures(
        self, asset: str, amount: float, min_amount: float = 0.2,
        reduction: float = 0.7, max_attempts: int = 4,
    ) -> float:
        self.logger.info(f"[DRY {self.name}] Safe transfer {amount} spot → futures")
        return float(max(0.0, amount))

    async def safe_transfer_futures_to_spot(
        self, asset: str, amount: float, min_amount: float = 0.2,
        reduction: float = 0.7, max_attempts: int = 4,
    ) -> float:
        self.logger.info(f"[DRY {self.name}] Safe transfer {amount} futures → spot")
        return float(max(0.0, amount))

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        self.logger.info(f"[DRY {self.name}] Set leverage {leverage}x on {symbol}")
