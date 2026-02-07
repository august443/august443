#!/usr/bin/env python3
"""
Kalshi + Coinbase Arbitrage Bot with Web UI
Real API Integration for Prediction Market Arbitrage
Based on IMDEA Networks research methodology
"""

import asyncio
import time
import random
import os
import hmac
import hashlib
import base64
import json
import logging
from collections import deque
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
import threading
import aiohttp
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Config:
    """Bot configuration - set via environment variables or modify defaults"""
    # Kalshi API
    KALSHI_API_KEY: str = os.getenv("KALSHI_API_KEY", "")
    KALSHI_PRIVATE_KEY: str = os.getenv("KALSHI_PRIVATE_KEY", "")  # RSA private key path or content
    KALSHI_BASE_URL: str = os.getenv("KALSHI_BASE_URL", "https://demo-api.kalshi.co")  # Use demo by default

    # Coinbase API (for Coinbase main)
    COINBASE_API_KEY: str = os.getenv("COINBASE_API_KEY", "")
    COINBASE_API_SECRET: str = os.getenv("COINBASE_API_SECRET", "")

    # Polymarket API (Polygon-based prediction market)
    POLYMARKET_API_KEY: str = os.getenv("POLYMARKET_API_KEY", "")
    POLYMARKET_PRIVATE_KEY: str = os.getenv("POLYMARKET_PRIVATE_KEY", "")  # Wallet private key for signing
    POLYMARKET_CLOB_URL: str = os.getenv("POLYMARKET_CLOB_URL", "https://clob.polymarket.com")
    POLYMARKET_GAMMA_URL: str = os.getenv("POLYMARKET_GAMMA_URL", "https://gamma-api.polymarket.com")

    # Base L2 (Coinbase Layer 2) - for wallet integration
    BASE_RPC_URL: str = os.getenv("BASE_RPC_URL", "https://mainnet.base.org")
    BASE_WALLET_ADDRESS: str = os.getenv("BASE_WALLET_ADDRESS", "0x8475F6aAc937FdA3549431Dc4A72aC067D4E0678")
    BASE_PRIVATE_KEY: str = os.getenv("BASE_PRIVATE_KEY", "")  # For signing transactions

    # Scanning settings
    SCAN_INTERVAL_MS: int = int(os.getenv("SCAN_INTERVAL_MS", "1000"))  # 1 second default for real API
    TOP_MARKETS: int = int(os.getenv("TOP_MARKETS", "50"))

    # Profit thresholds
    MIN_PROFIT_THRESHOLD: float = float(os.getenv("MIN_PROFIT_THRESHOLD", "0.005"))  # $0.005 minimum
    MIN_ALERT_PROFIT: float = float(os.getenv("MIN_ALERT_PROFIT", "1.0"))  # $1 for alerts
    HIGH_URGENCY_ROI: float = 0.10  # 10%
    MEDIUM_URGENCY_ROI: float = 0.05  # 5%

    # Risk management
    MAX_RISK_SCORE: float = 0.7
    POSITION_SIZE: float = float(os.getenv("POSITION_SIZE", "10.0"))  # $10 per arb (5% of $200)

    # Strategy toggles
    ENABLE_SINGLE_CONDITION: bool = True
    ENABLE_MULTI_OUTCOME: bool = True  # Similar to NegRisk for multi-outcome markets
    ENABLE_WHALE_TRACKING: bool = True  # Track large trades
    ENABLE_POLYMARKET: bool = True  # Enable Polymarket scanning
    ENABLE_CROSS_MARKET: bool = True  # Enable cross-market arbitrage (Kalshi vs Polymarket)

    # Whale tracking settings
    WHALE_THRESHOLD: float = float(os.getenv("WHALE_THRESHOLD", "5000"))  # $5K minimum
    WHALE_LOOKBACK_TRADES: int = 50  # Recent trades to analyze

    # Cross-market arbitrage settings
    CROSS_MARKET_MIN_SPREAD: float = float(os.getenv("CROSS_MARKET_MIN_SPREAD", "0.02"))  # 2% minimum spread

    # Parallel request settings
    MAX_CONCURRENT_REQUESTS: int = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))

    # Mode
    DEMO_MODE: bool = os.getenv("DEMO_MODE", "true").lower() == "true"

    # ==========================================================================
    # RISK MANAGEMENT - Configured for $200 starting capital
    # Entry is everything. Small trades, tight limits, survive to compound.
    # ==========================================================================

    # Starting capital
    STARTING_CAPITAL: float = float(os.getenv("STARTING_CAPITAL", "200.0"))

    # Per-trade limits (5% of capital per trade max)
    MAX_TRADE_SIZE: float = float(os.getenv("MAX_TRADE_SIZE", "10.0"))  # $10 per trade
    MIN_EDGE_THRESHOLD: float = float(os.getenv("MIN_EDGE_THRESHOLD", "0.005"))  # 0.5% min edge
    MAX_SLIPPAGE: float = float(os.getenv("MAX_SLIPPAGE", "0.01"))  # 1% max slippage

    # Position limits (stay small, stay alive)
    MAX_POSITION_PER_MARKET: float = float(os.getenv("MAX_POSITION_PER_MARKET", "25.0"))  # $25/market
    MAX_TOTAL_EXPOSURE: float = float(os.getenv("MAX_TOTAL_EXPOSURE", "150.0"))  # 75% of capital
    MAX_CONCURRENT_ORDERS: int = int(os.getenv("MAX_CONCURRENT_ORDERS", "3"))

    # Timing - fast rejection of bad data
    ORDER_TIMEOUT_MS: int = int(os.getenv("ORDER_TIMEOUT_MS", "5000"))
    STALE_PRICE_MS: int = int(os.getenv("STALE_PRICE_MS", "500"))

    # Loss limits (protect the $200)
    MAX_DAILY_LOSS: float = float(os.getenv("MAX_DAILY_LOSS", "20.0"))  # 10% daily max loss
    MAX_DRAWDOWN_PCT: float = float(os.getenv("MAX_DRAWDOWN_PCT", "0.15"))  # 15% max drawdown

    # Execution mode
    LIVE_EXECUTION: bool = os.getenv("LIVE_EXECUTION", "false").lower() == "true"


config = Config()


# =============================================================================
# POSITION MANAGER - Track positions and exposure
# =============================================================================

@dataclass
class Position:
    """Represents a position in a single market"""
    ticker: str
    platform: str           # "kalshi" or "polymarket"
    yes_contracts: int = 0
    no_contracts: int = 0
    avg_yes_price: float = 0.0
    avg_no_price: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    last_updated: float = 0.0


class PositionManager:
    """
    Tracks positions across all markets and platforms.
    Essential for risk management and P&L tracking.
    """

    def __init__(self):
        self.positions: Dict[str, Position] = {}  # key: "platform:ticker"
        self.pending_orders: Dict[str, Dict] = {}  # key: order_id
        self.daily_pnl: float = 0.0
        self.daily_start_balance: float = 0.0
        self.peak_balance: float = 0.0
        self.total_exposure: float = 0.0

    def _key(self, platform: str, ticker: str) -> str:
        return f"{platform}:{ticker}"

    def get_position(self, platform: str, ticker: str) -> Optional[Position]:
        """Get position for a specific market"""
        return self.positions.get(self._key(platform, ticker))

    def update_position(
        self,
        platform: str,
        ticker: str,
        side: str,
        action: str,
        count: int,
        price: float
    ):
        """Update position from a fill"""
        key = self._key(platform, ticker)

        if key not in self.positions:
            self.positions[key] = Position(ticker=ticker, platform=platform)

        pos = self.positions[key]
        pos.last_updated = time.time()

        if side == "yes":
            if action == "buy":
                # Update average price
                total_contracts = pos.yes_contracts + count
                if total_contracts > 0:
                    pos.avg_yes_price = (
                        (pos.avg_yes_price * pos.yes_contracts + price * count) / total_contracts
                    )
                pos.yes_contracts = total_contracts
            else:  # sell
                pos.yes_contracts = max(0, pos.yes_contracts - count)
                # Realize P&L
                pnl = (price - pos.avg_yes_price) * count
                pos.realized_pnl += pnl
                self.daily_pnl += pnl
        else:  # no
            if action == "buy":
                total_contracts = pos.no_contracts + count
                if total_contracts > 0:
                    pos.avg_no_price = (
                        (pos.avg_no_price * pos.no_contracts + price * count) / total_contracts
                    )
                pos.no_contracts = total_contracts
            else:  # sell
                pos.no_contracts = max(0, pos.no_contracts - count)
                pnl = (price - pos.avg_no_price) * count
                pos.realized_pnl += pnl
                self.daily_pnl += pnl

        self._recalc_exposure()

    def _recalc_exposure(self):
        """Recalculate total exposure"""
        total = 0.0
        for pos in self.positions.values():
            # Exposure = contracts * average price
            total += pos.yes_contracts * pos.avg_yes_price
            total += pos.no_contracts * pos.avg_no_price
        self.total_exposure = total

    def get_market_exposure(self, platform: str, ticker: str) -> float:
        """Get exposure for a specific market"""
        pos = self.get_position(platform, ticker)
        if not pos:
            return 0.0
        return (pos.yes_contracts * pos.avg_yes_price +
                pos.no_contracts * pos.avg_no_price)

    def add_pending_order(self, order_id: str, order_data: Dict):
        """Track a pending order"""
        self.pending_orders[order_id] = {
            **order_data,
            "created_time": time.time()
        }

    def remove_pending_order(self, order_id: str):
        """Remove a pending order (filled or cancelled)"""
        self.pending_orders.pop(order_id, None)

    def get_pending_count(self) -> int:
        """Get number of pending orders"""
        return len(self.pending_orders)

    def reset_daily(self, current_balance: float):
        """Reset daily tracking (call at start of day)"""
        self.daily_pnl = 0.0
        self.daily_start_balance = current_balance
        self.peak_balance = current_balance

    def get_drawdown(self, current_balance: float) -> float:
        """Calculate current drawdown from peak"""
        if self.peak_balance <= 0:
            return 0.0
        self.peak_balance = max(self.peak_balance, current_balance)
        return (self.peak_balance - current_balance) / self.peak_balance

    def to_dict(self) -> Dict:
        """Export state for dashboard"""
        return {
            "positions": [
                {
                    "ticker": p.ticker,
                    "platform": p.platform,
                    "yes_contracts": p.yes_contracts,
                    "no_contracts": p.no_contracts,
                    "realized_pnl": p.realized_pnl
                }
                for p in self.positions.values()
                if p.yes_contracts > 0 or p.no_contracts > 0
            ],
            "pending_orders": len(self.pending_orders),
            "total_exposure": self.total_exposure,
            "daily_pnl": self.daily_pnl
        }


# =============================================================================
# RISK MANAGER - Pre-trade checks and limits
# =============================================================================

class RiskManager:
    """
    Enforces risk limits before order execution.
    All checks must pass before an order is placed.
    """

    def __init__(self, position_manager: PositionManager):
        self.pm = position_manager
        self.orders_today: int = 0
        self.last_order_time: float = 0
        self.circuit_breaker_active: bool = False
        self.circuit_breaker_reason: str = ""

    def check_all(
        self,
        platform: str,
        ticker: str,
        side: str,
        action: str,
        count: int,
        price: float,
        current_balance: float,
        price_timestamp: float = None
    ) -> tuple:
        """
        Run all pre-trade checks.
        Returns (passed: bool, reason: str)
        """
        checks = [
            self._check_circuit_breaker(),
            self._check_daily_loss(current_balance),
            self._check_drawdown(current_balance),
            self._check_position_limit(platform, ticker, count, price),
            self._check_exposure_limit(count, price),
            self._check_trade_size(count, price),
            self._check_pending_orders(),
            self._check_stale_price(price_timestamp),
        ]

        for passed, reason in checks:
            if not passed:
                return False, reason

        return True, "All checks passed"

    def _check_circuit_breaker(self) -> tuple:
        """Check if circuit breaker is active"""
        if self.circuit_breaker_active:
            return False, f"Circuit breaker active: {self.circuit_breaker_reason}"
        return True, ""

    def _check_daily_loss(self, current_balance: float) -> tuple:
        """Check daily loss limit"""
        if self.pm.daily_pnl < -config.MAX_DAILY_LOSS:
            self.circuit_breaker_active = True
            self.circuit_breaker_reason = f"Daily loss limit hit: ${-self.pm.daily_pnl:.2f}"
            return False, self.circuit_breaker_reason
        return True, ""

    def _check_drawdown(self, current_balance: float) -> tuple:
        """Check drawdown limit"""
        drawdown = self.pm.get_drawdown(current_balance)
        if drawdown > config.MAX_DRAWDOWN_PCT:
            self.circuit_breaker_active = True
            self.circuit_breaker_reason = f"Max drawdown exceeded: {drawdown:.1%}"
            return False, self.circuit_breaker_reason
        return True, ""

    def _check_position_limit(self, platform: str, ticker: str, count: int, price: float) -> tuple:
        """Check per-market position limit"""
        current_exposure = self.pm.get_market_exposure(platform, ticker)
        new_exposure = current_exposure + (count * price)
        if new_exposure > config.MAX_POSITION_PER_MARKET:
            return False, f"Position limit: ${new_exposure:.2f} > ${config.MAX_POSITION_PER_MARKET:.2f}"
        return True, ""

    def _check_exposure_limit(self, count: int, price: float) -> tuple:
        """Check total exposure limit"""
        new_total = self.pm.total_exposure + (count * price)
        if new_total > config.MAX_TOTAL_EXPOSURE:
            return False, f"Exposure limit: ${new_total:.2f} > ${config.MAX_TOTAL_EXPOSURE:.2f}"
        return True, ""

    def _check_trade_size(self, count: int, price: float) -> tuple:
        """Check single trade size limit"""
        trade_value = count * price
        if trade_value > config.MAX_TRADE_SIZE:
            return False, f"Trade size: ${trade_value:.2f} > ${config.MAX_TRADE_SIZE:.2f}"
        return True, ""

    def _check_pending_orders(self) -> tuple:
        """Check concurrent order limit"""
        if self.pm.get_pending_count() >= config.MAX_CONCURRENT_ORDERS:
            return False, f"Too many pending orders: {self.pm.get_pending_count()}"
        return True, ""

    def _check_stale_price(self, price_timestamp: float) -> tuple:
        """Check if price data is too old"""
        if price_timestamp is None:
            return True, ""  # No timestamp = skip check

        age_ms = (time.time() - price_timestamp) * 1000
        if age_ms > config.STALE_PRICE_MS:
            return False, f"Stale price: {age_ms:.0f}ms > {config.STALE_PRICE_MS}ms"
        return True, ""

    def reset_circuit_breaker(self):
        """Manually reset circuit breaker"""
        self.circuit_breaker_active = False
        self.circuit_breaker_reason = ""

    def reset_daily(self):
        """Reset daily counters"""
        self.orders_today = 0
        self.reset_circuit_breaker()


# =============================================================================
# KALSHI API CLIENT
# =============================================================================

class KalshiClient:
    """Client for Kalshi prediction market API"""

    def __init__(self, api_key: str = "", private_key: str = "", base_url: str = ""):
        self.api_key = api_key or config.KALSHI_API_KEY
        self.private_key = private_key or config.KALSHI_PRIVATE_KEY
        self.base_url = (base_url or config.KALSHI_BASE_URL).rstrip('/')
        self.session: Optional[aiohttp.ClientSession] = None
        self.token: Optional[str] = None
        self.token_expires: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def _get_headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def login(self) -> bool:
        """Authenticate with Kalshi API"""
        if not self.api_key:
            logger.warning("No Kalshi API key configured - running in read-only mode")
            return False

        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/login"

            # Kalshi uses email/password or API key authentication
            payload = {"email": self.api_key, "password": self.private_key}

            async with session.post(url, json=payload, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.token = data.get("token")
                    self.token_expires = time.time() + 1800  # 30 min expiry
                    logger.info("Kalshi authentication successful")
                    return True
                else:
                    error = await resp.text()
                    logger.error(f"Kalshi login failed: {resp.status} - {error}")
                    return False
        except Exception as e:
            logger.error(f"Kalshi login error: {e}")
            return False

    async def get_markets(self, limit: int = 100, status: str = "open") -> List[Dict]:
        """Fetch active markets from Kalshi"""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/markets"
            params = {"limit": limit, "status": status}

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("markets", [])
                else:
                    logger.error(f"Failed to fetch markets: {resp.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []

    async def get_market(self, ticker: str) -> Optional[Dict]:
        """Fetch single market details"""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/markets/{ticker}"

            async with session.get(url, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("market")
                return None
        except Exception as e:
            logger.error(f"Error fetching market {ticker}: {e}")
            return None

    async def get_orderbook(self, ticker: str) -> Optional[Dict]:
        """Fetch orderbook for a market"""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/markets/{ticker}/orderbook"

            async with session.get(url, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception as e:
            logger.error(f"Error fetching orderbook {ticker}: {e}")
            return None

    async def get_events(self, limit: int = 50, status: str = "open") -> List[Dict]:
        """Fetch events (groups of related markets)"""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/events"
            params = {"limit": limit, "status": status}

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("events", [])
                return []
        except Exception as e:
            logger.error(f"Error fetching events: {e}")
            return []

    async def get_event_markets(self, event_ticker: str) -> List[Dict]:
        """Fetch all markets for a specific event (for NegRisk detection)"""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/markets"
            params = {"event_ticker": event_ticker, "limit": 100}

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("markets", [])
                return []
        except Exception as e:
            logger.error(f"Error fetching event markets {event_ticker}: {e}")
            return []

    async def get_trades(self, ticker: str, limit: int = 100) -> List[Dict]:
        """Fetch recent trades for whale tracking"""
        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/markets/{ticker}/trades"
            params = {"limit": limit}

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("trades", [])
                return []
        except Exception as e:
            logger.error(f"Error fetching trades {ticker}: {e}")
            return []

    async def get_orderbooks_batch(self, tickers: List[str], max_concurrent: int = 10) -> Dict[str, Dict]:
        """Fetch multiple orderbooks concurrently for speed"""
        import asyncio

        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_one(ticker: str) -> tuple:
            async with semaphore:
                ob = await self.get_orderbook(ticker)
                return ticker, ob

        results = await asyncio.gather(*[fetch_one(t) for t in tickers], return_exceptions=True)

        orderbooks = {}
        for result in results:
            if isinstance(result, tuple):
                ticker, ob = result
                if ob:
                    orderbooks[ticker] = ob
        return orderbooks

    # =========================================================================
    # ORDER EXECUTION METHODS - Phase 1 HFT Implementation
    # =========================================================================

    async def get_balance(self) -> Dict[str, float]:
        """Get account balance from Kalshi"""
        if not self.token:
            logger.warning("Not authenticated - cannot get balance")
            return {"balance": 0.0, "available": 0.0}

        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/portfolio/balance"

            async with session.get(url, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "balance": float(data.get("balance", 0)) / 100,  # Convert cents to dollars
                        "available": float(data.get("portfolio_value", 0)) / 100
                    }
                else:
                    logger.error(f"Failed to get balance: {resp.status}")
                    return {"balance": 0.0, "available": 0.0}
        except Exception as e:
            logger.error(f"Error getting balance: {e}")
            return {"balance": 0.0, "available": 0.0}

    async def get_positions(self) -> List[Dict]:
        """Get current positions from Kalshi"""
        if not self.token:
            logger.warning("Not authenticated - cannot get positions")
            return []

        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/portfolio/positions"

            async with session.get(url, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    positions = data.get("market_positions", [])
                    # Normalize position data
                    return [
                        {
                            "ticker": p.get("ticker", ""),
                            "position": p.get("position", 0),  # Positive = YES, Negative = NO
                            "market_exposure": float(p.get("market_exposure", 0)) / 100,
                            "realized_pnl": float(p.get("realized_pnl", 0)) / 100,
                            "resting_orders_count": p.get("resting_orders_count", 0)
                        }
                        for p in positions
                    ]
                else:
                    logger.error(f"Failed to get positions: {resp.status}")
                    return []
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []

    async def create_order(
        self,
        ticker: str,
        side: str,           # "yes" or "no"
        action: str,         # "buy" or "sell"
        count: int,          # Number of contracts
        price: int,          # Price in cents (1-99)
        order_type: str = "limit",
        expiration_ts: Optional[int] = None,
        client_order_id: Optional[str] = None
    ) -> Dict:
        """
        Place an order on Kalshi.

        Args:
            ticker: Market ticker (e.g., "KXBTC-24DEC31-B100000")
            side: "yes" or "no"
            action: "buy" or "sell"
            count: Number of contracts to trade
            price: Limit price in cents (1-99)
            order_type: "limit" or "market"
            expiration_ts: Unix timestamp for order expiry (optional)
            client_order_id: Your reference ID (optional)

        Returns:
            Dict with order details including order_id, or error info
        """
        if not self.token:
            return {"error": "Not authenticated", "success": False}

        # Validate inputs
        if side not in ("yes", "no"):
            return {"error": f"Invalid side: {side}", "success": False}
        if action not in ("buy", "sell"):
            return {"error": f"Invalid action: {action}", "success": False}
        if not 1 <= price <= 99:
            return {"error": f"Price must be 1-99 cents, got: {price}", "success": False}
        if count <= 0:
            return {"error": f"Count must be positive, got: {count}", "success": False}

        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/portfolio/orders"

            payload = {
                "ticker": ticker,
                "side": side,
                "action": action,
                "count": count,
                "type": order_type,
            }

            # Add price for limit orders
            if order_type == "limit":
                payload["yes_price" if side == "yes" else "no_price"] = price

            # Optional fields
            if expiration_ts:
                payload["expiration_ts"] = expiration_ts
            if client_order_id:
                payload["client_order_id"] = client_order_id

            async with session.post(url, json=payload, headers=self._get_headers()) as resp:
                data = await resp.json()

                if resp.status in (200, 201):
                    order = data.get("order", {})
                    logger.info(f"Order placed: {order.get('order_id')} - {action} {count} {side} @ {price}¢")
                    return {
                        "success": True,
                        "order_id": order.get("order_id"),
                        "status": order.get("status"),
                        "ticker": ticker,
                        "side": side,
                        "action": action,
                        "count": count,
                        "price": price,
                        "created_time": order.get("created_time"),
                        "raw": order
                    }
                else:
                    error_msg = data.get("error", {}).get("message", str(data))
                    logger.error(f"Order failed: {resp.status} - {error_msg}")
                    return {
                        "success": False,
                        "error": error_msg,
                        "status_code": resp.status
                    }

        except Exception as e:
            logger.error(f"Error creating order: {e}")
            return {"success": False, "error": str(e)}

    async def cancel_order(self, order_id: str) -> Dict:
        """Cancel a pending order"""
        if not self.token:
            return {"error": "Not authenticated", "success": False}

        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/portfolio/orders/{order_id}"

            async with session.delete(url, headers=self._get_headers()) as resp:
                if resp.status in (200, 204):
                    logger.info(f"Order cancelled: {order_id}")
                    return {"success": True, "order_id": order_id}
                else:
                    data = await resp.json()
                    error_msg = data.get("error", {}).get("message", str(data))
                    logger.error(f"Cancel failed: {resp.status} - {error_msg}")
                    return {"success": False, "error": error_msg}

        except Exception as e:
            logger.error(f"Error cancelling order: {e}")
            return {"success": False, "error": str(e)}

    async def get_order(self, order_id: str) -> Optional[Dict]:
        """Get order status and details"""
        if not self.token:
            return None

        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/portfolio/orders/{order_id}"

            async with session.get(url, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    order = data.get("order", {})
                    return {
                        "order_id": order.get("order_id"),
                        "status": order.get("status"),  # "resting", "canceled", "executed"
                        "ticker": order.get("ticker"),
                        "side": order.get("side"),
                        "action": order.get("action"),
                        "original_count": order.get("count"),
                        "remaining_count": order.get("remaining_count"),
                        "filled_count": order.get("count", 0) - order.get("remaining_count", 0),
                        "price": order.get("yes_price") or order.get("no_price"),
                        "created_time": order.get("created_time"),
                        "raw": order
                    }
                return None
        except Exception as e:
            logger.error(f"Error getting order {order_id}: {e}")
            return None

    async def get_fills(self, ticker: str = None, limit: int = 100) -> List[Dict]:
        """Get recent fills (executed trades)"""
        if not self.token:
            return []

        try:
            session = await self._get_session()
            url = f"{self.base_url}/trade-api/v2/portfolio/fills"
            params = {"limit": limit}
            if ticker:
                params["ticker"] = ticker

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    fills = data.get("fills", [])
                    return [
                        {
                            "trade_id": f.get("trade_id"),
                            "order_id": f.get("order_id"),
                            "ticker": f.get("ticker"),
                            "side": f.get("side"),
                            "action": f.get("action"),
                            "count": f.get("count"),
                            "price": f.get("yes_price") or f.get("no_price"),
                            "created_time": f.get("created_time"),
                            "is_taker": f.get("is_taker")
                        }
                        for f in fills
                    ]
                return []
        except Exception as e:
            logger.error(f"Error getting fills: {e}")
            return []

    async def execute_arbitrage_order(
        self,
        ticker: str,
        yes_price: float,
        no_price: float,
        position_size: float = 10.0
    ) -> Dict:
        """
        Execute a single-condition arbitrage: buy both YES and NO when sum < $1.

        This is the core HFT execution for guaranteed profit.

        Args:
            ticker: Market ticker
            yes_price: Current YES ask price (0-1 scale)
            no_price: Current NO ask price (0-1 scale)
            position_size: Dollar amount to trade

        Returns:
            Dict with execution results
        """
        total = yes_price + no_price
        if total >= 1.0:
            return {"success": False, "error": "No arbitrage - prices sum to >= $1"}

        profit_per_contract = 1.0 - total

        # Calculate contract count based on position size
        # Each contract costs (yes_price + no_price) and pays $1
        cost_per_pair = total
        num_contracts = int(position_size / cost_per_pair)

        if num_contracts <= 0:
            return {"success": False, "error": "Position size too small"}

        # Convert prices to cents for Kalshi API
        yes_cents = int(yes_price * 100)
        no_cents = int(no_price * 100)

        results = {
            "ticker": ticker,
            "num_contracts": num_contracts,
            "yes_price": yes_price,
            "no_price": no_price,
            "expected_profit": profit_per_contract * num_contracts,
            "orders": []
        }

        # Place YES order
        yes_order = await self.create_order(
            ticker=ticker,
            side="yes",
            action="buy",
            count=num_contracts,
            price=yes_cents,
            order_type="limit"
        )
        results["orders"].append({"side": "yes", "result": yes_order})

        # Place NO order
        no_order = await self.create_order(
            ticker=ticker,
            side="no",
            action="buy",
            count=num_contracts,
            price=no_cents,
            order_type="limit"
        )
        results["orders"].append({"side": "no", "result": no_order})

        # Determine overall success
        results["success"] = yes_order.get("success") and no_order.get("success")

        if results["success"]:
            results["yes_order_id"] = yes_order.get("order_id")
            results["no_order_id"] = no_order.get("order_id")
            logger.info(f"Arbitrage orders placed: {ticker} - {num_contracts} contracts, expected profit: ${results['expected_profit']:.2f}")
        else:
            # If one side failed, try to cancel the other
            if yes_order.get("success") and not no_order.get("success"):
                await self.cancel_order(yes_order.get("order_id"))
                results["rolled_back"] = "yes_order_cancelled"
            elif no_order.get("success") and not yes_order.get("success"):
                await self.cancel_order(no_order.get("order_id"))
                results["rolled_back"] = "no_order_cancelled"

        return results


# =============================================================================
# COINBASE API CLIENT
# =============================================================================

class CoinbaseClient:
    """Client for Coinbase Advanced Trade API"""

    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key = api_key or config.COINBASE_API_KEY
        self.api_secret = api_secret or config.COINBASE_API_SECRET
        self.base_url = "https://api.coinbase.com"
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def _sign_request(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        """Generate JWT signature for Coinbase API"""
        if not self.api_key or not self.api_secret:
            return {}

        timestamp = str(int(time.time()))
        message = f"{timestamp}{method.upper()}{path}{body}"

        signature = hmac.new(
            self.api_secret.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()

        return {
            "CB-ACCESS-KEY": self.api_key,
            "CB-ACCESS-SIGN": signature,
            "CB-ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json"
        }

    async def get_accounts(self) -> List[Dict]:
        """Fetch wallet accounts/balances"""
        if not self.api_key:
            logger.warning("No Coinbase API key configured")
            return []

        try:
            session = await self._get_session()
            path = "/api/v3/brokerage/accounts"
            headers = self._sign_request("GET", path)

            async with session.get(f"{self.base_url}{path}", headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("accounts", [])
                else:
                    error = await resp.text()
                    logger.error(f"Coinbase accounts error: {resp.status} - {error}")
                    return []
        except Exception as e:
            logger.error(f"Coinbase accounts error: {e}")
            return []

    async def get_balance(self, currency: str = "USD") -> float:
        """Get balance for specific currency"""
        accounts = await self.get_accounts()
        for account in accounts:
            if account.get("currency") == currency:
                return float(account.get("available_balance", {}).get("value", 0))
        return 0.0


# =============================================================================
# POLYMARKET API CLIENT
# =============================================================================

class PolymarketClient:
    """
    Client for Polymarket CLOB (Central Limit Order Book) API
    Polymarket is a decentralized prediction market on Polygon
    Uses the CLOB API for market data and trading
    """

    def __init__(self, api_key: str = "", private_key: str = ""):
        self.api_key = api_key or config.POLYMARKET_API_KEY
        self.private_key = private_key or config.POLYMARKET_PRIVATE_KEY
        self.clob_url = config.POLYMARKET_CLOB_URL.rstrip('/')
        self.gamma_url = config.POLYMARKET_GAMMA_URL.rstrip('/')
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def get_markets(self, limit: int = 100, active: bool = True) -> List[Dict]:
        """
        Fetch markets from Polymarket Gamma API
        Returns list of prediction markets with current prices
        """
        try:
            session = await self._get_session()
            url = f"{self.gamma_url}/markets"
            params = {
                "limit": limit,
                "active": str(active).lower(),
                "closed": "false"
            }

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data if isinstance(data, list) else data.get("markets", [])
                else:
                    logger.error(f"Polymarket markets error: {resp.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching Polymarket markets: {e}")
            return []

    async def get_market(self, condition_id: str) -> Optional[Dict]:
        """Fetch single market details by condition ID"""
        try:
            session = await self._get_session()
            url = f"{self.gamma_url}/markets/{condition_id}"

            async with session.get(url, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception as e:
            logger.error(f"Error fetching Polymarket market {condition_id}: {e}")
            return None

    async def get_orderbook(self, token_id: str) -> Optional[Dict]:
        """
        Fetch orderbook from CLOB for a specific token
        token_id is the outcome token (YES or NO token)
        """
        try:
            session = await self._get_session()
            url = f"{self.clob_url}/book"
            params = {"token_id": token_id}

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception as e:
            logger.error(f"Error fetching Polymarket orderbook {token_id}: {e}")
            return None

    async def get_price(self, token_id: str) -> Optional[Dict]:
        """Get current price for a token"""
        try:
            session = await self._get_session()
            url = f"{self.clob_url}/price"
            params = {"token_id": token_id}

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception as e:
            logger.error(f"Error fetching Polymarket price {token_id}: {e}")
            return None

    async def get_prices_batch(self, token_ids: List[str], max_concurrent: int = 10) -> Dict[str, Dict]:
        """Fetch multiple prices concurrently"""
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_one(token_id: str) -> tuple:
            async with semaphore:
                price = await self.get_price(token_id)
                return token_id, price

        results = await asyncio.gather(*[fetch_one(t) for t in token_ids], return_exceptions=True)

        prices = {}
        for result in results:
            if isinstance(result, tuple):
                token_id, price = result
                if price:
                    prices[token_id] = price
        return prices

    async def get_events(self, limit: int = 50) -> List[Dict]:
        """Fetch events (groups of related markets)"""
        try:
            session = await self._get_session()
            url = f"{self.gamma_url}/events"
            params = {"limit": limit, "active": "true", "closed": "false"}

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data if isinstance(data, list) else data.get("events", [])
                return []
        except Exception as e:
            logger.error(f"Error fetching Polymarket events: {e}")
            return []

    async def get_trades(self, market_id: str, limit: int = 100) -> List[Dict]:
        """Fetch recent trades for a market"""
        try:
            session = await self._get_session()
            url = f"{self.clob_url}/trades"
            params = {"market": market_id, "limit": limit}

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data if isinstance(data, list) else data.get("trades", [])
                return []
        except Exception as e:
            logger.error(f"Error fetching Polymarket trades: {e}")
            return []

    # =========================================================================
    # ORDER EXECUTION - Polymarket CLOB with EIP-712 Signing
    # =========================================================================

    def _get_wallet_address(self) -> Optional[str]:
        """Derive wallet address from private key"""
        if not self.private_key:
            return None
        try:
            # Remove 0x prefix if present
            pk = self.private_key.replace("0x", "")
            # Use simple ECDSA to derive address (requires eth-account or manual impl)
            # For now, we'll require the address to be set in config
            return config.BASE_WALLET_ADDRESS
        except Exception:
            return None

    def _create_order_signature(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        nonce: int,
        expiration: int
    ) -> Optional[str]:
        """
        Create EIP-712 signature for Polymarket order.

        Polymarket uses typed data signing (EIP-712) for order authentication.
        This requires the user's private key to sign orders.
        """
        if not self.private_key:
            logger.error("No private key configured for Polymarket signing")
            return None

        try:
            from eth_account import Account

            # Get wallet address from private key
            pk = self.private_key if self.private_key.startswith("0x") else f"0x{self.private_key}"
            account = Account.from_key(pk)
            wallet_address = account.address

            # Convert price/size to amounts (USDC has 6 decimals)
            maker_amount = int(size * 1_000_000)
            taker_amount = int(size * price * 1_000_000)

            # EIP-712 typed data structure
            full_message = {
                "types": {
                    "EIP712Domain": [
                        {"name": "name", "type": "string"},
                        {"name": "version", "type": "string"},
                        {"name": "chainId", "type": "uint256"},
                    ],
                    "Order": [
                        {"name": "salt", "type": "uint256"},
                        {"name": "maker", "type": "address"},
                        {"name": "signer", "type": "address"},
                        {"name": "taker", "type": "address"},
                        {"name": "tokenId", "type": "uint256"},
                        {"name": "makerAmount", "type": "uint256"},
                        {"name": "takerAmount", "type": "uint256"},
                        {"name": "expiration", "type": "uint256"},
                        {"name": "nonce", "type": "uint256"},
                        {"name": "feeRateBps", "type": "uint256"},
                        {"name": "side", "type": "uint8"},
                        {"name": "signatureType", "type": "uint8"},
                    ]
                },
                "primaryType": "Order",
                "domain": {
                    "name": "Polymarket CTF Exchange",
                    "version": "1",
                    "chainId": 137,  # Polygon mainnet
                },
                "message": {
                    "salt": nonce,
                    "maker": wallet_address,
                    "signer": wallet_address,
                    "taker": "0x0000000000000000000000000000000000000000",
                    "tokenId": int(token_id) if token_id.isdigit() else 0,
                    "makerAmount": maker_amount,
                    "takerAmount": taker_amount,
                    "expiration": expiration,
                    "nonce": nonce,
                    "feeRateBps": 0,
                    "side": 0 if side.upper() == "BUY" else 1,
                    "signatureType": 0,
                }
            }

            # Sign the typed data
            signed = Account.sign_typed_data(pk, full_message=full_message)
            logger.info(f"Order signed by {wallet_address[:10]}...")
            return signed.signature.hex()

        except ImportError:
            logger.error("eth-account not installed. Run: pip install eth-account")
            return None

        except Exception as e:
            logger.error(f"Error creating order signature: {e}")
            return None

    async def create_order(
        self,
        token_id: str,
        side: str,           # "BUY" or "SELL"
        price: float,        # 0.01 to 0.99
        size: float,         # Size in USDC
        order_type: str = "GTC"  # Good Till Cancelled
    ) -> Dict:
        """
        Create and submit an order to Polymarket CLOB.

        Args:
            token_id: The outcome token ID (YES or NO token)
            side: "BUY" or "SELL"
            price: Price per share (0.01 to 0.99)
            size: Order size in USDC

        Returns:
            Dict with order details or error
        """
        if not self.private_key:
            return {"success": False, "error": "No private key configured"}

        try:
            # Generate nonce and expiration
            nonce = int(time.time() * 1000)
            expiration = int(time.time()) + 86400  # 24 hour expiry

            # Create signature
            signature = self._create_order_signature(
                token_id, side, price, size, nonce, expiration
            )

            if not signature:
                return {"success": False, "error": "Failed to sign order - install eth-account: pip install eth-account"}

            session = await self._get_session()
            url = f"{self.clob_url}/order"

            payload = {
                "tokenID": token_id,
                "price": str(price),
                "size": str(size),
                "side": side.upper(),
                "type": order_type,
                "signature": signature,
                "nonce": nonce,
                "expiration": expiration
            }

            headers = self._get_headers()
            if self.api_key:
                headers["POLY_API_KEY"] = self.api_key

            async with session.post(url, json=payload, headers=headers) as resp:
                data = await resp.json()

                if resp.status in (200, 201):
                    logger.info(f"Polymarket order placed: {side} {size} @ {price}")
                    return {
                        "success": True,
                        "order_id": data.get("orderID", data.get("id")),
                        "token_id": token_id,
                        "side": side,
                        "price": price,
                        "size": size,
                        "status": data.get("status", "open"),
                        "raw": data
                    }
                else:
                    error_msg = data.get("error", data.get("message", str(data)))
                    logger.error(f"Polymarket order failed: {resp.status} - {error_msg}")
                    return {"success": False, "error": error_msg}

        except Exception as e:
            logger.error(f"Error creating Polymarket order: {e}")
            return {"success": False, "error": str(e)}

    async def cancel_order(self, order_id: str) -> Dict:
        """Cancel an open order"""
        try:
            session = await self._get_session()
            url = f"{self.clob_url}/order/{order_id}"

            headers = self._get_headers()
            if self.api_key:
                headers["POLY_API_KEY"] = self.api_key

            async with session.delete(url, headers=headers) as resp:
                if resp.status in (200, 204):
                    logger.info(f"Polymarket order cancelled: {order_id}")
                    return {"success": True, "order_id": order_id}
                else:
                    data = await resp.json()
                    return {"success": False, "error": data.get("error", "Cancel failed")}

        except Exception as e:
            logger.error(f"Error cancelling Polymarket order: {e}")
            return {"success": False, "error": str(e)}

    async def get_open_orders(self, market: str = None) -> List[Dict]:
        """Get open orders, optionally filtered by market"""
        try:
            session = await self._get_session()
            url = f"{self.clob_url}/orders"
            params = {}
            if market:
                params["market"] = market

            headers = self._get_headers()
            if self.api_key:
                headers["POLY_API_KEY"] = self.api_key

            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data if isinstance(data, list) else data.get("orders", [])
                return []

        except Exception as e:
            logger.error(f"Error getting Polymarket orders: {e}")
            return []

    async def get_balances(self) -> Dict:
        """Get USDC and token balances on Polymarket"""
        wallet = self._get_wallet_address()
        if not wallet:
            return {"usdc": 0.0, "positions": []}

        try:
            session = await self._get_session()
            url = f"{self.clob_url}/balances"
            params = {"address": wallet}

            async with session.get(url, params=params, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "usdc": float(data.get("usdc", 0)),
                        "positions": data.get("positions", [])
                    }
                return {"usdc": 0.0, "positions": []}

        except Exception as e:
            logger.error(f"Error getting Polymarket balances: {e}")
            return {"usdc": 0.0, "positions": []}


# =============================================================================
# BASE L2 (COINBASE LAYER 2) CLIENT
# =============================================================================

class BaseClient:
    """
    Client for Base L2 (Coinbase's Ethereum Layer 2)
    Used for wallet balance and transaction monitoring
    Base is an Ethereum L2 built on the OP Stack
    """

    def __init__(self, rpc_url: str = "", wallet_address: str = ""):
        self.rpc_url = rpc_url or config.BASE_RPC_URL
        self.wallet_address = wallet_address or config.BASE_WALLET_ADDRESS
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def _rpc_call(self, method: str, params: List = None) -> Optional[Dict]:
        """Make JSON-RPC call to Base node"""
        try:
            session = await self._get_session()
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params or []
            }

            async with session.post(
                self.rpc_url,
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "error" in data:
                        logger.error(f"Base RPC error: {data['error']}")
                        return None
                    return data.get("result")
                return None
        except Exception as e:
            logger.error(f"Base RPC call error: {e}")
            return None

    async def get_eth_balance(self, address: str = None) -> float:
        """Get ETH balance on Base L2"""
        addr = address or self.wallet_address
        if not addr:
            return 0.0

        try:
            result = await self._rpc_call("eth_getBalance", [addr, "latest"])
            if result:
                # Convert from hex wei to ETH
                wei = int(result, 16)
                return wei / 10**18
            return 0.0
        except Exception as e:
            logger.error(f"Error getting Base ETH balance: {e}")
            return 0.0

    async def get_token_balance(self, token_address: str, wallet_address: str = None) -> float:
        """
        Get ERC-20 token balance on Base
        Common tokens on Base:
        - USDC: 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
        - USDbC (bridged USDC): 0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA
        - DAI: 0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb
        """
        addr = wallet_address or self.wallet_address
        if not addr:
            return 0.0

        try:
            # ERC-20 balanceOf(address) function signature
            # Function selector: 0x70a08231
            # Pad address to 32 bytes
            padded_addr = addr.lower().replace("0x", "").zfill(64)
            data = f"0x70a08231{padded_addr}"

            result = await self._rpc_call("eth_call", [
                {"to": token_address, "data": data},
                "latest"
            ])

            if result and result != "0x":
                # Convert from hex to decimal
                balance = int(result, 16)
                # Assuming 6 decimals for USDC, 18 for most others
                # For USDC on Base
                if token_address.lower() in [
                    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC
                    "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca"   # USDbC
                ]:
                    return balance / 10**6
                return balance / 10**18
            return 0.0
        except Exception as e:
            logger.error(f"Error getting Base token balance: {e}")
            return 0.0

    async def get_usdc_balance(self, wallet_address: str = None) -> float:
        """Get USDC balance on Base (native USDC)"""
        usdc_address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        return await self.get_token_balance(usdc_address, wallet_address)

    async def get_block_number(self) -> Optional[int]:
        """Get current block number"""
        try:
            result = await self._rpc_call("eth_blockNumber", [])
            if result:
                return int(result, 16)
            return None
        except Exception as e:
            logger.error(f"Error getting Base block number: {e}")
            return None

    async def get_gas_price(self) -> Optional[float]:
        """Get current gas price in Gwei"""
        try:
            result = await self._rpc_call("eth_gasPrice", [])
            if result:
                wei = int(result, 16)
                return wei / 10**9  # Convert to Gwei
            return None
        except Exception as e:
            logger.error(f"Error getting Base gas price: {e}")
            return None


# =============================================================================
# ARBITRAGE DETECTOR
# =============================================================================

@dataclass
class ArbitrageOpportunity:
    """Represents a detected arbitrage opportunity"""
    market_id: str
    market_title: str
    strategy: str  # "single_condition", "multi_outcome", or "whale_signal"
    yes_price: float
    no_price: float
    total_price: float
    profit_per_share: float
    profit_dollars: float
    roi_percent: float
    risk_score: float
    urgency: str  # "high", "medium", "low"
    timestamp: float
    details: Dict[str, Any]


@dataclass
class WhaleSignal:
    """Represents a whale trading signal"""
    market_id: str
    market_title: str
    direction: str  # "YES" or "NO"
    total_volume: float
    trade_count: int
    avg_price: float
    confidence: float  # 0-1 based on consistency
    timestamp: float


@dataclass
class CrossMarketOpportunity:
    """
    Represents a cross-market arbitrage opportunity
    When the same event has different prices on Kalshi vs Polymarket
    """
    event_title: str
    kalshi_market_id: str
    polymarket_market_id: str
    kalshi_yes_price: float
    kalshi_no_price: float
    polymarket_yes_price: float
    polymarket_no_price: float
    spread: float  # Price difference
    direction: str  # "buy_kalshi_sell_poly" or "buy_poly_sell_kalshi"
    profit_per_share: float
    profit_dollars: float
    roi_percent: float
    risk_score: float
    urgency: str
    timestamp: float
    details: Dict[str, Any]


class ArbitrageDetector:
    """Detects arbitrage opportunities in prediction markets"""

    def __init__(self):
        self.opportunities: List[ArbitrageOpportunity] = []
        self.seen_opportunities: Dict[str, float] = {}  # Deduplication
        self.dedup_window = 300  # 5 minutes

    def detect_single_condition(self, market: Dict, orderbook: Optional[Dict]) -> Optional[ArbitrageOpportunity]:
        """
        Detect single-condition arbitrage: YES + NO ≠ $1.00
        Based on IMDEA research showing $10.58M extracted via this method
        """
        try:
            ticker = market.get("ticker", "")
            title = market.get("title", market.get("question", "Unknown"))

            # Get prices from orderbook or market data
            if orderbook:
                yes_ask = orderbook.get("yes", {}).get("ask", 0)
                no_ask = orderbook.get("no", {}).get("ask", 0)
                yes_bid = orderbook.get("yes", {}).get("bid", 0)
                no_bid = orderbook.get("no", {}).get("bid", 0)

                # Use best available prices
                yes_price = yes_ask if yes_ask > 0 else market.get("yes_price", 0.5)
                no_price = no_ask if no_ask > 0 else market.get("no_price", 0.5)
            else:
                # Fallback to market mid prices
                yes_price = market.get("yes_price", market.get("last_price", 0.5))
                no_price = market.get("no_price", 1 - yes_price)

            # Convert from cents to dollars if needed (Kalshi uses cents)
            if yes_price > 1:
                yes_price = yes_price / 100
            if no_price > 1:
                no_price = no_price / 100

            total_price = yes_price + no_price

            # Filter invalid data
            if yes_price <= 0 or no_price <= 0:
                return None
            if yes_price >= 0.99 and no_price >= 0.99:  # Both sides maxed = stale
                return None

            # Calculate profit (buying both YES and NO should cost < $1 for arb)
            profit_per_share = 1.0 - total_price

            # Must exceed minimum threshold
            if profit_per_share < config.MIN_PROFIT_THRESHOLD:
                return None

            # Filter unrealistic opportunities (likely stale data)
            if profit_per_share > 0.50:  # >50% profit is suspicious
                return None

            profit_dollars = profit_per_share * config.POSITION_SIZE
            roi_percent = (profit_per_share / total_price) * 100 if total_price > 0 else 0

            # Calculate risk score
            risk_score = self._calculate_risk(market)

            # Determine urgency
            if roi_percent >= config.HIGH_URGENCY_ROI * 100:
                urgency = "high"
            elif roi_percent >= config.MEDIUM_URGENCY_ROI * 100:
                urgency = "medium"
            else:
                urgency = "low"

            return ArbitrageOpportunity(
                market_id=ticker,
                market_title=title,
                strategy="single_condition",
                yes_price=yes_price,
                no_price=no_price,
                total_price=total_price,
                profit_per_share=profit_per_share,
                profit_dollars=profit_dollars,
                roi_percent=roi_percent,
                risk_score=risk_score,
                urgency=urgency,
                timestamp=time.time(),
                details={"market": market}
            )

        except Exception as e:
            logger.error(f"Error detecting single condition arb: {e}")
            return None

    def detect_multi_outcome(self, event: Dict, markets: List[Dict]) -> Optional[ArbitrageOpportunity]:
        """
        Detect multi-outcome arbitrage (similar to NegRisk)
        When sum of all outcome probabilities ≠ 100%
        IMDEA research: $28.99M extracted, 29× capital efficiency
        """
        try:
            if len(markets) < 3:  # Need at least 3 outcomes
                return None

            event_title = event.get("title", "Multi-outcome event")

            # Sum all YES prices (probabilities)
            total_probability = 0
            prices = []
            for m in markets:
                yes_price = m.get("yes_price", m.get("last_price", 0))
                if yes_price > 1:
                    yes_price = yes_price / 100
                if yes_price > 0:
                    total_probability += yes_price
                    prices.append({"ticker": m.get("ticker"), "price": yes_price})

            # Arbitrage exists if total probability ≠ 1.0
            deviation = abs(1.0 - total_probability)

            if deviation < config.MIN_PROFIT_THRESHOLD:
                return None

            # Calculate profit potential
            if total_probability < 1.0:
                # Can buy all outcomes for < $1, guaranteed $1 payout
                profit_per_share = 1.0 - total_probability
            else:
                # Can sell all outcomes for > $1 (more complex execution)
                profit_per_share = total_probability - 1.0

            profit_dollars = profit_per_share * config.POSITION_SIZE
            roi_percent = (profit_per_share / total_probability) * 100 if total_probability > 0 else 0

            # Multi-outcome has higher complexity risk
            risk_score = min(0.9, self._calculate_risk(event) + 0.2)

            if roi_percent >= config.HIGH_URGENCY_ROI * 100:
                urgency = "high"
            elif roi_percent >= config.MEDIUM_URGENCY_ROI * 100:
                urgency = "medium"
            else:
                urgency = "low"

            return ArbitrageOpportunity(
                market_id=event.get("event_ticker", "multi"),
                market_title=event_title,
                strategy="multi_outcome",
                yes_price=total_probability,
                no_price=0,
                total_price=total_probability,
                profit_per_share=profit_per_share,
                profit_dollars=profit_dollars,
                roi_percent=roi_percent,
                risk_score=risk_score,
                urgency=urgency,
                timestamp=time.time(),
                details={"event": event, "markets": markets, "prices": prices}
            )

        except Exception as e:
            logger.error(f"Error detecting multi-outcome arb: {e}")
            return None

    def _calculate_risk(self, market: Dict) -> float:
        """Calculate risk score (0-1) based on market properties"""
        risk = 0.3  # Base risk

        # Time to resolution risk
        close_time = market.get("close_time", market.get("expected_expiration_time"))
        if close_time:
            try:
                if isinstance(close_time, str):
                    # Parse ISO format
                    close_dt = datetime.fromisoformat(close_time.replace('Z', '+00:00'))
                    days_to_close = (close_dt - datetime.now(timezone.utc)).days
                else:
                    days_to_close = 30  # Default

                if days_to_close < 2:
                    risk += 0.3  # High risk near resolution
                elif days_to_close < 7:
                    risk += 0.15
            except:
                pass

        # Volume/liquidity risk
        volume = market.get("volume", market.get("volume_24h", 0))
        if volume < 1000:
            risk += 0.2
        elif volume < 10000:
            risk += 0.1

        return min(1.0, risk)

    def is_duplicate(self, opp: ArbitrageOpportunity) -> bool:
        """Check if opportunity was recently seen"""
        key = f"{opp.market_id}_{opp.strategy}"
        now = time.time()

        # Clean old entries
        self.seen_opportunities = {
            k: v for k, v in self.seen_opportunities.items()
            if now - v < self.dedup_window
        }

        if key in self.seen_opportunities:
            return True

        self.seen_opportunities[key] = now
        return False

    def detect_whale_activity(self, market: Dict, trades: List[Dict]) -> Optional[WhaleSignal]:
        """
        Detect whale trading activity - large trades indicating informed traders
        IMDEA research: 61-68% accuracy predicting price movement within T+15 to T+60 min
        """
        if not trades:
            return None

        try:
            ticker = market.get("ticker", "")
            title = market.get("title", market.get("question", "Unknown"))

            # Filter to whale-sized trades
            whale_trades = []
            for t in trades:
                # Calculate trade value (price * count)
                price = t.get("price", t.get("yes_price", 0))
                if price > 1:
                    price = price / 100
                count = t.get("count", t.get("size", 0))
                value = price * count

                if value >= config.WHALE_THRESHOLD:
                    whale_trades.append({
                        "side": t.get("taker_side", t.get("side", "unknown")),
                        "price": price,
                        "count": count,
                        "value": value
                    })

            if len(whale_trades) < 2:
                return None

            # Analyze direction bias
            yes_volume = sum(t["value"] for t in whale_trades if t["side"].lower() in ["yes", "buy"])
            no_volume = sum(t["value"] for t in whale_trades if t["side"].lower() in ["no", "sell"])
            total_volume = yes_volume + no_volume

            if total_volume < config.WHALE_THRESHOLD * 2:
                return None

            # Determine dominant direction
            if yes_volume > no_volume * 1.5:
                direction = "YES"
                confidence = yes_volume / total_volume
            elif no_volume > yes_volume * 1.5:
                direction = "NO"
                confidence = no_volume / total_volume
            else:
                return None  # No clear signal

            avg_price = sum(t["price"] * t["value"] for t in whale_trades) / total_volume

            return WhaleSignal(
                market_id=ticker,
                market_title=title,
                direction=direction,
                total_volume=total_volume,
                trade_count=len(whale_trades),
                avg_price=avg_price,
                confidence=confidence,
                timestamp=time.time()
            )

        except Exception as e:
            logger.error(f"Error detecting whale activity: {e}")
            return None

    def compare_event_outcomes(self, event: Dict, markets: List[Dict]) -> Dict[str, Any]:
        """
        Compare all outcomes in an event for NegRisk opportunities
        Returns analysis of probability distribution
        """
        try:
            prices = []
            for m in markets:
                yes_price = m.get("yes_price", m.get("last_price", 0))
                if yes_price > 1:
                    yes_price = yes_price / 100

                prices.append({
                    "ticker": m.get("ticker"),
                    "title": m.get("title", m.get("subtitle", "")),
                    "yes_price": yes_price,
                    "no_price": 1 - yes_price,
                    "volume": m.get("volume", 0)
                })

            total_probability = sum(p["yes_price"] for p in prices)
            deviation = abs(1.0 - total_probability)

            return {
                "event_ticker": event.get("event_ticker", event.get("ticker", "")),
                "event_title": event.get("title", ""),
                "outcome_count": len(prices),
                "total_probability": total_probability,
                "deviation": deviation,
                "is_arbitrage": deviation >= config.MIN_PROFIT_THRESHOLD,
                "profit_potential": deviation * config.POSITION_SIZE if deviation >= config.MIN_PROFIT_THRESHOLD else 0,
                "outcomes": prices
            }

        except Exception as e:
            logger.error(f"Error comparing outcomes: {e}")
            return {}

    def detect_cross_market(
        self,
        kalshi_market: Dict,
        polymarket_market: Dict,
        kalshi_orderbook: Optional[Dict] = None,
        polymarket_price: Optional[Dict] = None
    ) -> Optional[CrossMarketOpportunity]:
        """
        Detect cross-market arbitrage between Kalshi and Polymarket
        When the same event has different prices on different platforms
        """
        try:
            # Extract Kalshi prices
            if kalshi_orderbook:
                k_yes = kalshi_orderbook.get("yes", {}).get("ask", 0)
                k_no = kalshi_orderbook.get("no", {}).get("ask", 0)
            else:
                k_yes = kalshi_market.get("yes_price", 0.5)
                k_no = kalshi_market.get("no_price", 0.5)

            # Convert from cents if needed
            if k_yes > 1:
                k_yes = k_yes / 100
            if k_no > 1:
                k_no = k_no / 100

            # Extract Polymarket prices
            if polymarket_price:
                p_yes = float(polymarket_price.get("price", 0.5))
                p_no = 1.0 - p_yes
            else:
                p_yes = float(polymarket_market.get("outcomePrices", [0.5, 0.5])[0])
                p_no = float(polymarket_market.get("outcomePrices", [0.5, 0.5])[1])

            # Validate prices
            if k_yes <= 0 or k_no <= 0 or p_yes <= 0 or p_no <= 0:
                return None

            # Calculate spreads
            # Strategy 1: Buy YES on Kalshi, Sell YES on Polymarket
            spread_kalshi_to_poly = p_yes - k_yes

            # Strategy 2: Buy YES on Polymarket, Sell YES on Kalshi
            spread_poly_to_kalshi = k_yes - p_yes

            # Determine best direction
            if spread_kalshi_to_poly > spread_poly_to_kalshi:
                spread = spread_kalshi_to_poly
                direction = "buy_kalshi_sell_poly"
            else:
                spread = spread_poly_to_kalshi
                direction = "buy_poly_sell_kalshi"

            # Check if spread exceeds minimum threshold
            if spread < config.CROSS_MARKET_MIN_SPREAD:
                return None

            profit_per_share = spread
            profit_dollars = profit_per_share * config.POSITION_SIZE
            roi_percent = (spread / min(k_yes, p_yes)) * 100 if min(k_yes, p_yes) > 0 else 0

            # Cross-market has higher risk (execution, timing, fees)
            risk_score = min(0.9, self._calculate_risk(kalshi_market) + 0.25)

            # Determine urgency
            if roi_percent >= config.HIGH_URGENCY_ROI * 100:
                urgency = "high"
            elif roi_percent >= config.MEDIUM_URGENCY_ROI * 100:
                urgency = "medium"
            else:
                urgency = "low"

            event_title = kalshi_market.get("title", kalshi_market.get("question", "Unknown"))

            return CrossMarketOpportunity(
                event_title=event_title,
                kalshi_market_id=kalshi_market.get("ticker", ""),
                polymarket_market_id=polymarket_market.get("condition_id", polymarket_market.get("id", "")),
                kalshi_yes_price=k_yes,
                kalshi_no_price=k_no,
                polymarket_yes_price=p_yes,
                polymarket_no_price=p_no,
                spread=spread,
                direction=direction,
                profit_per_share=profit_per_share,
                profit_dollars=profit_dollars,
                roi_percent=roi_percent,
                risk_score=risk_score,
                urgency=urgency,
                timestamp=time.time(),
                details={
                    "kalshi_market": kalshi_market,
                    "polymarket_market": polymarket_market
                }
            )

        except Exception as e:
            logger.error(f"Error detecting cross-market arb: {e}")
            return None

    def match_markets(
        self,
        kalshi_markets: List[Dict],
        polymarket_markets: List[Dict]
    ) -> List[tuple]:
        """
        Match similar markets between Kalshi and Polymarket
        Uses fuzzy matching on titles/questions
        Returns list of (kalshi_market, polymarket_market) tuples
        """
        matches = []

        # Create normalized title lookup for Polymarket
        poly_lookup = {}
        for pm in polymarket_markets:
            title = pm.get("question", pm.get("title", "")).lower().strip()
            # Normalize common variations
            title = title.replace("will ", "").replace("?", "").strip()
            poly_lookup[title] = pm

        for km in kalshi_markets:
            k_title = km.get("title", km.get("question", "")).lower().strip()
            k_title = k_title.replace("will ", "").replace("?", "").strip()

            # Look for exact match first
            if k_title in poly_lookup:
                matches.append((km, poly_lookup[k_title]))
                continue

            # Look for partial match (substring)
            for p_title, pm in poly_lookup.items():
                # Check if significant overlap exists
                k_words = set(k_title.split())
                p_words = set(p_title.split())

                # Remove common words
                common_words = {"the", "a", "an", "in", "on", "at", "to", "for", "of", "by"}
                k_words = k_words - common_words
                p_words = p_words - common_words

                if len(k_words) > 2 and len(p_words) > 2:
                    overlap = len(k_words & p_words)
                    similarity = overlap / max(len(k_words), len(p_words))

                    if similarity > 0.6:  # 60% word overlap
                        matches.append((km, pm))
                        break

        return matches


# =============================================================================
# MAIN ARBITRAGE BOT
# =============================================================================

class FastArbitrageBot:
    def __init__(self):
        # Config - configurable via API
        self.poll_interval_ms = config.SCAN_INTERVAL_MS
        self.position_size = config.POSITION_SIZE
        self.threshold = 0.99
        self.demo_mode = config.DEMO_MODE

        # API Clients
        self.kalshi_client = KalshiClient()
        self.coinbase_client = CoinbaseClient()
        self.polymarket_client = PolymarketClient()
        self.base_client = BaseClient()
        self.detector = ArbitrageDetector()

        # Risk Management (Phase 1 HFT)
        self.position_manager = PositionManager()
        self.risk_manager = RiskManager(self.position_manager)

        # Determine mode
        if self.demo_mode or not config.KALSHI_API_KEY:
            self.mode = "DEMO"
        else:
            self.mode = "LIVE"

        # State
        self.markets = []
        self.total_pnl = 0.0
        self.trades = []
        self.opportunities = []  # Current arbitrage opportunities
        self.is_running = True
        self.is_paused = False
        self.start_time = time.time()

        # Wallet balance
        self.wallet_balance = 0.0

        # Current prices for dashboard
        self.market_prices = {}

        # Price history for sparklines (last 50 prices per market)
        self.price_history = {}

        # Performance tracking with deque for O(1) operations
        self.checks = 0
        self.total_checks = 0
        self.last_report = time.time()
        self.cycle_times = deque(maxlen=100)
        self.checks_per_sec_history = deque(maxlen=60)  # Last 60 seconds

        # Best trade tracking
        self.best_trade = None
        self.last_arb_time = None

        # Real-time stats
        self.instant_checks_per_sec = 0.0
        self.last_check_time = time.perf_counter_ns()
        self.check_times = deque(maxlen=100)

        # API stats
        self.api_calls = 0
        self.api_errors = 0
        self.last_api_call = None

        # NegRisk / Multi-outcome tracking
        self.events = []  # Events with multiple outcomes
        self.negrisk_opportunities = []  # Detected NegRisk arbs

        # Whale tracking
        self.whale_signals = []  # Recent whale signals
        self.whale_trades_cache = {}  # Cache of recent trades per market

        # Polymarket tracking
        self.polymarket_markets = []  # Markets from Polymarket
        self.polymarket_prices = {}  # Current Polymarket prices

        # Cross-market arbitrage tracking
        self.cross_market_opportunities = []  # Kalshi vs Polymarket opportunities
        self.matched_markets = []  # Paired markets between platforms

        # Base L2 wallet tracking
        self.base_eth_balance = 0.0
        self.base_usdc_balance = 0.0

        self.log(f"Bot initialized - {self.mode} MODE", "⚡")
        self.log(f"Target polling: {self.poll_interval_ms}ms")
        if self.mode == "DEMO":
            self.log("Running in DEMO mode - set KALSHI_API_KEY for live data", "⚠️")

    def log(self, msg, emoji="ℹ️"):
        """Logging with millisecond precision"""
        t = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{t}] {emoji} {msg}")

    async def fetch_real_markets(self):
        """Fetch real markets from Kalshi API"""
        try:
            self.api_calls += 1
            self.last_api_call = time.time()

            markets = await self.kalshi_client.get_markets(
                limit=config.TOP_MARKETS,
                status="open"
            )

            if markets:
                self.markets = []
                for m in markets:
                    market = {
                        'id': m.get('ticker', ''),
                        'question': m.get('title', m.get('subtitle', 'Unknown')),
                        'type': self._categorize_market(m),
                        'yes_price': m.get('yes_price', 50) / 100,  # Convert cents to dollars
                        'no_price': m.get('no_price', 50) / 100,
                        'volume': m.get('volume', 0),
                        'close_time': m.get('close_time'),
                        'raw': m  # Keep raw data for detailed analysis
                    }
                    self.markets.append(market)
                    if market['id'] not in self.price_history:
                        self.price_history[market['id']] = deque(maxlen=50)

                self.log(f"Fetched {len(self.markets)} markets from Kalshi", "✅")
                return True
            else:
                self.log("No markets returned from Kalshi API", "⚠️")
                return False

        except Exception as e:
            self.api_errors += 1
            self.log(f"Error fetching Kalshi markets: {e}", "❌")
            return False

    def _categorize_market(self, market: Dict) -> str:
        """Categorize market by type for display"""
        title = market.get('title', '').lower()
        category = market.get('category', '').upper()

        if category:
            return category[:6]  # Truncate long categories

        # Keyword-based categorization
        if any(k in title for k in ['bitcoin', 'btc', 'crypto']):
            return 'CRYPTO'
        elif any(k in title for k in ['election', 'president', 'senate', 'congress']):
            return 'POLITICS'
        elif any(k in title for k in ['fed', 'rate', 'inflation', 'gdp']):
            return 'ECON'
        elif any(k in title for k in ['weather', 'temperature', 'hurricane']):
            return 'WEATHER'
        elif any(k in title for k in ['sport', 'nba', 'nfl', 'mlb']):
            return 'SPORTS'
        else:
            return 'OTHER'

    async def fetch_wallet_balance(self):
        """Fetch wallet balance from Coinbase"""
        if not config.COINBASE_API_KEY:
            return

        try:
            self.wallet_balance = await self.coinbase_client.get_balance("USD")
            self.log(f"Coinbase balance: ${self.wallet_balance:.2f}", "💰")
        except Exception as e:
            self.log(f"Error fetching Coinbase balance: {e}", "⚠️")

    async def fetch_base_balance(self):
        """Fetch wallet balances from Base L2"""
        if not config.BASE_WALLET_ADDRESS:
            return

        try:
            self.base_eth_balance = await self.base_client.get_eth_balance()
            self.base_usdc_balance = await self.base_client.get_usdc_balance()
            self.log(f"Base L2: {self.base_eth_balance:.4f} ETH | ${self.base_usdc_balance:.2f} USDC", "🔵")
        except Exception as e:
            self.log(f"Error fetching Base balance: {e}", "⚠️")

    async def fetch_polymarket_markets(self):
        """Fetch markets from Polymarket"""
        if not config.ENABLE_POLYMARKET:
            return

        try:
            self.api_calls += 1
            self.last_api_call = time.time()

            markets = await self.polymarket_client.get_markets(
                limit=config.TOP_MARKETS,
                active=True
            )

            if markets:
                self.polymarket_markets = []
                for m in markets:
                    # Extract prices from outcome prices if available
                    outcome_prices = m.get("outcomePrices", [])
                    if outcome_prices and len(outcome_prices) >= 2:
                        yes_price = float(outcome_prices[0])
                        no_price = float(outcome_prices[1])
                    else:
                        yes_price = 0.5
                        no_price = 0.5

                    market = {
                        'id': m.get('condition_id', m.get('id', '')),
                        'question': m.get('question', m.get('title', 'Unknown')),
                        'type': 'POLY',
                        'yes_price': yes_price,
                        'no_price': no_price,
                        'volume': float(m.get('volume', m.get('volumeNum', 0)) or 0),
                        'liquidity': float(m.get('liquidity', 0) or 0),
                        'end_date': m.get('endDate', m.get('end_date_iso')),
                        'raw': m,
                        'tokens': m.get('tokens', [])  # Token IDs for orderbook
                    }
                    self.polymarket_markets.append(market)

                self.log(f"Fetched {len(self.polymarket_markets)} markets from Polymarket", "🟣")

                # Match markets with Kalshi for cross-market opportunities
                if self.markets and config.ENABLE_CROSS_MARKET:
                    kalshi_raw = [m.get('raw', m) for m in self.markets]
                    poly_raw = [m.get('raw', m) for m in self.polymarket_markets]
                    self.matched_markets = self.detector.match_markets(kalshi_raw, poly_raw)
                    if self.matched_markets:
                        self.log(f"Found {len(self.matched_markets)} matched markets for cross-market arb", "🔗")

                return True
            else:
                self.log("No markets returned from Polymarket API", "⚠️")
                return False

        except Exception as e:
            self.api_errors += 1
            self.log(f"Error fetching Polymarket markets: {e}", "❌")
            return False

    async def scan_cross_market_opportunities(self):
        """
        Scan for cross-market arbitrage between Kalshi and Polymarket
        Compares prices on matched markets between platforms
        """
        if not config.ENABLE_CROSS_MARKET or not self.matched_markets:
            return

        try:
            for kalshi_market, poly_market in self.matched_markets:
                opp = self.detector.detect_cross_market(
                    kalshi_market,
                    poly_market
                )

                if opp:
                    # Check for duplicates using a combined key
                    dup_key = f"{opp.kalshi_market_id}_{opp.polymarket_market_id}"
                    now = time.time()

                    # Simple deduplication
                    is_dup = False
                    for existing in self.cross_market_opportunities:
                        existing_key = f"{existing.kalshi_market_id}_{existing.polymarket_market_id}"
                        if existing_key == dup_key and now - existing.timestamp < 300:
                            is_dup = True
                            break

                    if not is_dup:
                        self.cross_market_opportunities.append(opp)
                        self._log_cross_market_opportunity(opp)

            # Keep last 50 opportunities
            self.cross_market_opportunities = self.cross_market_opportunities[-50:]

        except Exception as e:
            self.api_errors += 1
            self.log(f"Error scanning cross-market: {e}", "❌")

    def _log_cross_market_opportunity(self, opp: CrossMarketOpportunity):
        """Log a cross-market arbitrage opportunity"""
        direction_text = "Kalshi→Poly" if opp.direction == "buy_kalshi_sell_poly" else "Poly→Kalshi"
        urgency_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(opp.urgency, "🟢")

        self.log("=" * 60, "🔀")
        self.log(f"CROSS-MARKET ARBITRAGE {urgency_emoji}", "💎")
        self.log(f"Event: {opp.event_title[:50]}")
        self.log(f"Direction: {direction_text}")
        self.log(f"Kalshi:     YES ${opp.kalshi_yes_price:.4f} | NO ${opp.kalshi_no_price:.4f}")
        self.log(f"Polymarket: YES ${opp.polymarket_yes_price:.4f} | NO ${opp.polymarket_no_price:.4f}")
        self.log(f"Spread: {opp.spread:.2%} | Profit: ${opp.profit_dollars:.2f} | ROI: {opp.roi_percent:.2f}%")
        self.log("=" * 60, "🔀")

    async def scan_negrisk_opportunities(self):
        """
        Scan for NegRisk/multi-outcome arbitrage opportunities
        Fetches events and their markets, checks if probabilities sum to != 100%
        IMDEA research: $28.99M extracted with 29× capital efficiency
        """
        if not config.ENABLE_MULTI_OUTCOME:
            return

        try:
            self.api_calls += 1
            events = await self.kalshi_client.get_events(limit=20, status="open")

            if not events:
                return

            # Fetch markets for each event concurrently
            async def fetch_event_markets(event):
                event_ticker = event.get("event_ticker", event.get("ticker", ""))
                if not event_ticker:
                    return None
                markets = await self.kalshi_client.get_event_markets(event_ticker)
                self.api_calls += 1
                return (event, markets)

            results = await asyncio.gather(
                *[fetch_event_markets(e) for e in events[:10]],  # Limit to 10 events
                return_exceptions=True
            )

            for result in results:
                if isinstance(result, tuple) and result[1]:
                    event, markets = result
                    if len(markets) >= 3:  # Need at least 3 outcomes for NegRisk
                        opp = self.detector.detect_multi_outcome(event, markets)
                        if opp and not self.detector.is_duplicate(opp):
                            self.negrisk_opportunities.append(opp)
                            self._log_negrisk_opportunity(event, markets, opp)

            # Keep last 50 opportunities
            self.negrisk_opportunities = self.negrisk_opportunities[-50:]

        except Exception as e:
            self.api_errors += 1
            self.log(f"Error scanning NegRisk: {e}", "❌")

    def _log_negrisk_opportunity(self, event: Dict, markets: List[Dict], opp: ArbitrageOpportunity):
        """Log a NegRisk arbitrage opportunity"""
        self.log("=" * 60, "🎯")
        self.log(f"NEGRISK ARBITRAGE - {len(markets)} OUTCOMES", "💎")
        self.log(f"Event: {event.get('title', 'Unknown')}")
        self.log(f"Total Probability: {opp.total_price:.2%} (should be 100%)")
        self.log(f"Deviation: {opp.profit_per_share:.2%}")
        self.log(f"Profit: ${opp.profit_dollars:.2f} | ROI: {opp.roi_percent:.2f}%")
        self.log("Outcomes:")
        for m in markets[:5]:  # Show first 5
            price = m.get("yes_price", 0)
            if price > 1:
                price = price / 100
            self.log(f"  • {m.get('title', '')[:40]}: {price:.2%}")
        self.log("=" * 60, "🎯")

    async def scan_whale_activity(self):
        """
        Scan for whale trading activity across markets
        IMDEA research: 61-68% accuracy predicting price movement
        """
        if not config.ENABLE_WHALE_TRACKING or not self.markets:
            return

        try:
            # Get trades for top markets by volume
            top_markets = sorted(
                self.markets,
                key=lambda m: m.get('volume', 0),
                reverse=True
            )[:10]

            async def fetch_trades(market):
                trades = await self.kalshi_client.get_trades(market['id'], limit=config.WHALE_LOOKBACK_TRADES)
                self.api_calls += 1
                return (market, trades)

            results = await asyncio.gather(
                *[fetch_trades(m) for m in top_markets],
                return_exceptions=True
            )

            for result in results:
                if isinstance(result, tuple) and result[1]:
                    market, trades = result
                    signal = self.detector.detect_whale_activity(market.get('raw', market), trades)
                    if signal:
                        self.whale_signals.append(signal)
                        self._log_whale_signal(signal)

            # Keep last 50 signals
            self.whale_signals = self.whale_signals[-50:]

        except Exception as e:
            self.api_errors += 1
            self.log(f"Error scanning whales: {e}", "❌")

    def _log_whale_signal(self, signal: WhaleSignal):
        """Log a whale trading signal"""
        self.log("=" * 60, "🐋")
        self.log(f"WHALE DETECTED - {signal.direction}", "🐋")
        self.log(f"Market: {signal.market_title[:50]}")
        self.log(f"Volume: ${signal.total_volume:,.0f} ({signal.trade_count} trades)")
        self.log(f"Avg Price: ${signal.avg_price:.4f} | Confidence: {signal.confidence:.0%}")
        self.log("=" * 60, "🐋")

    async def parallel_market_scan(self):
        """
        Scan all markets in parallel for maximum speed
        Uses batch orderbook fetching instead of sequential
        """
        if not self.markets:
            return

        tickers = [m['id'] for m in self.markets]

        # Fetch all orderbooks in parallel
        orderbooks = await self.kalshi_client.get_orderbooks_batch(
            tickers,
            max_concurrent=config.MAX_CONCURRENT_REQUESTS
        )
        self.api_calls += len(orderbooks)

        # Process each market with its orderbook
        for market in self.markets:
            ticker = market['id']
            orderbook = orderbooks.get(ticker)

            if orderbook:
                # Extract prices from orderbook
                ob_data = orderbook.get('orderbook', {})
                yes_orders = ob_data.get('yes', [])
                no_orders = ob_data.get('no', [])

                yes_price = yes_orders[0][0] / 100 if yes_orders else market.get('yes_price', 0.5)
                no_price = no_orders[0][0] / 100 if no_orders else market.get('no_price', 0.5)
            else:
                yes_price, no_price = self.get_prices(market)

            self._process_market_check(market, yes_price, no_price)

    def create_demo_markets(self):
        """Create local demo markets (fallback when no API)"""
        self.markets = [
            {
                'id': 'demo_btc_100k',
                'question': 'Will BTC reach $100K by end of month?',
                'type': 'CRYPTO',
                'base_yes': 0.52,
                'volatility': 0.03
            },
            {
                'id': 'demo_btc_90k',
                'question': 'Will BTC drop below $90K this week?',
                'type': 'CRYPTO',
                'base_yes': 0.35,
                'volatility': 0.04
            },
            {
                'id': 'demo_fed_rate',
                'question': 'Will Fed cut rates at next meeting?',
                'type': 'ECON',
                'base_yes': 0.45,
                'volatility': 0.02
            },
            {
                'id': 'demo_eth_5k',
                'question': 'Will ETH reach $5K this quarter?',
                'type': 'CRYPTO',
                'base_yes': 0.40,
                'volatility': 0.04
            }
        ]

        # Initialize price history for each market
        for m in self.markets:
            self.price_history[m['id']] = deque(maxlen=50)

        self.log(f"Created {len(self.markets)} demo markets (no API key)", "⚠️")

    def get_prices(self, market):
        """Get prices - real from API or simulated for demo"""
        if self.mode == "LIVE" and 'yes_price' in market:
            # Real prices from API
            yes = market.get('yes_price', 0.50)
            no = market.get('no_price', 0.50)
            return yes, no

        # Demo mode - simulate prices with occasional arbitrage
        base = market.get('base_yes', 0.50)
        vol = market.get('volatility', 0.03)

        # Simulate price movement
        yes = base + random.gauss(0, vol)
        yes = max(0.01, min(0.99, yes))
        yes = round(yes, 4)

        # 6% chance of arbitrage opportunity in demo mode
        if random.random() < 0.06:
            no = round(random.uniform(0.94, 0.975) - yes, 4)
            no = max(0.01, no)
        else:
            no = round(1.0 - yes + random.gauss(0, 0.01), 4)
            no = max(0.01, min(0.99, no))

        return yes, no

    async def get_prices_async(self, market) -> tuple:
        """Get prices with real orderbook data when available"""
        if self.mode == "LIVE":
            try:
                orderbook = await self.kalshi_client.get_orderbook(market['id'])
                if orderbook:
                    self.api_calls += 1
                    # Extract best bid/ask
                    yes_bids = orderbook.get('orderbook', {}).get('yes', [])
                    no_bids = orderbook.get('orderbook', {}).get('no', [])

                    yes_price = yes_bids[0][0] / 100 if yes_bids else market.get('yes_price', 0.5)
                    no_price = no_bids[0][0] / 100 if no_bids else market.get('no_price', 0.5)

                    return yes_price, no_price
            except Exception as e:
                self.api_errors += 1
                logger.debug(f"Orderbook fetch failed for {market['id']}: {e}")

        # Fallback to market prices or simulation
        return self.get_prices(market)

    def execute_trade(self, market, yes, no, profit, opportunity: Optional[ArbitrageOpportunity] = None):
        """
        Execute arbitrage trade.

        Modes:
        - DEMO: Simulate trade, add to P&L
        - LIVE + LIVE_EXECUTION=false: Detect and log only
        - LIVE + LIVE_EXECUTION=true: Actually place orders on Kalshi
        """
        total_profit = profit * self.position_size
        now = datetime.now()

        urgency_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(
            opportunity.urgency if opportunity else "low", "🟢"
        )
        strategy = opportunity.strategy if opportunity else "single_condition"

        self.log("=" * 60, "⚡")
        self.log(f"ARBITRAGE DETECTED - {market['type']} {urgency_emoji}", "💰")
        self.log(f"Strategy: {strategy.upper()}")
        self.log(f"Market: {market['question']}")
        self.log(f"YES: ${yes:.4f} | NO: ${no:.4f} | Sum: ${yes+no:.4f}")
        self.log(f"Profit: ${profit:.4f}/share × {self.position_size} = ${total_profit:.2f}")
        if opportunity:
            self.log(f"ROI: {opportunity.roi_percent:.2f}% | Risk: {opportunity.risk_score:.2f}")

        # Determine execution status
        execution_status = 'simulated'
        execution_result = None

        # === LIVE EXECUTION PATH ===
        if self.mode == "LIVE" and config.LIVE_EXECUTION:
            # Pre-trade risk checks
            num_contracts = int(self.position_size / (yes + no))
            passed, reason = self.risk_manager.check_all(
                platform="kalshi",
                ticker=market['id'],
                side="yes",
                action="buy",
                count=num_contracts,
                price=yes,
                current_balance=self.wallet_balance
            )

            if not passed:
                self.log(f"⛔ BLOCKED: {reason}", "🚫")
                execution_status = 'blocked'
            else:
                self.log(f"✓ Risk checks passed - EXECUTING {num_contracts} contracts", "🚀")
                # Execute in background to not block
                asyncio.create_task(self._execute_live_arbitrage(market, yes, no, num_contracts))
                execution_status = 'submitted'
        elif self.mode == "LIVE":
            execution_status = 'detected'
            self.log("📋 Detection only (set LIVE_EXECUTION=true to trade)", "ℹ️")

        self.log("=" * 60, "⚡")

        trade = {
            'id': len(self.trades) + 1,
            'time': now.strftime("%H:%M:%S.%f")[:-3],
            'timestamp': time.time(),
            'market': market['type'],
            'market_id': market['id'],
            'question': market['question'],
            'yes': yes,
            'no': no,
            'profit': total_profit,
            'strategy': strategy,
            'urgency': opportunity.urgency if opportunity else "low",
            'risk_score': opportunity.risk_score if opportunity else 0.5,
            'status': execution_status
        }

        self.trades.append(trade)

        # Only add to PnL in demo mode (simulated execution)
        if self.mode == "DEMO":
            self.total_pnl += total_profit

        self.last_arb_time = time.time()

        # Track opportunities
        if opportunity:
            self.opportunities.append(opportunity)
            # Keep last 100 opportunities
            if len(self.opportunities) > 100:
                self.opportunities = self.opportunities[-100:]

        # Track best trade
        if self.best_trade is None or total_profit > self.best_trade['profit']:
            self.best_trade = trade.copy()

        self.log(f"Total P/L: ${self.total_pnl:.2f} ({len(self.trades)} trades)", "✅")

        # SECONDARY: Enrich with whale data in background (non-blocking, informational only)
        # This does NOT affect the arbitrage decision - pure math already made that call
        if config.ENABLE_WHALE_TRACKING and self.mode == "LIVE":
            asyncio.create_task(self._enrich_with_whale_data(market, trade))

    async def _execute_live_arbitrage(self, market, yes_price, no_price, num_contracts):
        """
        Execute live arbitrage orders on Kalshi.
        Runs in background to not block detection loop.
        """
        ticker = market['id']

        try:
            # Execute the arbitrage (buy both YES and NO)
            result = await self.kalshi_client.execute_arbitrage_order(
                ticker=ticker,
                yes_price=yes_price,
                no_price=no_price,
                position_size=self.position_size
            )

            if result.get("success"):
                self.log(f"✅ ORDERS PLACED: {ticker}", "💵")
                self.log(f"   YES Order: {result.get('yes_order_id')}")
                self.log(f"   NO Order: {result.get('no_order_id')}")
                self.log(f"   Expected Profit: ${result.get('expected_profit', 0):.2f}")

                # Track pending orders
                if result.get('yes_order_id'):
                    self.position_manager.add_pending_order(
                        result['yes_order_id'],
                        {"ticker": ticker, "side": "yes", "count": num_contracts}
                    )
                if result.get('no_order_id'):
                    self.position_manager.add_pending_order(
                        result['no_order_id'],
                        {"ticker": ticker, "side": "no", "count": num_contracts}
                    )

                # Start monitoring for fills
                asyncio.create_task(self._monitor_order_fills(
                    result.get('yes_order_id'),
                    result.get('no_order_id'),
                    ticker,
                    yes_price,
                    no_price,
                    num_contracts
                ))
            else:
                self.log(f"❌ ORDER FAILED: {result.get('error')}", "🚫")
                if result.get('rolled_back'):
                    self.log(f"   Rolled back: {result['rolled_back']}")

        except Exception as e:
            self.log(f"❌ Execution error: {e}", "🚫")
            logger.exception("Live execution failed")

    async def _monitor_order_fills(self, yes_order_id, no_order_id, ticker, yes_price, no_price, count):
        """Monitor orders until filled or timeout"""
        timeout_sec = config.ORDER_TIMEOUT_MS / 1000
        start_time = time.time()
        yes_filled = False
        no_filled = False

        while time.time() - start_time < timeout_sec:
            try:
                if yes_order_id and not yes_filled:
                    yes_status = await self.kalshi_client.get_order(yes_order_id)
                    if yes_status and yes_status.get('status') == 'executed':
                        yes_filled = True
                        self.position_manager.update_position(
                            "kalshi", ticker, "yes", "buy", count, yes_price
                        )
                        self.position_manager.remove_pending_order(yes_order_id)
                        self.log(f"   ✓ YES filled @ ${yes_price:.4f}", "💚")

                if no_order_id and not no_filled:
                    no_status = await self.kalshi_client.get_order(no_order_id)
                    if no_status and no_status.get('status') == 'executed':
                        no_filled = True
                        self.position_manager.update_position(
                            "kalshi", ticker, "no", "buy", count, no_price
                        )
                        self.position_manager.remove_pending_order(no_order_id)
                        self.log(f"   ✓ NO filled @ ${no_price:.4f}", "💚")

                if yes_filled and no_filled:
                    profit = (1.0 - yes_price - no_price) * count
                    self.total_pnl += profit
                    self.log(f"   💰 ARBITRAGE COMPLETE: +${profit:.2f}", "🎉")
                    return

                await asyncio.sleep(0.5)  # Check every 500ms

            except Exception as e:
                logger.debug(f"Order monitor error: {e}")
                await asyncio.sleep(1)

        # Timeout - cancel unfilled orders
        self.log(f"⏰ Order timeout after {timeout_sec}s", "⚠️")
        if yes_order_id and not yes_filled:
            await self.kalshi_client.cancel_order(yes_order_id)
            self.position_manager.remove_pending_order(yes_order_id)
        if no_order_id and not no_filled:
            await self.kalshi_client.cancel_order(no_order_id)
            self.position_manager.remove_pending_order(no_order_id)

    async def _enrich_with_whale_data(self, market, trade):
        """
        SECONDARY function: Enrich an already-detected opportunity with whale data.
        This is informational only - the arbitrage decision was already made by pure math.
        Runs in background, does not block primary detection.
        """
        try:
            trades_data = await self.kalshi_client.get_trades(market['id'], limit=config.WHALE_LOOKBACK_TRADES)
            if trades_data:
                signal = self.detector.detect_whale_activity(market.get('raw', market), trades_data)
                if signal:
                    self.whale_signals.append(signal)
                    # Just log it - this is confirmation, not decision
                    self.log(f"  └─ Whale confirmation: {signal.direction} bias ({signal.confidence:.0%} confidence)", "🐋")
                    # Keep last 50 signals
                    self.whale_signals = self.whale_signals[-50:]
        except Exception as e:
            # Silently fail - whale data is supplementary
            logger.debug(f"Whale enrichment failed: {e}")

    def check_market(self, market):
        """Check single market for arbitrage (sync version for demo)"""
        yes, no = self.get_prices(market)
        self._process_market_check(market, yes, no)

    async def check_market_async(self, market):
        """Check single market for arbitrage (async version for live API)"""
        yes, no = await self.get_prices_async(market)
        self._process_market_check(market, yes, no)

    def _process_market_check(self, market, yes, no):
        """Process market check and detect arbitrage"""
        # Track timing for checks/sec calculation
        now_ns = time.perf_counter_ns()
        self.check_times.append(now_ns)
        self.last_check_time = now_ns

        self.checks += 1
        self.total_checks += 1

        total = yes + no
        total_with_fees = total * 1.01  # 1% fee assumption
        profit = 1.0 - total_with_fees

        # Store price history for sparkline
        if market['id'] in self.price_history:
            self.price_history[market['id']].append({
                'yes': yes,
                'no': no,
                'total': total,
                'time': time.time()
            })

        # Use ArbitrageDetector for real analysis in LIVE mode
        opportunity = None
        if self.mode == "LIVE" and config.ENABLE_SINGLE_CONDITION:
            opportunity = self.detector.detect_single_condition(
                market.get('raw', market),
                None  # Could pass orderbook here
            )

        # Determine if this is an arbitrage opportunity
        is_arb = total_with_fees < 1.0
        if opportunity:
            is_arb = True
            profit = opportunity.profit_per_share

        # Update dashboard data
        self.market_prices[market['id']] = {
            'id': market['id'],
            'type': market['type'],
            'question': market['question'],
            'yes': yes,
            'no': no,
            'total': total,
            'total_with_fees': total_with_fees,
            'is_arb': is_arb,
            'profit': max(0, profit),
            'roi': opportunity.roi_percent if opportunity else (profit / total * 100 if total > 0 else 0),
            'urgency': opportunity.urgency if opportunity else 'low',
            'history': list(self.price_history.get(market['id'], []))
        }

        # Execute if arbitrage opportunity (don't duplicate if detector found it)
        if is_arb:
            if opportunity and not self.detector.is_duplicate(opportunity):
                self.execute_trade(market, yes, no, profit, opportunity)
            elif not opportunity and total_with_fees < 1.0:
                self.execute_trade(market, yes, no, profit)

    def calculate_checks_per_sec(self):
        """Calculate real-time checks per second"""
        if len(self.check_times) < 2:
            return 0.0

        # Get time span of last N checks
        times = list(self.check_times)
        time_span_ns = times[-1] - times[0]
        if time_span_ns <= 0:
            return 0.0

        time_span_sec = time_span_ns / 1_000_000_000
        return (len(times) - 1) / time_span_sec

    async def monitor_loop(self):
        """Main monitoring loop - targets configurable ms"""
        self.log("Starting monitor loop...", "🔄")

        # For LIVE mode, periodically refresh market list
        last_market_refresh = 0
        last_negrisk_scan = 0
        last_polymarket_refresh = 0
        last_cross_market_scan = 0
        market_refresh_interval = 60  # Refresh market list every 60 seconds
        negrisk_scan_interval = 30  # Scan NegRisk every 30 seconds
        # NOTE: Whale tracking is now SECONDARY - runs only after arb detected, not on interval
        polymarket_refresh_interval = 60  # Refresh Polymarket every 60 seconds
        cross_market_scan_interval = 20  # Scan cross-market every 20 seconds

        while self.is_running:
            if self.is_paused:
                await asyncio.sleep(0.1)
                continue

            target_interval = self.poll_interval_ms / 1000.0
            cycle_start = time.perf_counter_ns()
            now = time.time()

            try:
                if self.mode == "LIVE":
                    # Refresh markets periodically
                    if now - last_market_refresh > market_refresh_interval:
                        await self.fetch_real_markets()
                        last_market_refresh = now

                    # PARALLEL market scan - no rate limiting, max speed
                    await self.parallel_market_scan()

                    # NegRisk scan (less frequent, more API calls)
                    if config.ENABLE_MULTI_OUTCOME and now - last_negrisk_scan > negrisk_scan_interval:
                        await self.scan_negrisk_opportunities()
                        last_negrisk_scan = now

                    # NOTE: Whale tracking removed from main loop - it's now SECONDARY
                    # Only enriches opportunities AFTER they're detected by pure math
                    # See enrich_opportunity_with_whale_data() called in execute_trade()

                    # Polymarket market refresh
                    if config.ENABLE_POLYMARKET and now - last_polymarket_refresh > polymarket_refresh_interval:
                        await self.fetch_polymarket_markets()
                        last_polymarket_refresh = now

                    # Cross-market arbitrage scan (Kalshi vs Polymarket)
                    if config.ENABLE_CROSS_MARKET and now - last_cross_market_scan > cross_market_scan_interval:
                        await self.scan_cross_market_opportunities()
                        last_cross_market_scan = now
                else:
                    # Demo mode - sync checks
                    for market in self.markets:
                        self.check_market(market)

            except Exception as e:
                self.log(f"Error in cycle: {e}", "❌")
                self.api_errors += 1

            # Calculate cycle time in ms using nanoseconds
            cycle_time_ns = time.perf_counter_ns() - cycle_start
            cycle_time_ms = cycle_time_ns / 1_000_000
            self.cycle_times.append(cycle_time_ms)

            # Update instant checks/sec
            self.instant_checks_per_sec = self.calculate_checks_per_sec()

            # Sleep to maintain target interval
            sleep_time = max(0, target_interval - (cycle_time_ms / 1000.0))
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    async def stats_updater(self):
        """Update rolling stats every second"""
        while self.is_running:
            await asyncio.sleep(1)
            self.checks_per_sec_history.append(self.checks)
            self.checks = 0

    async def performance_reporter(self):
        """Report performance stats every 10 seconds"""
        while self.is_running:
            await asyncio.sleep(10)

            if self.is_paused:
                continue

            elapsed = time.time() - self.last_report
            checks_per_sec = sum(self.checks_per_sec_history) / max(1, len(self.checks_per_sec_history))

            cycle_times_list = list(self.cycle_times)
            avg_cycle = sum(cycle_times_list) / len(cycle_times_list) if cycle_times_list else 0
            min_cycle = min(cycle_times_list) if cycle_times_list else 0
            max_cycle = max(cycle_times_list) if cycle_times_list else 0

            self.log("=" * 60, "📊")
            self.log(f"PERFORMANCE REPORT [{self.mode}]", "📊")
            self.log(f"Checks/sec: {checks_per_sec:.1f} (instant: {self.instant_checks_per_sec:.1f})")
            self.log(f"Cycle time: avg={avg_cycle:.2f}ms min={min_cycle:.2f}ms max={max_cycle:.2f}ms")
            self.log(f"Total checks: {self.total_checks} | Trades: {len(self.trades)} | P/L: ${self.total_pnl:.2f}")
            if self.mode == "LIVE":
                self.log(f"API calls: {self.api_calls} | Errors: {self.api_errors} | Markets: {len(self.markets)}")
                if self.wallet_balance > 0:
                    self.log(f"Coinbase Balance: ${self.wallet_balance:.2f}")
            self.log("=" * 60, "📊")

            self.last_report = time.time()

    def get_state(self):
        """Get current state for dashboard"""
        cycle_times_list = list(self.cycle_times)
        avg_cycle = sum(cycle_times_list) / len(cycle_times_list) if cycle_times_list else 0
        min_cycle = min(cycle_times_list) if cycle_times_list else 0
        max_cycle = max(cycle_times_list) if cycle_times_list else 0

        # Calculate profit rates
        elapsed_sec = time.time() - self.start_time
        elapsed_min = elapsed_sec / 60
        profit_per_min = self.total_pnl / elapsed_min if elapsed_min > 0 else 0
        profit_per_hour = profit_per_min * 60

        # Time since last arb
        time_since_arb = None
        if self.last_arb_time:
            time_since_arb = time.time() - self.last_arb_time

        # Format whale signals for dashboard
        whale_signals_data = [
            {
                'market_id': s.market_id,
                'market_title': s.market_title[:50],
                'direction': s.direction,
                'volume': s.total_volume,
                'confidence': s.confidence,
                'time': datetime.fromtimestamp(s.timestamp).strftime("%H:%M:%S")
            }
            for s in self.whale_signals[-10:]
        ]

        # Format NegRisk opportunities for dashboard
        negrisk_data = [
            {
                'event': o.market_title[:50],
                'deviation': o.profit_per_share,
                'profit': o.profit_dollars,
                'roi': o.roi_percent,
                'urgency': o.urgency
            }
            for o in self.negrisk_opportunities[-10:]
        ]

        # Format cross-market opportunities for dashboard
        cross_market_data = [
            {
                'event': o.event_title[:50],
                'kalshi_yes': o.kalshi_yes_price,
                'polymarket_yes': o.polymarket_yes_price,
                'spread': o.spread,
                'direction': o.direction,
                'profit': o.profit_dollars,
                'roi': o.roi_percent,
                'urgency': o.urgency,
                'time': datetime.fromtimestamp(o.timestamp).strftime("%H:%M:%S")
            }
            for o in self.cross_market_opportunities[-10:]
        ]

        # Format Polymarket markets for dashboard
        polymarket_data = [
            {
                'id': m['id'],
                'question': m['question'][:50],
                'yes_price': m['yes_price'],
                'no_price': m['no_price'],
                'volume': m['volume']
            }
            for m in self.polymarket_markets[:10]
        ]

        return {
            'mode': self.mode,
            'markets': list(self.market_prices.values()),
            'trades': self.trades[-50:],
            'total_pnl': self.total_pnl,
            'total_trades': len(self.trades),
            'is_running': self.is_running,
            'is_paused': self.is_paused,
            'best_trade': self.best_trade,
            'time_since_arb': time_since_arb,
            'uptime': time.time() - self.start_time,
            'wallet_balance': self.wallet_balance,
            'whale_signals': whale_signals_data,
            'negrisk_opportunities': negrisk_data,
            'cross_market_opportunities': cross_market_data,
            'polymarket_markets': polymarket_data,
            'matched_markets_count': len(self.matched_markets),
            'base_wallet': {
                'eth_balance': self.base_eth_balance,
                'usdc_balance': self.base_usdc_balance
            },
            'stats': {
                'total_checks': self.total_checks,
                'checks_per_sec': round(self.instant_checks_per_sec, 1),
                'avg_cycle_ms': round(avg_cycle, 3),
                'min_cycle_ms': round(min_cycle, 3),
                'max_cycle_ms': round(max_cycle, 3),
                'target_ms': self.poll_interval_ms,
                'profit_per_min': round(profit_per_min, 2),
                'profit_per_hour': round(profit_per_hour, 2),
                'api_calls': self.api_calls,
                'api_errors': self.api_errors,
                'negrisk_count': len(self.negrisk_opportunities),
                'whale_signals_count': len(self.whale_signals),
                'cross_market_count': len(self.cross_market_opportunities),
                'polymarket_count': len(self.polymarket_markets)
            }
        }

    def set_speed(self, ms):
        """Set polling speed"""
        self.poll_interval_ms = max(10, min(200, ms))
        self.log(f"Polling speed set to {self.poll_interval_ms}ms", "⚙️")

    def toggle_pause(self):
        """Toggle pause state"""
        self.is_paused = not self.is_paused
        state = "PAUSED" if self.is_paused else "RUNNING"
        self.log(f"Bot {state}", "⏸️" if self.is_paused else "▶️")

    async def run(self):
        """Main run loop"""
        try:
            # Initialize based on mode
            if self.mode == "LIVE":
                self.log("Connecting to Kalshi API...", "🔌")

                # Attempt login if credentials provided
                if config.KALSHI_API_KEY:
                    await self.kalshi_client.login()

                # Fetch initial markets
                success = await self.fetch_real_markets()
                if not success:
                    self.log("Failed to fetch markets, falling back to DEMO mode", "⚠️")
                    self.mode = "DEMO"
                    self.create_demo_markets()

                # Fetch wallet balances
                await self.fetch_wallet_balance()  # Coinbase
                await self.fetch_base_balance()     # Base L2

                # Fetch Polymarket markets for cross-market arbitrage
                if config.ENABLE_POLYMARKET:
                    self.log("Connecting to Polymarket API...", "🟣")
                    await self.fetch_polymarket_markets()
            else:
                self.create_demo_markets()

            self.log(f"Monitoring {len(self.markets)} markets at {self.poll_interval_ms}ms...", "⚡")
            if self.polymarket_markets:
                self.log(f"+ {len(self.polymarket_markets)} Polymarket markets for cross-market arb", "🟣")
            self.log("Dashboard: http://localhost:5000", "🌐")
            if self.mode == "LIVE":
                self.log("Mode: LIVE - Real Kalshi + Polymarket data", "🟢")
            else:
                self.log("Mode: DEMO - Simulated data", "🟡")

            await asyncio.gather(
                self.monitor_loop(),
                self.stats_updater(),
                self.performance_reporter(),
                self.wallet_updater() if self.mode == "LIVE" else asyncio.sleep(0)
            )

        except KeyboardInterrupt:
            self.log("Stopped by user", "⏸️")
        except Exception as e:
            self.log(f"Fatal error: {e}", "❌")
            import traceback
            traceback.print_exc()
        finally:
            self.is_running = False
            # Cleanup API sessions
            await self.kalshi_client.close()
            await self.coinbase_client.close()
            await self.polymarket_client.close()
            await self.base_client.close()

    async def wallet_updater(self):
        """Periodically update wallet balances (Coinbase + Base L2)"""
        while self.is_running:
            await asyncio.sleep(60)  # Update every minute
            if not self.is_paused:
                await self.fetch_wallet_balance()  # Coinbase
                await self.fetch_base_balance()     # Base L2


# Flask Web Dashboard
app = Flask(__name__)
CORS(app)

bot_instance = None

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Kalshi Arbitrage Bot</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Roboto Mono', monospace;
            background: #0a0a0f;
            color: #e0e0e0;
            padding: 15px;
            min-height: 100vh;
        }
        .container { max-width: 1400px; margin: 0 auto; }

        /* Header */
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 15px;
        }
        h1 {
            font-size: 1.5em;
            color: #00ff88;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .controls {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
        }
        .btn {
            background: rgba(0,255,136,0.2);
            border: 1px solid #00ff88;
            color: #00ff88;
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            font-family: inherit;
            font-size: 0.85em;
            transition: all 0.2s;
        }
        .btn:hover { background: rgba(0,255,136,0.3); }
        .btn.paused { background: rgba(255,100,100,0.2); border-color: #ff6464; color: #ff6464; }
        .btn.sound-on { background: rgba(0,200,255,0.2); border-color: #00c8ff; color: #00c8ff; }
        .mode-badge {
            font-size: 0.5em;
            padding: 4px 8px;
            border-radius: 4px;
            margin-left: 10px;
            font-weight: normal;
        }
        .mode-badge.live { background: #00ff88; color: #000; }
        .mode-badge.demo { background: #ffaa00; color: #000; }

        .speed-control {
            display: flex;
            align-items: center;
            gap: 8px;
            background: rgba(255,255,255,0.05);
            padding: 6px 12px;
            border-radius: 6px;
        }
        .speed-control input {
            width: 80px;
            accent-color: #00ff88;
        }
        .speed-label { font-size: 0.75em; color: #888; }
        .speed-value { color: #00ff88; font-weight: bold; min-width: 45px; }

        /* Stats Grid */
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }
        .stat-card {
            background: linear-gradient(135deg, rgba(0,255,136,0.1) 0%, rgba(0,100,80,0.1) 100%);
            border: 1px solid rgba(0,255,136,0.2);
            padding: 12px;
            border-radius: 8px;
            text-align: center;
        }
        .stat-card.highlight {
            border-color: #00ff88;
            box-shadow: 0 0 15px rgba(0,255,136,0.2);
        }
        .stat-label { font-size: 0.65em; opacity: 0.6; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px; }
        .stat-value { font-size: 1.3em; font-weight: bold; color: #00ff88; }
        .stat-value.warn { color: #ffaa00; }
        .stat-sub { font-size: 0.7em; opacity: 0.5; margin-top: 2px; }

        /* Performance Bar */
        .perf-bar {
            display: flex;
            gap: 20px;
            background: rgba(0,0,0,0.4);
            padding: 10px 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            flex-wrap: wrap;
            font-size: 0.8em;
        }
        .perf-item { display: flex; gap: 6px; align-items: center; }
        .perf-label { opacity: 0.5; }
        .perf-value { color: #00d4ff; font-weight: bold; }

        /* Markets */
        .section-title {
            font-size: 0.9em;
            color: #00d4ff;
            margin-bottom: 12px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .markets {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }
        .market-card {
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.08);
            padding: 12px;
            border-radius: 8px;
            transition: all 0.15s ease;
        }
        .market-card.arb {
            border-color: #00ff88;
            background: rgba(0,255,136,0.1);
            box-shadow: 0 0 20px rgba(0,255,136,0.2);
            animation: glow 1s ease-in-out infinite alternate;
        }
        @keyframes glow {
            from { box-shadow: 0 0 15px rgba(0,255,136,0.2); }
            to { box-shadow: 0 0 25px rgba(0,255,136,0.4); }
        }
        .market-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }
        .market-type {
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 0.7em;
            font-weight: bold;
        }
        .btc { background: #f7931a; color: #000; }
        .eth { background: #627eea; color: #fff; }
        .arb-badge { color: #00ff88; font-weight: bold; font-size: 0.8em; }
        .market-question { font-size: 0.75em; opacity: 0.6; margin-bottom: 10px; }

        /* Sparkline */
        .sparkline {
            height: 30px;
            margin-bottom: 8px;
            background: rgba(0,0,0,0.3);
            border-radius: 4px;
            overflow: hidden;
            position: relative;
        }
        .sparkline svg { width: 100%; height: 100%; }
        .sparkline-line { fill: none; stroke: #00d4ff; stroke-width: 1.5; }
        .sparkline-area { fill: url(#sparkGradient); opacity: 0.3; }

        .prices {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 6px;
            margin-bottom: 6px;
        }
        .price {
            background: rgba(0,0,0,0.3);
            padding: 6px;
            border-radius: 4px;
            text-align: center;
        }
        .price-label { font-size: 0.65em; opacity: 0.5; }
        .price-value { font-size: 1em; font-weight: bold; }
        .total-row {
            background: rgba(0,0,0,0.4);
            padding: 6px;
            border-radius: 4px;
            text-align: center;
            font-size: 0.8em;
        }
        .total-row.arb { background: #00ff88; color: #000; font-weight: bold; }

        /* Best Trade Card */
        .best-trade {
            background: linear-gradient(135deg, rgba(255,215,0,0.1) 0%, rgba(255,140,0,0.1) 100%);
            border: 1px solid rgba(255,215,0,0.3);
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        .best-trade-title { color: #ffd700; font-size: 0.8em; margin-bottom: 8px; }
        .best-trade-profit { font-size: 1.5em; color: #ffd700; font-weight: bold; }
        .best-trade-details { font-size: 0.75em; opacity: 0.7; margin-top: 4px; }

        /* Trades */
        .trades-container {
            background: rgba(0,0,0,0.3);
            border-radius: 8px;
            overflow: hidden;
        }
        .trades-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 12px;
            background: rgba(0,0,0,0.3);
        }
        .trades-header h3 { font-size: 0.85em; color: #00d4ff; }
        .auto-scroll-toggle {
            font-size: 0.7em;
            display: flex;
            align-items: center;
            gap: 5px;
            cursor: pointer;
        }
        .auto-scroll-toggle input { accent-color: #00ff88; }

        .trades {
            max-height: 350px;
            overflow-y: auto;
            padding: 8px;
        }
        .trade {
            background: rgba(0,255,136,0.05);
            border-left: 3px solid #00ff88;
            padding: 8px 10px;
            margin-bottom: 6px;
            border-radius: 0 6px 6px 0;
            display: grid;
            grid-template-columns: auto 1fr auto;
            gap: 10px;
            align-items: center;
            font-size: 0.8em;
            animation: slideIn 0.3s ease;
        }
        @keyframes slideIn {
            from { opacity: 0; transform: translateX(-10px); }
            to { opacity: 1; transform: translateX(0); }
        }
        .trade.best { border-color: #ffd700; background: rgba(255,215,0,0.1); }
        .trade-time { font-size: 0.75em; opacity: 0.5; }
        .trade-market { font-weight: bold; color: #00d4ff; }
        .trade-prices { font-size: 0.75em; opacity: 0.6; }
        .trade-profit { font-weight: bold; color: #00ff88; }

        .no-data {
            text-align: center;
            padding: 30px;
            opacity: 0.4;
        }

        /* Toast */
        .toast-container {
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 1000;
        }
        .toast {
            background: linear-gradient(135deg, #00ff88 0%, #00cc6a 100%);
            color: #000;
            padding: 12px 20px;
            border-radius: 8px;
            margin-bottom: 10px;
            font-weight: bold;
            animation: toastIn 0.3s ease, toastOut 0.3s ease 2.7s forwards;
            box-shadow: 0 4px 20px rgba(0,255,136,0.4);
        }
        @keyframes toastIn {
            from { opacity: 0; transform: translateX(100px); }
            to { opacity: 1; transform: translateX(0); }
        }
        @keyframes toastOut {
            from { opacity: 1; }
            to { opacity: 0; }
        }

        /* Mobile */
        @media (max-width: 600px) {
            body { padding: 10px; }
            .header { flex-direction: column; align-items: flex-start; }
            h1 { font-size: 1.2em; }
            .stats { grid-template-columns: repeat(2, 1fr); }
            .markets { grid-template-columns: 1fr; }
            .perf-bar { font-size: 0.7em; gap: 12px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>⚡ Kalshi Arbitrage Bot <span id="modeIndicator" class="mode-badge">-</span></h1>
            <div class="controls">
                <div class="speed-control">
                    <span class="speed-label">Speed:</span>
                    <input type="range" id="speedSlider" min="10" max="100" value="30">
                    <span class="speed-value" id="speedValue">30ms</span>
                </div>
                <button class="btn" id="pauseBtn" onclick="togglePause()">⏸️ Pause</button>
                <button class="btn" id="soundBtn" onclick="toggleSound()">🔇 Sound</button>
            </div>
        </div>

        <div class="stats">
            <div class="stat-card highlight">
                <div class="stat-label">Total P/L</div>
                <div class="stat-value" id="totalPnl">$0.00</div>
                <div class="stat-sub" id="profitRate">$0.00/hr</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Trades</div>
                <div class="stat-value" id="totalTrades">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Checks/sec</div>
                <div class="stat-value" id="checksPerSec">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Avg Cycle</div>
                <div class="stat-value" id="avgCycle">-</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Last Arb</div>
                <div class="stat-value" id="lastArb">-</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Status</div>
                <div class="stat-value" id="status">-</div>
            </div>
            <div class="stat-card" style="border-color: #9945FF;">
                <div class="stat-label">Polymarket</div>
                <div class="stat-value" id="polymarketCount" style="color: #9945FF;">0</div>
                <div class="stat-sub" id="matchedCount">0 matched</div>
            </div>
            <div class="stat-card" style="border-color: #0052FF;">
                <div class="stat-label">Base L2</div>
                <div class="stat-value" id="baseBalance" style="color: #0052FF;">$0</div>
                <div class="stat-sub" id="baseEth">0 ETH</div>
            </div>
        </div>

        <div class="perf-bar">
            <div class="perf-item">
                <span class="perf-label">Total Checks:</span>
                <span class="perf-value" id="totalChecks">0</span>
            </div>
            <div class="perf-item">
                <span class="perf-label">Target:</span>
                <span class="perf-value" id="targetMs">30ms</span>
            </div>
            <div class="perf-item">
                <span class="perf-label">Min:</span>
                <span class="perf-value" id="minCycle">-</span>
            </div>
            <div class="perf-item">
                <span class="perf-label">Max:</span>
                <span class="perf-value" id="maxCycle">-</span>
            </div>
            <div class="perf-item">
                <span class="perf-label">Uptime:</span>
                <span class="perf-value" id="uptime">0s</span>
            </div>
        </div>

        <div id="bestTradeContainer"></div>

        <div class="section-title">📊 Kalshi Markets</div>
        <div class="markets" id="markets">
            <div class="no-data">Loading markets...</div>
        </div>

        <div class="section-title" style="color: #9945FF;">🔀 Cross-Market Arbitrage (Kalshi vs Polymarket)</div>
        <div id="crossMarketOpps" class="trades-container" style="margin-bottom: 20px; border-color: rgba(153,69,255,0.3);">
            <div class="no-data">Scanning for cross-market opportunities...</div>
        </div>

        <div class="trades-container">
            <div class="trades-header">
                <h3>📈 Recent Trades</h3>
                <label class="auto-scroll-toggle">
                    <input type="checkbox" id="autoScroll" checked> Auto-scroll
                </label>
            </div>
            <div class="trades" id="trades">
                <div class="no-data">Waiting for arbitrage...</div>
            </div>
        </div>
    </div>

    <div class="toast-container" id="toastContainer"></div>

    <!-- SVG Gradient Definition -->
    <svg width="0" height="0">
        <defs>
            <linearGradient id="sparkGradient" x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" style="stop-color:#00d4ff;stop-opacity:0.5" />
                <stop offset="100%" style="stop-color:#00d4ff;stop-opacity:0" />
            </linearGradient>
        </defs>
    </svg>

    <script>
        let soundEnabled = false;
        let lastTradeCount = 0;
        let isPaused = false;

        // Audio context for notification sound
        let audioCtx = null;
        function playSound() {
            if (!soundEnabled) return;
            if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();
            osc.connect(gain);
            gain.connect(audioCtx.destination);
            osc.frequency.value = 880;
            osc.type = 'sine';
            gain.gain.setValueAtTime(0.3, audioCtx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + 0.2);
            osc.start(audioCtx.currentTime);
            osc.stop(audioCtx.currentTime + 0.2);
        }

        function showToast(message) {
            const container = document.getElementById('toastContainer');
            const toast = document.createElement('div');
            toast.className = 'toast';
            toast.textContent = message;
            container.appendChild(toast);
            setTimeout(() => toast.remove(), 3000);
        }

        function toggleSound() {
            soundEnabled = !soundEnabled;
            const btn = document.getElementById('soundBtn');
            btn.textContent = soundEnabled ? '🔊 Sound' : '🔇 Sound';
            btn.classList.toggle('sound-on', soundEnabled);
        }

        async function togglePause() {
            await fetch('/api/toggle-pause', { method: 'POST' });
            isPaused = !isPaused;
            const btn = document.getElementById('pauseBtn');
            btn.textContent = isPaused ? '▶️ Resume' : '⏸️ Pause';
            btn.classList.toggle('paused', isPaused);
        }

        // Speed slider
        const speedSlider = document.getElementById('speedSlider');
        const speedValue = document.getElementById('speedValue');
        speedSlider.addEventListener('input', async (e) => {
            const ms = e.target.value;
            speedValue.textContent = ms + 'ms';
            await fetch('/api/set-speed?ms=' + ms, { method: 'POST' });
        });

        function formatTime(seconds) {
            if (seconds === null || seconds === undefined) return '-';
            if (seconds < 60) return Math.floor(seconds) + 's';
            if (seconds < 3600) return Math.floor(seconds / 60) + 'm ' + Math.floor(seconds % 60) + 's';
            return Math.floor(seconds / 3600) + 'h ' + Math.floor((seconds % 3600) / 60) + 'm';
        }

        function createSparkline(history) {
            if (!history || history.length < 2) return '';
            const totals = history.map(h => h.total);
            const min = Math.min(...totals);
            const max = Math.max(...totals);
            const range = max - min || 0.01;

            const width = 100;
            const height = 30;
            const points = totals.map((v, i) => {
                const x = (i / (totals.length - 1)) * width;
                const y = height - ((v - min) / range) * height;
                return `${x},${y}`;
            });

            const linePath = 'M' + points.join(' L');
            const areaPath = linePath + ` L${width},${height} L0,${height} Z`;

            return `
                <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
                    <path class="sparkline-area" d="${areaPath}"/>
                    <path class="sparkline-line" d="${linePath}"/>
                </svg>
            `;
        }

        async function updateDashboard() {
            try {
                const response = await fetch('/api/state');
                const data = await response.json();

                // Update mode indicator
                const modeIndicator = document.getElementById('modeIndicator');
                modeIndicator.textContent = data.mode;
                modeIndicator.className = 'mode-badge ' + data.mode.toLowerCase();

                // Update stats
                document.getElementById('totalPnl').textContent = '$' + data.total_pnl.toFixed(2);
                document.getElementById('profitRate').textContent = '$' + data.stats.profit_per_hour.toFixed(2) + '/hr';
                document.getElementById('totalTrades').textContent = data.total_trades;
                document.getElementById('checksPerSec').textContent = data.stats.checks_per_sec;
                document.getElementById('avgCycle').textContent = data.stats.avg_cycle_ms + 'ms';
                document.getElementById('lastArb').textContent = formatTime(data.time_since_arb);
                document.getElementById('status').textContent = data.is_paused ? '⏸️' : '🟢';

                document.getElementById('totalChecks').textContent = data.stats.total_checks.toLocaleString();
                document.getElementById('targetMs').textContent = data.stats.target_ms + 'ms';
                document.getElementById('minCycle').textContent = data.stats.min_cycle_ms + 'ms';
                document.getElementById('maxCycle').textContent = data.stats.max_cycle_ms + 'ms';
                document.getElementById('uptime').textContent = formatTime(data.uptime);

                // Update speed slider if changed externally
                speedSlider.value = data.stats.target_ms;
                speedValue.textContent = data.stats.target_ms + 'ms';

                // Update Polymarket and Base stats
                document.getElementById('polymarketCount').textContent = data.stats.polymarket_count || 0;
                document.getElementById('matchedCount').textContent = (data.matched_markets_count || 0) + ' matched';
                if (data.base_wallet) {
                    document.getElementById('baseBalance').textContent = '$' + (data.base_wallet.usdc_balance || 0).toFixed(2);
                    document.getElementById('baseEth').textContent = (data.base_wallet.eth_balance || 0).toFixed(4) + ' ETH';
                }

                // Best trade
                const bestContainer = document.getElementById('bestTradeContainer');
                if (data.best_trade) {
                    bestContainer.innerHTML = `
                        <div class="best-trade">
                            <div class="best-trade-title">🏆 Best Trade</div>
                            <div class="best-trade-profit">+$${data.best_trade.profit.toFixed(2)}</div>
                            <div class="best-trade-details">${data.best_trade.market} @ ${data.best_trade.time}</div>
                        </div>
                    `;
                }

                // Markets with sparklines
                const marketsDiv = document.getElementById('markets');
                if (data.markets && data.markets.length > 0) {
                    marketsDiv.innerHTML = data.markets.map(m => `
                        <div class="market-card ${m.is_arb ? 'arb' : ''}">
                            <div class="market-header">
                                <span class="market-type ${m.type.toLowerCase()}">${m.type}</span>
                                ${m.is_arb ? '<span class="arb-badge">⚡ ARB</span>' : ''}
                            </div>
                            <div class="market-question">${m.question}</div>
                            <div class="sparkline">${createSparkline(m.history)}</div>
                            <div class="prices">
                                <div class="price">
                                    <div class="price-label">YES</div>
                                    <div class="price-value">$${m.yes.toFixed(4)}</div>
                                </div>
                                <div class="price">
                                    <div class="price-label">NO</div>
                                    <div class="price-value">$${m.no.toFixed(4)}</div>
                                </div>
                            </div>
                            <div class="total-row ${m.is_arb ? 'arb' : ''}">
                                Σ $${m.total.toFixed(4)} → $${m.total_with_fees.toFixed(4)}
                                ${m.is_arb ? ' | +$' + m.profit.toFixed(4) : ''}
                            </div>
                        </div>
                    `).join('');
                }

                // Cross-market opportunities (Kalshi vs Polymarket)
                const crossMarketDiv = document.getElementById('crossMarketOpps');
                if (data.cross_market_opportunities && data.cross_market_opportunities.length > 0) {
                    crossMarketDiv.innerHTML = data.cross_market_opportunities.map(o => `
                        <div class="trade" style="border-left-color: #9945FF; background: rgba(153,69,255,0.1);">
                            <div class="trade-time">${o.time}</div>
                            <div>
                                <div class="trade-market" style="color: #9945FF;">${o.event}</div>
                                <div class="trade-prices">
                                    Kalshi: $${o.kalshi_yes.toFixed(4)} | Poly: $${o.polymarket_yes.toFixed(4)} | Spread: ${(o.spread * 100).toFixed(2)}%
                                </div>
                            </div>
                            <div class="trade-profit" style="color: #9945FF;">+$${o.profit.toFixed(2)}</div>
                        </div>
                    `).join('');
                } else if (data.matched_markets_count > 0) {
                    crossMarketDiv.innerHTML = '<div class="no-data">Monitoring ' + data.matched_markets_count + ' matched markets...</div>';
                } else {
                    crossMarketDiv.innerHTML = '<div class="no-data">Searching for matching markets between Kalshi and Polymarket...</div>';
                }

                // Trades with new trade detection
                const tradesDiv = document.getElementById('trades');
                if (data.trades && data.trades.length > 0) {
                    // Check for new trades
                    if (data.total_trades > lastTradeCount && lastTradeCount > 0) {
                        const newTrade = data.trades[data.trades.length - 1];
                        playSound();
                        showToast(`⚡ +$${newTrade.profit.toFixed(2)} ${newTrade.market}`);
                    }
                    lastTradeCount = data.total_trades;

                    const bestId = data.best_trade ? data.best_trade.id : null;
                    tradesDiv.innerHTML = data.trades.slice().reverse().map(t => `
                        <div class="trade ${t.id === bestId ? 'best' : ''}">
                            <div class="trade-time">${t.time}</div>
                            <div>
                                <div class="trade-market">${t.market}</div>
                                <div class="trade-prices">Y:$${t.yes.toFixed(4)} N:$${t.no.toFixed(4)}</div>
                            </div>
                            <div class="trade-profit">+$${t.profit.toFixed(2)}</div>
                        </div>
                    `).join('');

                    // Auto-scroll
                    if (document.getElementById('autoScroll').checked) {
                        tradesDiv.scrollTop = 0;
                    }
                }

                isPaused = data.is_paused;
                const pauseBtn = document.getElementById('pauseBtn');
                pauseBtn.textContent = isPaused ? '▶️ Resume' : '⏸️ Pause';
                pauseBtn.classList.toggle('paused', isPaused);

            } catch (error) {
                console.error('Dashboard error:', error);
            }
        }

        updateDashboard();
        setInterval(updateDashboard, 100);
    </script>
</body>
</html>
"""


@app.route('/')
def dashboard():
    return DASHBOARD_HTML


@app.route('/api/state')
def get_state():
    if bot_instance:
        return jsonify(bot_instance.get_state())
    return jsonify({'error': 'Bot not running', 'is_running': False})


@app.route('/api/set-speed', methods=['POST'])
def set_speed():
    ms = request.args.get('ms', 30, type=int)
    if bot_instance:
        bot_instance.set_speed(ms)
        return jsonify({'success': True, 'speed': bot_instance.poll_interval_ms})
    return jsonify({'error': 'Bot not running'}), 500


@app.route('/api/toggle-pause', methods=['POST'])
def toggle_pause():
    if bot_instance:
        bot_instance.toggle_pause()
        return jsonify({'success': True, 'paused': bot_instance.is_paused})
    return jsonify({'error': 'Bot not running'}), 500


def get_local_ip():
    """Get the local IP address for network access"""
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "localhost"


def run_flask():
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    # Bind to 0.0.0.0 to allow access from other devices on the network
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False, threaded=True)


def main():
    global bot_instance

    local_ip = get_local_ip()

    # Determine mode for banner
    mode = "LIVE (Kalshi API)" if config.KALSHI_API_KEY and not config.DEMO_MODE else "DEMO (Simulated)"

    print(f"""
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║   ⚡ KALSHI + POLYMARKET + BASE ARBITRAGE BOT ⚡                   ║
║                                                                    ║
║   Based on IMDEA Networks research ($39.59M extraction)            ║
║                                                                    ║
║   Mode: {mode:<56} ║
║                                                                    ║
║   Strategies:                                                      ║
║   • Single-Condition: YES + NO ≠ $1.00                             ║
║   • Multi-Outcome: Sum of probabilities ≠ 100%                     ║
║   • Cross-Market: Kalshi vs Polymarket price differences           ║
║   • Whale Tracking: Follow large trades for signals                ║
║                                                                    ║
║   Platforms:                                                       ║
║   • Kalshi     - US regulated prediction market                    ║
║   • Polymarket - Decentralized prediction market (Polygon)         ║
║   • Base L2    - Coinbase Layer 2 wallet integration               ║
║                                                                    ║
║   Environment Variables:                                           ║
║   • KALSHI_API_KEY        - Kalshi email/API key                   ║
║   • KALSHI_PRIVATE_KEY    - Kalshi password/private key            ║
║   • POLYMARKET_API_KEY    - Polymarket API key (optional)          ║
║   • BASE_WALLET_ADDRESS   - Base L2 wallet address                 ║
║   • COINBASE_API_KEY      - Coinbase CDP API key                   ║
║   • COINBASE_API_SECRET   - Coinbase API secret                    ║
║   • DEMO_MODE=false       - Enable live trading                    ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
""")

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    time.sleep(0.5)
    print("🌐 Dashboard ready!")
    print(f"   Local:   http://localhost:5000")
    print(f"   Network: http://{local_ip}:5000")
    print()

    bot_instance = FastArbitrageBot()

    try:
        asyncio.run(bot_instance.run())
    except KeyboardInterrupt:
        print("\n\n⏹️  Bot stopped by user.")


if __name__ == "__main__":
    main()
