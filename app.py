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

    # Coinbase API
    COINBASE_API_KEY: str = os.getenv("COINBASE_API_KEY", "")
    COINBASE_API_SECRET: str = os.getenv("COINBASE_API_SECRET", "")

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
    POSITION_SIZE: float = float(os.getenv("POSITION_SIZE", "10.0"))

    # Strategy toggles
    ENABLE_SINGLE_CONDITION: bool = True
    ENABLE_MULTI_OUTCOME: bool = True  # Similar to NegRisk for multi-outcome markets
    ENABLE_WHALE_TRACKING: bool = True  # Track large trades

    # Whale tracking settings
    WHALE_THRESHOLD: float = float(os.getenv("WHALE_THRESHOLD", "5000"))  # $5K minimum
    WHALE_LOOKBACK_TRADES: int = 50  # Recent trades to analyze

    # Parallel request settings
    MAX_CONCURRENT_REQUESTS: int = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))

    # Mode
    DEMO_MODE: bool = os.getenv("DEMO_MODE", "true").lower() == "true"


config = Config()


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
        self.detector = ArbitrageDetector()

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
        """Execute arbitrage trade (demo) or log opportunity (live)"""
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
            'status': 'detected' if self.mode == "LIVE" else 'simulated'
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
        last_whale_scan = 0
        market_refresh_interval = 60  # Refresh market list every 60 seconds
        negrisk_scan_interval = 30  # Scan NegRisk every 30 seconds
        whale_scan_interval = 45  # Scan whales every 45 seconds

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

                    # Whale tracking scan
                    if config.ENABLE_WHALE_TRACKING and now - last_whale_scan > whale_scan_interval:
                        await self.scan_whale_activity()
                        last_whale_scan = now
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
                'whale_signals_count': len(self.whale_signals)
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

                # Fetch wallet balance if Coinbase configured
                await self.fetch_wallet_balance()
            else:
                self.create_demo_markets()

            self.log(f"Monitoring {len(self.markets)} markets at {self.poll_interval_ms}ms...", "⚡")
            self.log("Dashboard: http://localhost:5000", "🌐")
            if self.mode == "LIVE":
                self.log("Mode: LIVE - Real Kalshi data", "🟢")
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

    async def wallet_updater(self):
        """Periodically update wallet balance"""
        while self.is_running:
            await asyncio.sleep(60)  # Update every minute
            if not self.is_paused:
                await self.fetch_wallet_balance()


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

        <div class="section-title">📊 Markets</div>
        <div class="markets" id="markets">
            <div class="no-data">Loading markets...</div>
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
╔════════════════════════════════════════════════════════════════╗
║                                                                ║
║   ⚡ KALSHI + COINBASE ARBITRAGE BOT ⚡                        ║
║                                                                ║
║   Based on IMDEA Networks research ($39.59M extraction)        ║
║                                                                ║
║   Mode: {mode:<52} ║
║                                                                ║
║   Strategies:                                                  ║
║   • Single-Condition: YES + NO ≠ $1.00                         ║
║   • Multi-Outcome: Sum of probabilities ≠ 100%                 ║
║                                                                ║
║   Environment Variables:                                       ║
║   • KALSHI_API_KEY     - Kalshi email/API key                  ║
║   • KALSHI_PRIVATE_KEY - Kalshi password/private key           ║
║   • COINBASE_API_KEY   - Coinbase CDP API key                  ║
║   • COINBASE_API_SECRET- Coinbase API secret                   ║
║   • DEMO_MODE=false    - Enable live trading                   ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝
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
