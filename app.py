#!/usr/bin/env python3
"""
Fast Local Arbitrage Bot with Web UI
25-100ms configurable polling - Local Demo Mode
Enhanced Performance  & UX
"""

import asyncio
import time
import random
from collections import deque
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
import threading


class FastArbitrageBot:
    def __init__(self):
        # Config - configurable via API
        self.poll_interval_ms = 30
        self.position_size = 10.0
        self.threshold = 0.99
        self.mode = "LOCAL"

        # State
        self.markets = []
        self.total_pnl = 0.0
        self.trades = []
        self.is_running = True
        self.is_paused = False
        self.start_time = time.time()

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

        self.log("Bot initialized - LOCAL FAST MODE", "⚡")
        self.log(f"Target polling: {self.poll_interval_ms}ms")

    def log(self, msg, emoji="ℹ️"):
        """Logging with millisecond precision"""
        t = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{t}] {emoji} {msg}")

    def create_demo_markets(self):
        """Create local demo markets"""
        self.markets = [
            {
                'id': 'local_btc_up',
                'question': 'BTC Up Next 30ms Window',
                'yes_token': 'btc_up_yes',
                'no_token': 'btc_up_no',
                'type': 'BTC',
                'base_yes': 0.52,
                'volatility': 0.03
            },
            {
                'id': 'local_btc_down',
                'question': 'BTC Down Next 30ms Window',
                'yes_token': 'btc_down_yes',
                'no_token': 'btc_down_no',
                'type': 'BTC',
                'base_yes': 0.48,
                'volatility': 0.03
            },
            {
                'id': 'local_eth_up',
                'question': 'ETH Up Next 30ms Window',
                'yes_token': 'eth_up_yes',
                'no_token': 'eth_up_no',
                'type': 'ETH',
                'base_yes': 0.50,
                'volatility': 0.04
            },
            {
                'id': 'local_eth_down',
                'question': 'ETH Down Next 30ms Window',
                'yes_token': 'eth_down_yes',
                'no_token': 'eth_down_no',
                'type': 'ETH',
                'base_yes': 0.50,
                'volatility': 0.04
            }
        ]

        # Initialize price history for each market
        for m in self.markets:
            self.price_history[m['id']] = deque(maxlen=50)

        self.log(f"Created {len(self.markets)} local demo markets", "✅")

    def get_prices(self, market):
        """Generate simulated prices with occasional arbitrage"""
        base = market.get('base_yes', 0.50)
        vol = market.get('volatility', 0.03)

        # Simulate price movement
        yes = base + random.gauss(0, vol)
        yes = max(0.01, min(0.99, yes))
        yes = round(yes, 4)

        # 6% chance of arbitrage opportunity
        if random.random() < 0.06:
            no = round(random.uniform(0.94, 0.975) - yes, 4)
            no = max(0.01, no)
        else:
            no = round(1.0 - yes + random.gauss(0, 0.01), 4)
            no = max(0.01, min(0.99, no))

        return yes, no

    def execute_trade(self, market, yes, no, profit):
        """Execute demo arbitrage trade"""
        total_profit = profit * self.position_size
        now = datetime.now()

        self.log("=" * 60, "⚡")
        self.log(f"ARBITRAGE DETECTED - {market['type']}", "💰")
        self.log(f"Market: {market['question']}")
        self.log(f"YES: ${yes:.4f} | NO: ${no:.4f} | Sum: ${yes+no:.4f}")
        self.log(f"Profit: ${profit:.4f}/share × {self.position_size} = ${total_profit:.2f}")
        self.log("=" * 60, "⚡")

        trade = {
            'id': len(self.trades) + 1,
            'time': now.strftime("%H:%M:%S.%f")[:-3],
            'timestamp': time.time(),
            'market': market['type'],
            'question': market['question'],
            'yes': yes,
            'no': no,
            'profit': total_profit,
            'status': 'executed'
        }

        self.trades.append(trade)
        self.total_pnl += total_profit
        self.last_arb_time = time.time()

        # Track best trade
        if self.best_trade is None or total_profit > self.best_trade['profit']:
            self.best_trade = trade.copy()

        self.log(f"Total P/L: ${self.total_pnl:.2f} ({len(self.trades)} trades)", "✅")

    def check_market(self, market):
        """Check single market for arbitrage"""
        yes, no = self.get_prices(market)

        # Track timing for checks/sec calculation
        now_ns = time.perf_counter_ns()
        self.check_times.append(now_ns)
        self.last_check_time = now_ns

        self.checks += 1
        self.total_checks += 1

        total = yes + no
        total_with_fees = total * 1.01
        profit = 1.0 - total_with_fees

        # Store price history for sparkline
        self.price_history[market['id']].append({
            'yes': yes,
            'no': no,
            'total': total,
            'time': time.time()
        })

        # Update dashboard data
        self.market_prices[market['id']] = {
            'id': market['id'],
            'type': market['type'],
            'question': market['question'],
            'yes': yes,
            'no': no,
            'total': total,
            'total_with_fees': total_with_fees,
            'is_arb': total_with_fees < 1.0,
            'profit': max(0, profit),
            'history': list(self.price_history[market['id']])
        }

        # Execute if arbitrage opportunity
        if total_with_fees < 1.0:
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

        while self.is_running:
            if self.is_paused:
                await asyncio.sleep(0.1)
                continue

            target_interval = self.poll_interval_ms / 1000.0
            cycle_start = time.perf_counter_ns()

            try:
                for market in self.markets:
                    self.check_market(market)
            except Exception as e:
                self.log(f"Error in cycle: {e}", "❌")

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
            self.log(f"PERFORMANCE REPORT", "📊")
            self.log(f"Checks/sec: {checks_per_sec:.1f} (instant: {self.instant_checks_per_sec:.1f})")
            self.log(f"Cycle time: avg={avg_cycle:.2f}ms min={min_cycle:.2f}ms max={max_cycle:.2f}ms")
            self.log(f"Total checks: {self.total_checks} | Trades: {len(self.trades)} | P/L: ${self.total_pnl:.2f}")
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
            'stats': {
                'total_checks': self.total_checks,
                'checks_per_sec': round(self.instant_checks_per_sec, 1),
                'avg_cycle_ms': round(avg_cycle, 3),
                'min_cycle_ms': round(min_cycle, 3),
                'max_cycle_ms': round(max_cycle, 3),
                'target_ms': self.poll_interval_ms,
                'profit_per_min': round(profit_per_min, 2),
                'profit_per_hour': round(profit_per_hour, 2)
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
            self.create_demo_markets()

            self.log(f"Monitoring {len(self.markets)} markets at {self.poll_interval_ms}ms...", "⚡")
            self.log("Dashboard: http://localhost:5000", "🌐")

            await asyncio.gather(
                self.monitor_loop(),
                self.stats_updater(),
                self.performance_reporter()
            )

        except KeyboardInterrupt:
            self.log("Stopped by user", "⏸️")
        except Exception as e:
            self.log(f"Fatal error: {e}", "❌")
        finally:
            self.is_running = False


# Flask Web Dashboard
app = Flask(__name__)
CORS(app)

bot_instance = None

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Fast Arbitrage Bot</title>
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
            <h1>⚡ Fast Arbitrage Bot</h1>
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

    print("""
╔════════════════════════════════════════════════════════════════╗
║                                                                ║
║   ⚡ FAST LOCAL ARBITRAGE BOT - ENHANCED ⚡                    ║
║                                                                ║
║   Mode: LOCAL (no external APIs)                               ║
║   Polling: 25-100ms configurable                               ║
║                                                                ║
║   Features: Sparklines, Sound alerts, Pause/Resume,            ║
║             Speed control, Best trade tracking                 ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝
""")

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    time.sleep(0.5)
    print("🌐 Dashboard ready!")
    print(f"   Local:   http://localhost:5000")
    print(f"   Network: http://{local_ip}:5000  ← Use this on iPhone")
    print()

    bot_instance = FastArbitrageBot()

    try:
        asyncio.run(bot_instance.run())
    except KeyboardInterrupt:
        print("\n\n⏹️  Bot stopped by user.")


if __name__ == "__main__":
    main()
