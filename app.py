#!/usr/bin/env python3
"""
Fast Local Arbitrage Bot with Web UI
25-40ms polling - Local Demo Mode
"""

import asyncio
import time
import random
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
import threading


class FastArbitrageBot:
    def __init__(self):
        # Config - 30ms target (middle of 25-40ms range)
        self.poll_interval_ms = 30
        self.position_size = 10.0
        self.threshold = 0.99
        self.mode = "LOCAL"

        # State
        self.markets = []
        self.total_pnl = 0.0
        self.trades = []
        self.is_running = True

        # Current prices for dashboard
        self.market_prices = {}

        # Performance tracking
        self.checks = 0
        self.total_checks = 0
        self.last_report = time.time()
        self.cycle_times = []  # Track actual cycle times
        self.avg_cycle_ms = 0.0
        self.min_cycle_ms = 999.0
        self.max_cycle_ms = 0.0

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
            # Create arb: YES + NO < 0.99 (before fees)
            no = round(random.uniform(0.94, 0.975) - yes, 4)
            no = max(0.01, no)
        else:
            # Normal: YES + NO ≈ 1.00
            no = round(1.0 - yes + random.gauss(0, 0.01), 4)
            no = max(0.01, min(0.99, no))

        return yes, no

    def execute_trade(self, market, yes, no, profit):
        """Execute demo arbitrage trade"""
        total_profit = profit * self.position_size

        self.log("=" * 60, "⚡")
        self.log(f"ARBITRAGE DETECTED - {market['type']}", "💰")
        self.log(f"Market: {market['question']}")
        self.log(f"YES: ${yes:.4f} | NO: ${no:.4f} | Sum: ${yes+no:.4f}")
        self.log(f"Profit: ${profit:.4f}/share × {self.position_size} = ${total_profit:.2f}")
        self.log("=" * 60, "⚡")

        trade = {
            'time': datetime.now().strftime("%H:%M:%S.%f")[:-3],
            'market': market['type'],
            'question': market['question'],
            'yes': yes,
            'no': no,
            'profit': total_profit,
            'status': 'executed'
        }

        self.trades.append(trade)
        self.total_pnl += total_profit

        self.log(f"Total P/L: ${self.total_pnl:.2f} ({len(self.trades)} trades)", "✅")

    def check_market(self, market):
        """Check single market for arbitrage"""
        yes, no = self.get_prices(market)

        self.checks += 1
        self.total_checks += 1

        total = yes + no
        total_with_fees = total * 1.01  # 1% fee
        profit = 1.0 - total_with_fees

        # Update dashboard data
        self.market_prices[market['id']] = {
            'type': market['type'],
            'question': market['question'],
            'yes': yes,
            'no': no,
            'total': total,
            'total_with_fees': total_with_fees,
            'is_arb': total_with_fees < 1.0,
            'profit': max(0, profit)
        }

        # Execute if arbitrage opportunity
        if total_with_fees < 1.0:
            self.execute_trade(market, yes, no, profit)

    async def monitor_loop(self):
        """Main monitoring loop - targets 25-40ms"""
        self.log("Starting monitor loop...", "🔄")

        target_interval = self.poll_interval_ms / 1000.0  # Convert to seconds

        while self.is_running:
            cycle_start = time.perf_counter()

            try:
                # Check all markets
                for market in self.markets:
                    self.check_market(market)

            except Exception as e:
                self.log(f"Error in cycle: {e}", "❌")

            # Calculate cycle time
            cycle_time = (time.perf_counter() - cycle_start) * 1000  # ms
            self.cycle_times.append(cycle_time)

            # Keep only last 100 cycle times
            if len(self.cycle_times) > 100:
                self.cycle_times.pop(0)

            # Update stats
            self.avg_cycle_ms = sum(self.cycle_times) / len(self.cycle_times)
            self.min_cycle_ms = min(self.min_cycle_ms, cycle_time)
            self.max_cycle_ms = max(self.max_cycle_ms, cycle_time)

            # Sleep to maintain target interval
            sleep_time = max(0, target_interval - (cycle_time / 1000.0))
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    async def performance_reporter(self):
        """Report performance stats every 10 seconds"""
        while self.is_running:
            await asyncio.sleep(10)

            elapsed = time.time() - self.last_report
            checks_per_sec = self.checks / elapsed if elapsed > 0 else 0

            self.log("=" * 60, "📊")
            self.log(f"PERFORMANCE REPORT", "📊")
            self.log(f"Checks/sec: {checks_per_sec:.1f} ({self.checks} in {elapsed:.1f}s)")
            self.log(f"Cycle time: avg={self.avg_cycle_ms:.2f}ms min={self.min_cycle_ms:.2f}ms max={self.max_cycle_ms:.2f}ms")
            self.log(f"Total checks: {self.total_checks} | Trades: {len(self.trades)} | P/L: ${self.total_pnl:.2f}")
            self.log("=" * 60, "📊")

            self.checks = 0
            self.last_report = time.time()

    def get_state(self):
        """Get current state for dashboard"""
        return {
            'mode': self.mode,
            'markets': list(self.market_prices.values()),
            'trades': self.trades[-50:],  # Last 50 trades
            'total_pnl': self.total_pnl,
            'total_trades': len(self.trades),
            'is_running': self.is_running,
            'stats': {
                'total_checks': self.total_checks,
                'avg_cycle_ms': round(self.avg_cycle_ms, 2),
                'min_cycle_ms': round(self.min_cycle_ms, 2),
                'max_cycle_ms': round(self.max_cycle_ms, 2),
                'target_ms': self.poll_interval_ms
            }
        }

    async def run(self):
        """Main run loop"""
        try:
            self.create_demo_markets()

            self.log(f"Monitoring {len(self.markets)} markets at {self.poll_interval_ms}ms...", "⚡")
            self.log("Dashboard: http://localhost:5000", "🌐")

            await asyncio.gather(
                self.monitor_loop(),
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
    <title>Fast Arbitrage Bot - Local</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, monospace;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            color: #eee;
            padding: 20px;
            min-height: 100vh;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { text-align: center; margin-bottom: 10px; font-size: 2em; color: #00ff88; }
        .subtitle { text-align: center; margin-bottom: 25px; opacity: 0.7; font-size: 0.9em; }
        h2 { margin-bottom: 12px; color: #00d4ff; font-size: 1.1em; }

        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-bottom: 25px;
        }
        .stat-card {
            background: rgba(0,255,136,0.1);
            border: 1px solid rgba(0,255,136,0.3);
            padding: 15px;
            border-radius: 10px;
            text-align: center;
        }
        .stat-label { font-size: 0.75em; opacity: 0.7; margin-bottom: 5px; text-transform: uppercase; }
        .stat-value { font-size: 1.5em; font-weight: bold; color: #00ff88; }
        .stat-value.highlight { color: #ff6b6b; }

        .perf-stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 10px;
            margin-bottom: 25px;
            background: rgba(0,0,0,0.3);
            padding: 15px;
            border-radius: 10px;
        }
        .perf-stat { text-align: center; }
        .perf-label { font-size: 0.7em; opacity: 0.6; }
        .perf-value { font-size: 1.1em; color: #00d4ff; font-family: monospace; }

        .markets {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 15px;
            margin-bottom: 25px;
        }
        .market-card {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.1);
            padding: 15px;
            border-radius: 10px;
            transition: all 0.15s ease;
        }
        .market-card.arb {
            border-color: #00ff88;
            background: rgba(0,255,136,0.15);
            box-shadow: 0 0 20px rgba(0,255,136,0.3);
        }
        .market-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }
        .market-type {
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 0.75em;
            font-weight: bold;
        }
        .btc { background: #f7931a; color: #000; }
        .eth { background: #627eea; color: #fff; }
        .market-question { font-size: 0.8em; opacity: 0.8; margin-bottom: 10px; }

        .prices {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            margin-bottom: 8px;
        }
        .price {
            background: rgba(0,0,0,0.3);
            padding: 8px;
            border-radius: 6px;
            text-align: center;
        }
        .price-label { font-size: 0.7em; opacity: 0.6; }
        .price-value { font-size: 1.1em; font-weight: bold; font-family: monospace; }

        .total {
            background: rgba(0,0,0,0.4);
            padding: 8px;
            border-radius: 6px;
            text-align: center;
            font-family: monospace;
            font-size: 0.85em;
        }
        .total.arb {
            background: #00ff88;
            color: #000;
            font-weight: bold;
        }

        .trades {
            background: rgba(0,0,0,0.3);
            padding: 15px;
            border-radius: 10px;
            max-height: 400px;
            overflow-y: auto;
        }
        .trade {
            background: rgba(0,255,136,0.1);
            border-left: 3px solid #00ff88;
            padding: 10px;
            margin-bottom: 8px;
            border-radius: 0 6px 6px 0;
            display: grid;
            grid-template-columns: auto 1fr auto;
            gap: 12px;
            align-items: center;
            font-size: 0.85em;
        }
        .trade-time {
            font-family: monospace;
            opacity: 0.7;
            font-size: 0.8em;
        }
        .trade-details { }
        .trade-market { font-weight: bold; color: #00d4ff; }
        .trade-prices { font-size: 0.8em; opacity: 0.7; font-family: monospace; }
        .trade-profit {
            font-size: 1.1em;
            font-weight: bold;
            color: #00ff88;
            font-family: monospace;
        }
        .no-trades {
            text-align: center;
            padding: 30px;
            opacity: 0.5;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>⚡ Fast Arbitrage Bot</h1>
        <div class="subtitle">Local Demo Mode - 25-40ms Polling</div>

        <div class="stats">
            <div class="stat-card">
                <div class="stat-label">Mode</div>
                <div class="stat-value" id="mode">-</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Trades</div>
                <div class="stat-value" id="totalTrades">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total P/L</div>
                <div class="stat-value" id="totalPnl">$0.00</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Status</div>
                <div class="stat-value" id="status">-</div>
            </div>
        </div>

        <div class="perf-stats">
            <div class="perf-stat">
                <div class="perf-label">Total Checks</div>
                <div class="perf-value" id="totalChecks">0</div>
            </div>
            <div class="perf-stat">
                <div class="perf-label">Target</div>
                <div class="perf-value" id="targetMs">-</div>
            </div>
            <div class="perf-stat">
                <div class="perf-label">Avg Cycle</div>
                <div class="perf-value" id="avgCycle">-</div>
            </div>
            <div class="perf-stat">
                <div class="perf-label">Min Cycle</div>
                <div class="perf-value" id="minCycle">-</div>
            </div>
            <div class="perf-stat">
                <div class="perf-label">Max Cycle</div>
                <div class="perf-value" id="maxCycle">-</div>
            </div>
        </div>

        <h2>Markets</h2>
        <div class="markets" id="markets">
            <div class="no-trades">Loading markets...</div>
        </div>

        <h2 style="margin-top: 20px;">Recent Trades</h2>
        <div class="trades" id="trades">
            <div class="no-trades">No trades yet</div>
        </div>
    </div>

    <script>
        async function updateDashboard() {
            try {
                const response = await fetch('/api/state');
                const data = await response.json();

                document.getElementById('mode').textContent = data.mode;
                document.getElementById('totalTrades').textContent = data.total_trades;
                document.getElementById('totalPnl').textContent = '$' + data.total_pnl.toFixed(2);
                document.getElementById('status').textContent = data.is_running ? '🟢 RUN' : '🔴 STOP';

                // Performance stats
                if (data.stats) {
                    document.getElementById('totalChecks').textContent = data.stats.total_checks.toLocaleString();
                    document.getElementById('targetMs').textContent = data.stats.target_ms + 'ms';
                    document.getElementById('avgCycle').textContent = data.stats.avg_cycle_ms + 'ms';
                    document.getElementById('minCycle').textContent = data.stats.min_cycle_ms + 'ms';
                    document.getElementById('maxCycle').textContent = data.stats.max_cycle_ms + 'ms';
                }

                // Update markets
                const marketsDiv = document.getElementById('markets');
                if (data.markets && data.markets.length > 0) {
                    marketsDiv.innerHTML = data.markets.map(m => `
                        <div class="market-card ${m.is_arb ? 'arb' : ''}">
                            <div class="market-header">
                                <span class="market-type ${m.type.toLowerCase()}">${m.type}</span>
                                ${m.is_arb ? '<span style="color:#00ff88;font-weight:bold;">⚡ ARB</span>' : ''}
                            </div>
                            <div class="market-question">${m.question}</div>
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
                            <div class="total ${m.is_arb ? 'arb' : ''}">
                                Sum: $${m.total.toFixed(4)} | +Fees: $${m.total_with_fees.toFixed(4)}
                                ${m.is_arb ? ' | PROFIT: $' + m.profit.toFixed(4) : ''}
                            </div>
                        </div>
                    `).join('');
                } else {
                    marketsDiv.innerHTML = '<div class="no-trades">Waiting for market data...</div>';
                }

                // Update trades
                const tradesDiv = document.getElementById('trades');
                if (data.trades && data.trades.length > 0) {
                    tradesDiv.innerHTML = data.trades.slice().reverse().map(t => `
                        <div class="trade">
                            <div class="trade-time">${t.time}</div>
                            <div class="trade-details">
                                <div class="trade-market">${t.market} - ${t.question}</div>
                                <div class="trade-prices">YES: $${t.yes.toFixed(4)} | NO: $${t.no.toFixed(4)}</div>
                            </div>
                            <div class="trade-profit">+$${t.profit.toFixed(2)}</div>
                        </div>
                    `).join('');
                } else {
                    tradesDiv.innerHTML = '<div class="no-trades">No trades yet - waiting for arbitrage...</div>';
                }

            } catch (error) {
                console.error('Dashboard error:', error);
            }
        }

        updateDashboard();
        setInterval(updateDashboard, 100);  // Update dashboard every 100ms
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


def run_flask():
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)  # Suppress Flask logs
    app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False, threaded=True)


def main():
    global bot_instance

    print("""
╔════════════════════════════════════════════════════════════════╗
║                                                                ║
║   ⚡ FAST LOCAL ARBITRAGE BOT ⚡                               ║
║                                                                ║
║   Mode: LOCAL (no external APIs)                               ║
║   Polling: 25-40ms target                                      ║
║   Dashboard: http://localhost:5000                             ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝
""")

    # Start Flask in background
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    time.sleep(0.5)
    print("🌐 Dashboard ready at http://localhost:5000\n")

    bot_instance = FastArbitrageBot()

    try:
        asyncio.run(bot_instance.run())
    except KeyboardInterrupt:
        print("\n\n⏹️  Bot stopped by user.")


if __name__ == "__main__":
    main()
