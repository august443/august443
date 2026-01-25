#!/usr/bin/env python3
"""
Fast Multi-Market Arbitrage Bot with Web UI & Real Execution
Bitcoin & Ethereum 15min Markets
"""

import asyncio
import aiohttp
import time
import json
import os
from datetime import datetime
from flask import Flask, jsonify, render_template_string
from flask_cors import CORS
import threading

# Web3 for order signing
try:
    from eth_account import Account
    from eth_account.messages import encode_defunct
    WEB3_AVAILABLE = True
except:
    WEB3_AVAILABLE = False


class FastArbitrageBot:
    def __init__(self):
        # Config
        self.position_size = float(os.getenv('POSITION_SIZE', '10'))
        self.threshold = 0.99
        self.mode = os.getenv('MODE', 'DEMO')
        self.api_key = os.getenv('POLYMARKET_API_KEY', '')
        self.private_key = os.getenv('POLYMARKET_PRIVATE_KEY', '')

        # API endpoints
        self.gamma_api = "https://gamma-api.polymarket.com"
        self.clob_api = "https://clob.polymarket.com"

        # State
        self.session = None
        self.markets = []
        self.total_pnl = 0
        self.trades = []
        self.is_running = True

        # Current prices for dashboard
        self.market_prices = {}

        # Performance
        self.checks = 0
        self.last_report = time.time()

        # Web3 account if available
        self.account = None
        if WEB3_AVAILABLE and self.private_key:
            try:
                self.account = Account.from_key(self.private_key)
                self.log(f"Wallet: {self.account.address[:10]}...", "🔑")
            except:
                self.log("Invalid private key", "❌")

    def log(self, msg, emoji="ℹ️"):
        """Fast logging"""
        t = datetime.now().strftime("%H:%M:%S")
        print(f"[{t}] {emoji} {msg}")

    async def init(self):
        """Initialize HTTP session"""
        timeout = aiohttp.ClientTimeout(total=5, connect=2)
        connector = aiohttp.TCPConnector(
            limit=50,
            ttl_dns_cache=300,
            force_close=False
        )

        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        )

        self.log("Bot initialized - FAST MODE", "⚡")
        self.log(f"Mode: {self.mode}")
        self.log(f"Position size: {self.position_size} shares")

        if self.mode == "LIVE" and not self.api_key:
            self.log("WARNING: LIVE mode but no API key set!", "⚠️")
            self.mode = "DEMO"

    async def find_markets(self):
        """Find all active BTC & ETH 15-minute markets"""
        self.log("Searching for markets...")

        try:
            url = f"{self.gamma_api}/markets"
            params = {"closed": "false", "active": "true", "limit": "100"}

            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    self.log(f"API error: {resp.status}", "❌")
                    self.create_demo_markets()
                    return

                all_markets = await resp.json()

                found = []
                for m in all_markets:
                    q = m.get('question', '').lower()

                    is_crypto = ('bitcoin' in q or 'ethereum' in q or 'eth' in q)
                    is_15min = ('15' in q or 'fifteen' in q) and 'minute' in q
                    is_updown = 'up' in q or 'down' in q

                    if is_crypto and is_15min and is_updown:
                        tokens = m.get('tokens', [])
                        if len(tokens) >= 2:
                            market = {
                                'id': m['id'],
                                'question': m['question'],
                                'yes_token': tokens[0]['token_id'],
                                'no_token': tokens[1]['token_id'],
                                'type': 'BTC' if 'bitcoin' in q else 'ETH'
                            }
                            found.append(market)

                if found:
                    self.markets = found
                    self.log(f"Found {len(found)} markets:", "✅")
                    for m in found:
                        self.log(f"  [{m['type']}] {m['question']}")
                else:
                    self.log("No live markets - using DEMO", "⚠️")
                    self.create_demo_markets()

        except Exception as e:
            self.log(f"Error finding markets: {e}", "❌")
            self.create_demo_markets()

    def create_demo_markets(self):
        """Create demo markets"""
        self.markets = [
            {
                'id': 'demo_btc',
                'question': 'Bitcoin Up or Down - Next 15 Minutes (DEMO)',
                'yes_token': 'demo_btc_yes',
                'no_token': 'demo_btc_no',
                'type': 'BTC',
                'demo': True
            },
            {
                'id': 'demo_eth',
                'question': 'Ethereum Up or Down - Next 15 Minutes (DEMO)',
                'yes_token': 'demo_eth_yes',
                'no_token': 'demo_eth_no',
                'type': 'ETH',
                'demo': True
            }
        ]
        self.mode = "DEMO"

    async def get_prices(self, market):
        """Get YES and NO prices"""
        if market.get('demo'):
            import random
            base = random.uniform(0.45, 0.55)
            yes = round(base + random.uniform(-0.02, 0.02), 4)

            if random.random() < 0.08:  # 8% chance of arb
                no = round(random.uniform(0.96, 0.985) - yes, 4)
            else:
                no = round(1.00 - yes + random.uniform(-0.02, 0.02), 4)

            return yes, no

        try:
            yes_task = self.get_best_ask(market['yes_token'])
            no_task = self.get_best_ask(market['no_token'])
            yes_price, no_price = await asyncio.gather(yes_task, no_task)
            return yes_price, no_price
        except:
            return None, None

    async def get_best_ask(self, token_id):
        """Get best ask price"""
        try:
            url = f"{self.clob_api}/book"
            params = {"token_id": token_id}

            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    book = await resp.json()
                    if book.get('asks') and book['asks']:
                        return float(book['asks'][0]['price'])
        except:
            pass
        return None

    async def place_order(self, token_id, price, size):
        """Place a real order on Polymarket"""
        if not self.api_key:
            self.log("No API key - cannot place order", "❌")
            return None

        try:
            order = {
                "token_id": token_id,
                "price": str(price),
                "size": str(size),
                "side": "BUY",
                "type": "LIMIT"
            }

            # Sign order if we have private key
            if self.account:
                order_hash = json.dumps(order, sort_keys=True)
                message = encode_defunct(text=order_hash)
                signed = self.account.sign_message(message)
                order['signature'] = signed.signature.hex()

            url = f"{self.clob_api}/order"
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            async with self.session.post(url, json=order, headers=headers) as resp:
                if resp.status in [200, 201]:
                    result = await resp.json()
                    self.log(f"Order placed: {result.get('id', 'unknown')}", "✅")
                    return result
                else:
                    error = await resp.text()
                    self.log(f"Order failed ({resp.status}): {error[:100]}", "❌")
                    return None

        except Exception as e:
            self.log(f"Order error: {e}", "❌")
            return None

    async def execute_trade(self, market, yes, no, profit):
        """Execute arbitrage trade"""
        total_profit = profit * self.position_size

        self.log("=" * 70, "⚡")
        self.log(f"ARBITRAGE - {market['type']}", "⚡")
        self.log(f"YES: ${yes:.4f} | NO: ${no:.4f} | Total: ${yes+no:.4f}", "⚡")
        self.log(f"Profit: ${profit:.4f}/share × {self.position_size} = ${total_profit:.2f}", "⚡")
        self.log("=" * 70, "⚡")

        trade = {
            'time': datetime.now().strftime("%H:%M:%S"),
            'market': market['type'],
            'question': market['question'],
            'yes': yes,
            'no': no,
            'profit': total_profit,
            'status': 'pending'
        }

        if self.mode == "LIVE":
            self.log("Placing LIVE orders...", "🚀")

            # Place both orders in parallel
            yes_task = self.place_order(market['yes_token'], yes, self.position_size)
            no_task = self.place_order(market['no_token'], no, self.position_size)

            yes_order, no_order = await asyncio.gather(yes_task, no_task)

            if yes_order and no_order:
                trade['status'] = 'executed'
                trade['yes_order_id'] = yes_order.get('id')
                trade['no_order_id'] = no_order.get('id')
                self.log("Both orders placed successfully!", "✅")
            else:
                trade['status'] = 'failed'
                self.log("Order placement failed", "❌")
        else:
            self.log(f"DEMO: Would buy {self.position_size} YES + NO", "ℹ️")
            trade['status'] = 'demo'

        self.trades.append(trade)
        self.total_pnl += total_profit

        self.log(f"Session P/L: ${self.total_pnl:.2f} ({len(self.trades)} trades)", "✅")

    async def check_market(self, market):
        """Check market for arbitrage"""
        yes, no = await self.get_prices(market)

        if yes is None or no is None:
            return

        self.checks += 1

        total = yes + no
        total_with_fees = total * 1.01
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
            'profit': profit if profit > 0 else 0
        }

        if total_with_fees < 1.0:
            await self.execute_trade(market, yes, no, profit)

    async def monitor_all_markets(self):
        """Monitor all markets"""
        while self.is_running:
            try:
                tasks = [self.check_market(m) for m in self.markets]
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(0.1)
            except Exception as e:
                self.log(f"Error: {e}", "❌")
                await asyncio.sleep(1)

    async def performance_reporter(self):
        """Report stats"""
        while self.is_running:
            await asyncio.sleep(60)

            elapsed = time.time() - self.last_report
            checks_per_sec = self.checks / elapsed if elapsed > 0 else 0

            self.log("=" * 70, "📊")
            self.log(f"PERFORMANCE: {checks_per_sec:.1f} checks/sec", "📊")
            self.log(f"Markets: {len(self.markets)} | Trades: {len(self.trades)} | P/L: ${self.total_pnl:.2f}", "📊")
            self.log("=" * 70, "📊")

            self.checks = 0
            self.last_report = time.time()

    async def market_refresher(self):
        """Refresh markets"""
        while self.is_running:
            await asyncio.sleep(300)
            self.log("Refreshing markets...", "🔄")
            await self.find_markets()

    def get_state(self):
        """Get current state for dashboard"""
        return {
            'mode': self.mode,
            'markets': list(self.market_prices.values()),
            'trades': self.trades[-20:],  # Last 20 trades
            'total_pnl': self.total_pnl,
            'total_trades': len(self.trades),
            'is_running': self.is_running
        }

    async def run(self):
        """Main run loop"""
        try:
            await self.init()
            await self.find_markets()

            if not self.markets:
                self.log("No markets. Exiting.", "❌")
                return

            self.log(f"Monitoring {len(self.markets)} markets...", "⚡")
            self.log("Dashboard: http://localhost:5000", "🌐")

            await asyncio.gather(
                self.monitor_all_markets(),
                self.performance_reporter(),
                self.market_refresher()
            )

        except KeyboardInterrupt:
            self.log("Stopped", "⏸️")
        finally:
            self.is_running = False
            if self.session:
                await self.session.close()


# Flask Web Dashboard
app = Flask(__name__)
CORS(app)

bot_instance = None

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Arbitrage Bot Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            min-height: 100vh;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { text-align: center; margin-bottom: 30px; font-size: 2.5em; }
        h2 { margin-bottom: 15px; }
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            padding: 20px;
            border-radius: 15px;
            border: 1px solid rgba(255,255,255,0.2);
        }
        .stat-label { font-size: 0.9em; opacity: 0.8; margin-bottom: 5px; }
        .stat-value { font-size: 2em; font-weight: bold; }
        .markets {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .market-card {
            background: rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            padding: 20px;
            border-radius: 15px;
            border: 2px solid rgba(255,255,255,0.2);
        }
        .market-card.arb {
            border-color: #00ff88;
            animation: pulse 1s infinite;
        }
        @keyframes pulse {
            0%, 100% { box-shadow: 0 0 20px #00ff88; }
            50% { box-shadow: 0 0 40px #00ff88; }
        }
        .market-type {
            display: inline-block;
            padding: 5px 10px;
            border-radius: 5px;
            font-size: 0.8em;
            font-weight: bold;
            margin-bottom: 10px;
        }
        .btc { background: #f7931a; }
        .eth { background: #627eea; }
        .market-question { font-size: 0.9em; margin-bottom: 15px; opacity: 0.9; }
        .prices {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            margin-bottom: 10px;
        }
        .price { background: rgba(0,0,0,0.2); padding: 10px; border-radius: 8px; }
        .price-label { font-size: 0.8em; opacity: 0.7; }
        .price-value { font-size: 1.2em; font-weight: bold; }
        .total {
            background: rgba(0,0,0,0.3);
            padding: 10px;
            border-radius: 8px;
            margin-top: 10px;
        }
        .total.arb { background: #00ff88; color: #000; }
        .trades {
            background: rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            padding: 20px;
            border-radius: 15px;
            border: 1px solid rgba(255,255,255,0.2);
        }
        .trade {
            background: rgba(0,0,0,0.2);
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 10px;
            display: grid;
            grid-template-columns: auto 1fr auto;
            gap: 15px;
            align-items: center;
        }
        .trade-time { opacity: 0.7; font-size: 0.9em; }
        .trade-profit {
            font-size: 1.2em;
            font-weight: bold;
            color: #00ff88;
        }
        .no-markets {
            text-align: center;
            padding: 40px;
            opacity: 0.6;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>⚡ Arbitrage Bot Dashboard</h1>

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
                <div class="stat-value" id="status">Running</div>
            </div>
        </div>

        <h2>Markets</h2>
        <div class="markets" id="markets">
            <div class="no-markets">Loading markets...</div>
        </div>

        <h2 style="margin-top: 30px;">Recent Trades</h2>
        <div class="trades" id="trades">
            <div style="text-align: center; opacity: 0.5;">No trades yet</div>
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
                document.getElementById('status').textContent = data.is_running ? '🟢 Running' : '🔴 Stopped';

                // Update markets
                const marketsDiv = document.getElementById('markets');
                if (data.markets && data.markets.length > 0) {
                    marketsDiv.innerHTML = data.markets.map(m => `
                        <div class="market-card ${m.is_arb ? 'arb' : ''}">
                            <span class="market-type ${m.type.toLowerCase()}">${m.type}</span>
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
                                Total: $${m.total.toFixed(4)} (w/fees: $${m.total_with_fees.toFixed(4)})
                                ${m.is_arb ? '<br><strong>⚡ ARBITRAGE: $' + m.profit.toFixed(4) + '/share</strong>' : ''}
                            </div>
                        </div>
                    `).join('');
                } else {
                    marketsDiv.innerHTML = '<div class="no-markets">Waiting for market data...</div>';
                }

                // Update trades
                const tradesDiv = document.getElementById('trades');
                if (data.trades && data.trades.length > 0) {
                    tradesDiv.innerHTML = data.trades.slice().reverse().map(t => `
                        <div class="trade">
                            <div class="trade-time">[${t.time}]</div>
                            <div>
                                <strong>${t.market}</strong> - ${t.question.substring(0, 50)}...<br>
                                <small>YES: $${t.yes.toFixed(4)} | NO: $${t.no.toFixed(4)}</small>
                            </div>
                            <div class="trade-profit">+$${t.profit.toFixed(2)}</div>
                        </div>
                    `).join('');
                } else {
                    tradesDiv.innerHTML = '<div style="text-align: center; opacity: 0.5;">No trades yet</div>';
                }

            } catch (error) {
                console.error('Error:', error);
            }
        }

        updateDashboard();
        setInterval(updateDashboard, 500);  // Update every 500ms
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
    return jsonify({'error': 'Bot not running'})


def run_flask():
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)


def main():
    global bot_instance

    print("""
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║     ARBITRAGE BOT - BTC & ETH with Web Dashboard & Execution    ║
║                                                                  ║
║  Dashboard: http://localhost:5000                               ║
║  Strategy: Buy YES + NO when total < $0.99                      ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
""")

    # Start Flask in background
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    time.sleep(1)
    print("\n🌐 Dashboard started at http://localhost:5000\n")

    bot_instance = FastArbitrageBot()

    try:
        asyncio.run(bot_instance.run())
    except KeyboardInterrupt:
        print("\n\nBot stopped.")


if __name__ == "__main__":
    main()
