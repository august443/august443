#!/bin/bash
# DigitalOcean Droplet Setup Script for Polymarket HFT Bot
# Run this on a fresh Ubuntu 22.04 droplet

set -e

echo "=========================================="
echo "  Polymarket HFT Bot - Server Setup"
echo "=========================================="

# Update system
echo "[1/5] Updating system..."
apt-get update && apt-get upgrade -y

# Install Docker
echo "[2/5] Installing Docker..."
apt-get install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Create app directory
echo "[3/5] Setting up application..."
mkdir -p /opt/polymarket-bot
cd /opt/polymarket-bot

# Create .env file template
echo "[4/5] Creating environment file..."
cat > .env << 'EOF'
# Polymarket API Credentials
POLYMARKET_API_KEY=your-api-key-here
POLYMARKET_API_SECRET=your-api-secret-here

# Wallet
BASE_WALLET_ADDRESS=0x8475F6aAc937FdA3549431Dc4A72aC067D4E0678

# Mode
DEMO_MODE=false
EOF

echo "[5/5] Setup complete!"
echo ""
echo "=========================================="
echo "  Next Steps:"
echo "=========================================="
echo ""
echo "1. Edit your credentials:"
echo "   nano /opt/polymarket-bot/.env"
echo ""
echo "2. Copy your app files to /opt/polymarket-bot/"
echo "   (app.py, requirements.txt, Dockerfile, docker-compose.yml)"
echo ""
echo "3. Start the bot:"
echo "   cd /opt/polymarket-bot && docker compose up -d"
echo ""
echo "4. Access dashboard at:"
echo "   http://YOUR_DROPLET_IP:5000"
echo ""
echo "=========================================="
