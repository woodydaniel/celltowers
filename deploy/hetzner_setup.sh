#!/bin/bash
# Hetzner VPS Setup Script for CellMapper Scraper
# Run this on a fresh Ubuntu 22.04+ VPS

set -e

echo "=========================================="
echo "CellMapper Scraper - Hetzner Setup"
echo "=========================================="

# Update system
echo "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install Python 3.11+ and dependencies
echo "Installing Python and dependencies..."
sudo apt install -y python3.11 python3.11-venv python3-pip git screen htop


# Create project directory
PROJECT_DIR="$HOME/cellmapper"
echo "Setting up project in $PROJECT_DIR..."

if [ -d "$PROJECT_DIR" ]; then
    echo "Project directory exists, updating..."
    cd "$PROJECT_DIR"
    git pull || true
else
    echo "Cloning project..."
    # Replace with your repo URL if using git
    mkdir -p "$PROJECT_DIR"
    cd "$PROJECT_DIR"
fi

# Create virtual environment
echo "Creating Python virtual environment..."
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt


# Create data directories
mkdir -p data/towers logs

# Create systemd service (optional)
echo "Creating systemd service..."
sudo tee /etc/systemd/system/cellmapper-scraper.service > /dev/null << EOF
[Unit]
Description=CellMapper Tower Scraper
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$PROJECT_DIR
ExecStart=$PROJECT_DIR/venv/bin/python main.py --format sqlite
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
EOF

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "STEP 1: Get CellMapper session cookies"
echo "----------------------------------------"
echo "  On your LOCAL machine with a browser:"
echo "  1. Go to https://www.cellmapper.net/map"
echo "  2. Select T-Mobile from carrier dropdown"
echo "  3. Pan the map to load towers"
echo "  4. DevTools → Network → getTowers request → Copy Cookie header"
echo ""
echo "  Then save cookies on the server:"
echo "  cd $PROJECT_DIR && source venv/bin/activate"
echo "  python scripts/save_cookies.py 'YOUR_COOKIES_HERE'"
echo ""
echo "STEP 2: Test the scraper"
echo "------------------------"
echo "  python test_run.py  # Should show 6 records"
echo ""
echo "STEP 3: Run full scrape"
echo "-----------------------"
echo "  # With proxies (recommended):"
echo "  python main.py --proxies --format sqlite"
echo ""
echo "  # In background with screen:"
echo "  screen -S cellmapper"
echo "  python main.py --proxies --format sqlite"
echo "  # Press Ctrl+A, D to detach"
echo "  # screen -r cellmapper to reattach"
echo ""
echo "STEP 4: (Optional) Run as a service"
echo "------------------------------------"
echo "  sudo systemctl enable cellmapper-scraper"
echo "  sudo systemctl start cellmapper-scraper"
echo "  sudo journalctl -u cellmapper-scraper -f"
echo ""
echo "If cookies expire, get fresh ones from browser and run save_cookies.py again"
echo ""

