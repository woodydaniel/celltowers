#!/bin/bash
# Sync cookies from local machine to Hetzner server
# Usage: ./scripts/sync_cookies.sh user@your-hetzner-ip

set -e

REMOTE="${1:-}"
COOKIE_FILE="config/cookies.txt"
REMOTE_PATH="~/cellmapper/config/cookies.txt"

if [ -z "$REMOTE" ]; then
    echo "Usage: $0 user@server-ip"
    echo "Example: $0 root@123.45.67.89"
    exit 1
fi

if [ ! -f "$COOKIE_FILE" ]; then
    echo "ERROR: $COOKIE_FILE not found"
    echo "Run: python scripts/save_cookies.py 'your-cookies-here'"
    exit 1
fi

echo "Syncing cookies to $REMOTE..."
scp "$COOKIE_FILE" "${REMOTE}:${REMOTE_PATH}"
echo "✓ Cookies synced to $REMOTE"

# Optionally restart the scraper
read -p "Restart scraper on server? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    ssh "$REMOTE" "cd ~/cellmapper && pkill -f 'python main.py' || true; screen -dmS cellmapper bash -c 'source venv/bin/activate && python main.py --proxies --resume'"
    echo "✓ Scraper restarted"
fi











