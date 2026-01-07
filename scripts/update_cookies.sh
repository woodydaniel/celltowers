#!/bin/bash
# Quick cookie update script
# Usage: ./scripts/update_cookies.sh "JSESSIONID=node0abc123..."

if [ -z "$1" ]; then
    echo "Usage: $0 'JSESSIONID=node0abc123...'"
    echo ""
    echo "Get cookies from browser:"
    echo "1. Visit https://www.cellmapper.net/map"
    echo "2. Open DevTools (F12) → Application → Cookies"
    echo "3. Copy JSESSIONID value"
    echo "4. Run: $0 'JSESSIONID=<value>'"
    exit 1
fi

COOKIE_FILE="config/cookies.txt"

# Backup old cookies
if [ -f "$COOKIE_FILE" ]; then
    cp "$COOKIE_FILE" "$COOKIE_FILE.backup"
    echo "✓ Backed up old cookies to $COOKIE_FILE.backup"
fi

# Write new cookies
echo "$1" > "$COOKIE_FILE"

echo "✓ Updated $COOKIE_FILE"
echo ""
echo "Test with:"
echo "  python main.py --test --carrier tmobile --proxies"







