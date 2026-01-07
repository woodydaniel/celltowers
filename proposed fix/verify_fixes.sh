#!/bin/bash
# Verification Script for AtlasGrid Scraper Fixes
# Run this to confirm all fixes are in place before starting the 100K scrape

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  AtlasGrid Scraper: Fix Verification                          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

ISSUES_FOUND=0

# ============================================================================
# Check 1: HARVEST_URL in cookie_manager.py
# ============================================================================
echo "▶ Checking cookie_manager.py for hardcoded URLs..."

if grep -n "https://www.atlasgrid.net/map" cookie_manager.py > /dev/null; then
    echo "  ❌ FAIL: Found hardcoded /map URL in cookie_manager.py"
    echo "     Run: grep -n 'https://www.atlasgrid.net/map' cookie_manager.py"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
else
    echo "  ✅ PASS: No hardcoded /map URLs found"
fi

# ============================================================================
# Check 2: HARVEST_URL uses env variable or config
# ============================================================================
echo ""
echo "▶ Checking if HARVEST_URL is configurable..."

if grep -n "os.getenv.*HARVEST_URL\|getenv.*HARVEST_URL" cookie_manager.py > /dev/null; then
    echo "  ✅ PASS: HARVEST_URL uses os.getenv()"
else
    echo "  ⚠️  WARNING: Couldn't confirm HARVEST_URL is using getenv()"
    echo "     Manually verify cookie_manager.py line ~576"
fi

# ============================================================================
# Check 3: docker-compose.yml MAX_COOKIE_REUSE default
# ============================================================================
echo ""
echo "▶ Checking docker-compose.yml for MAX_COOKIE_REUSE default..."

# Look for the old bad default
if grep "MAX_COOKIE_REUSE.*:-3" docker-compose.yml > /dev/null; then
    echo "  ❌ FAIL: Found old default 'MAX_COOKIE_REUSE:-3' in docker-compose.yml"
    echo "     Should be: MAX_COOKIE_REUSE:-10"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
else
    echo "  ✅ PASS: No old 'MAX_COOKIE_REUSE:-3' found"
fi

# Check for the new good default
if grep "MAX_COOKIE_REUSE.*:-10" docker-compose.yml > /dev/null; then
    echo "  ✅ PASS: Found correct default 'MAX_COOKIE_REUSE:-10'"
else
    echo "  ⚠️  WARNING: Couldn't confirm MAX_COOKIE_REUSE:-10 in docker-compose.yml"
    echo "     Manually verify all service definitions have this default"
fi

# ============================================================================
# Check 4: .env file overrides
# ============================================================================
echo ""
echo "▶ Checking for conflicting .env overrides..."

if [ -f .env ]; then
    if grep -E "^HARVEST_URL=.*\/map|^MAX_COOKIE_REUSE=.*3" .env > /dev/null; then
        echo "  ❌ FAIL: Found problematic values in .env:"
        grep -E "^HARVEST_URL=|^MAX_COOKIE_REUSE=" .env || true
        echo "     Consider removing these from .env to use compose defaults"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    else
        echo "  ✅ PASS: .env looks OK (no problematic overrides)"
    fi
else
    echo "  ℹ️  INFO: No .env file found (OK, will use docker-compose.yml defaults)"
fi

# ============================================================================
# Check 5: Verify we can build docker images
# ============================================================================
echo ""
echo "▶ Checking Docker build status..."

if docker compose config > /dev/null 2>&1; then
    echo "  ✅ PASS: docker-compose.yml is syntactically valid"
else
    echo "  ❌ FAIL: docker-compose.yml has syntax errors"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
fi

# ============================================================================
# Check 6: Redis configuration
# ============================================================================
echo ""
echo "▶ Checking Redis configuration..."

if docker compose config | grep -A 5 "redis:" > /dev/null; then
    echo "  ✅ PASS: Redis service is defined"
else
    echo "  ⚠️  WARNING: Couldn't verify Redis service definition"
fi

# ============================================================================
# Check 7: All worker services defined
# ============================================================================
echo ""
echo "▶ Checking worker services..."

WORKERS=("tmobile_east" "tmobile_west" "att_east" "att_west" "verizon_east" "verizon_west")
MISSING_WORKERS=0

for worker in "${WORKERS[@]}"; do
    if docker compose config | grep -q "\"${worker}\""; then
        echo "  ✅ ${worker}"
    else
        echo "  ❌ ${worker} (MISSING)"
        MISSING_WORKERS=$((MISSING_WORKERS + 1))
    fi
done

if [ $MISSING_WORKERS -gt 0 ]; then
    ISSUES_FOUND=$((ISSUES_FOUND + $MISSING_WORKERS))
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"

if [ $ISSUES_FOUND -eq 0 ]; then
    echo "║  ✅ ALL CHECKS PASSED - Ready to scrape!                    ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Next steps:"
    echo "  1. Set credentials in .env (PROXY_USERNAME, PROXY_PASSWORD, etc.)"
    echo "  2. Start services: docker compose up -d"
    echo "  3. Monitor: docker compose logs -f"
    echo "  4. Once pool is full, run your scrape job"
    echo ""
    exit 0
else
    echo "║  ❌ $ISSUES_FOUND ISSUE(S) FOUND - Fix before proceeding      ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    exit 1
fi
