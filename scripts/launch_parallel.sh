#!/bin/bash
# =============================================================================
# Parallel Carrier Scraper Launcher (Hardening v2)
# =============================================================================
# Launches all 3 carrier scrapers with:
# - Staggered start times (2 min apart) to avoid detection
# - Carrier-specific proxy files for session isolation
# - Carrier-specific cookie files to prevent conflicts
# - Carrier-specific log files for easier debugging
# - Hardening v2: Low requests-per-session, defer queue for stubborn tiles
#
# Usage:
#   ./scripts/launch_parallel.sh              # Launch all 3 carriers
#   ./scripts/launch_parallel.sh tmobile      # Launch single carrier
#   ./scripts/launch_parallel.sh att verizon  # Launch specific carriers
#
# Environment variables (optional):
#   REQUESTS_PER_SESSION=2   # Override default requests per session
# =============================================================================

set -e

cd "$(dirname "$0")/.."  # Navigate to project root

# Activate virtual environment if it exists
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

# Hardening v2: Default to 2 requests per session (one tile per sessid)
REQUESTS_PER_SESSION="${REQUESTS_PER_SESSION:-2}"

# Session-pool runs MUST mint cookies through the same proxy sticky session used for API calls.
# For hardened runs we force Playwright cookie engine and do not export FlareSolverr env vars here.

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%H:%M:%S')]${NC} $1"
}

# Check if carrier-specific proxy files exist
check_proxy_files() {
    local missing=0
    for carrier in tmobile att verizon; do
        if [ ! -f "config/proxies_${carrier}.txt" ]; then
            warn "Missing: config/proxies_${carrier}.txt"
            missing=1
        fi
    done
    if [ $missing -eq 1 ]; then
        warn "Some carrier proxy files missing. Will fall back to shared proxies.txt"
    fi
}

# Launch a single carrier scraper (Hardening v2)
launch_carrier() {
    local carrier=$1
    local proxy_file="config/proxies_${carrier}.txt"
    
    # Validate carrier-specific proxy file exists
    if [ ! -f "$proxy_file" ]; then
        error "Missing proxy file: $proxy_file"
        warn "Falling back to shared proxies.txt (not recommended for parallel runs)"
        proxy_file="config/proxies.txt"
    fi
    
    log "Starting ${carrier} scraper (requests-per-session=${REQUESTS_PER_SESSION})..."
    
    # Hardening v2: Use low requests-per-session, session pool with playwright cookies
    nohup python main.py \
        --carrier "$carrier" \
        --session-pool \
        --proxies \
        --proxy-file "$proxy_file" \
        --auto-refresh \
        --cookie-engine playwright \
        --requests-per-session "$REQUESTS_PER_SESSION" \
        --randomize \
        >> "logs/${carrier}.log" 2>&1 &
    
    local pid=$!
    
    # Verify process started successfully (give Python a moment to initialize)
    sleep 3
    if kill -0 "$pid" 2>/dev/null; then
        log "${carrier} started with PID: ${pid}"
        echo "$pid" > "logs/${carrier}.pid"
    else
        error "${carrier} failed to start! Check logs/${carrier}.log"
        return 1
    fi
}

# Main execution (Hardening v2)
main() {
    log "=== CellMapper Parallel Scraper Launcher (Hardening v2) ==="
    log "Requests per session: ${REQUESTS_PER_SESSION}"
    
    # Check dependencies
    check_proxy_files
    
    # Create logs directory
    mkdir -p logs
    
    # Determine which carriers to launch
    local carriers=("$@")
    if [ ${#carriers[@]} -eq 0 ]; then
        carriers=("tmobile" "att" "verizon")
    fi
    
    # If only one carrier, launch without stagger
    if [ ${#carriers[@]} -eq 1 ]; then
        launch_carrier "${carriers[0]}"
        log "Single carrier launched. Monitor with: tail -f logs/${carriers[0]}.log"
        exit 0
    fi
    
    # Launch multiple carriers with staggered timing
    log "Launching ${#carriers[@]} carriers with 2-minute stagger..."
    
    local first=true
    for carrier in "${carriers[@]}"; do
        if [ "$first" = true ]; then
            first=false
        else
            log "Waiting 2 minutes before launching ${carrier}..."
            sleep 120
        fi
        
        launch_carrier "$carrier"
        if [ $? -ne 0 ]; then
            error "Failed to launch ${carrier}, continuing with remaining carriers..."
        fi
    done
    
    log "=== All carriers launched ==="
    log ""
    log "Hardening v2 features enabled:"
    log "  - Per-tile retry budget with defer queue"
    log "  - Proxy CAPTCHA cooling (bad proxies cooled for hours)"
    log "  - Low requests-per-session (${REQUESTS_PER_SESSION})"
    log ""
    log "Monitor progress:"
    log "  tail -f logs/tmobile.log logs/att.log logs/verizon.log"
    log ""
    log "Check if running:"
    log "  ps aux | grep 'main.py'"
    log ""
    log "Stop all:"
    log "  pkill -f 'python main.py'"
}

main "$@"

