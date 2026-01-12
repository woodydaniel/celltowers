#!/bin/bash
# Quick health check for all regional workers
#
# Usage:
#   ./scripts/check_workers.sh              # Show all worker status
#   ./scripts/check_workers.sh --watch      # Continuous monitoring (5s refresh)
#
# Requires: jq (apt install jq)

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Directory containing metrics files
LOGS_DIR="${LOGS_DIR:-logs}"

check_workers() {
    echo "=== CellMapper Worker Health Check ==="
    echo "Time: $(date)"
    echo ""
    
    local found_any=false
    
    for metrics_file in "$LOGS_DIR"/worker_metrics_*.jsonl; do
        if [ -f "$metrics_file" ]; then
            found_any=true
            worker=$(basename "$metrics_file" .jsonl | sed 's/worker_metrics_//')
            
            # Get last line (most recent snapshot)
            last_line=$(tail -1 "$metrics_file" 2>/dev/null)
            
            if [ -z "$last_line" ]; then
                echo -e "[$worker] ${YELLOW}No metrics data${NC}"
                continue
            fi
            
            # Parse metrics with jq
            velocity=$(echo "$last_line" | jq -r '.velocity_tiles_per_hour // 0')
            stalled=$(echo "$last_line" | jq -r '.is_stalled // false')
            success_rate=$(echo "$last_line" | jq -r '.session_success_rate // 0')
            tiles_total=$(echo "$last_line" | jq -r '.tiles_completed_total // 0')
            towers_total=$(echo "$last_line" | jq -r '.towers_found_total // 0')
            captcha_5m=$(echo "$last_line" | jq -r '.captcha_hits_last_5m // 0')
            time_since=$(echo "$last_line" | jq -r '.time_since_last_success_sec // 0')
            timestamp=$(echo "$last_line" | jq -r '.timestamp // "unknown"')
            bad_proxies=$(echo "$last_line" | jq -r '.bad_proxies // 0')
            
            # Determine status color
            status_color=$GREEN
            status_icon="✓"
            if [ "$stalled" = "true" ]; then
                status_color=$RED
                status_icon="✗"
            elif [ "$captcha_5m" -gt 5 ] 2>/dev/null; then
                status_color=$YELLOW
                status_icon="⚠"
            fi
            
            echo -e "${status_color}[$status_icon $worker]${NC}"
            echo "  Velocity:     $velocity tiles/hr"
            echo "  Success Rate: $success_rate%"
            echo "  Tiles Done:   $tiles_total"
            echo "  Towers Found: $towers_total"
            echo "  CAPTCHAs (5m): $captcha_5m"
            echo "  Bad Proxies:  $bad_proxies"
            echo "  Last Success: ${time_since}s ago"
            echo "  Stalled:      $stalled"
            echo "  Last Update:  $timestamp"
            echo ""
        fi
    done
    
    if [ "$found_any" = false ]; then
        echo -e "${YELLOW}No worker metrics files found in $LOGS_DIR${NC}"
        echo "Workers may not have written their first snapshot yet (every 5 minutes)."
        echo ""
        echo "Check if workers are running:"
        echo "  ps aux | grep 'python main.py'"
    fi
}

# Check for required tools
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed."
    echo "Install with: apt install jq (Debian/Ubuntu) or brew install jq (macOS)"
    exit 1
fi

# Main execution
if [ "$1" = "--watch" ]; then
    while true; do
        clear
        check_workers
        echo "---"
        echo "Refreshing in 5 seconds... (Ctrl+C to exit)"
        sleep 5
    done
else
    check_workers
fi








