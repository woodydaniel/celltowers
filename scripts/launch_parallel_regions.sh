#!/bin/bash
# =============================================================================
# Parallel Carrier + Region Launcher (West/East) - Hardening v2
# =============================================================================
# Launches 2 workers per carrier (west/east) with:
# - Geographic bounds partitioning (no duplicate work)
# - Isolated progress/output/log files via --run-tag
# - Split proxy pools per region to avoid sessid contention
# - Staggered starts to reduce bot signals
#
# Usage:
#   ./scripts/launch_parallel_regions.sh
#   ./scripts/launch_parallel_regions.sh tmobile att
#
# Environment:
#   REQUESTS_PER_SESSION=2
# =============================================================================

set -e

cd "$(dirname "$0")/.."  # project root

if [ -f "venv/bin/activate" ]; then
  source venv/bin/activate
fi

REQUESTS_PER_SESSION="${REQUESTS_PER_SESSION:-3}"
STAGGER_SEC="${STAGGER_SEC:-300}"   # 5 min default to reduce concurrent Playwright cookie minting

# US bounds (from config/settings.py)
US_NORTH="49.384358"
US_SOUTH="24.396308"
US_WEST="-124.848974"
US_EAST="-66.934570"

# Split longitude (roughly mid-US). Must be consistent to avoid gaps/overlap.
SPLIT_LON="-96.0"

WEST_NORTH="$US_NORTH"
WEST_SOUTH="$US_SOUTH"
WEST_WEST="$US_WEST"
WEST_EAST="$SPLIT_LON"

EAST_NORTH="$US_NORTH"
EAST_SOUTH="$US_SOUTH"
EAST_WEST="$SPLIT_LON"
EAST_EAST="$US_EAST"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')]${NC} $1"; }
error() { echo -e "${RED}[$(date '+%H:%M:%S')]${NC} $1"; }

proxy_file_for() {
  local carrier="$1"
  local region="$2"
  echo "config/proxies_${carrier}_${region}.txt"
}

launch_worker() {
  local carrier="$1"
  local region="$2"        # west|east
  local proxy_file
  proxy_file="$(proxy_file_for "$carrier" "$region")"

  if [ ! -f "$proxy_file" ]; then
    error "Missing proxy file: $proxy_file"
    return 1
  fi

  local north west south east
  if [ "$region" = "west" ]; then
    north="$WEST_NORTH"; west="$WEST_WEST"; south="$WEST_SOUTH"; east="$WEST_EAST"
  else
    north="$EAST_NORTH"; west="$EAST_WEST"; south="$EAST_SOUTH"; east="$EAST_EAST"
  fi

  mkdir -p logs

  log "Starting ${carrier}_${region} (bounds N=${north} W=${west} S=${south} E=${east})..."

  nohup python main.py \
    --carrier "$carrier" \
    --bounds "$north" "$west" "$south" "$east" \
    --run-tag "$region" \
    --proxies \
    --proxy-file "$proxy_file" \
    --auto-refresh \
    --cookie-engine auto \
    --randomize \
    >> "logs/${carrier}_${region}.log" 2>&1 &

  local pid=$!
  sleep 3
  if kill -0 "$pid" 2>/dev/null; then
    log "${carrier}_${region} started with PID: ${pid}"
    echo "$pid" > "logs/${carrier}_${region}.pid"
  else
    error "${carrier}_${region} failed to start! Check logs/${carrier}_${region}.log"
    return 1
  fi
}

main() {
  log "=== CellMapper Regional Launcher (West/East) - Hardening v2 ==="
  log "Requests per session: ${REQUESTS_PER_SESSION}"
  log "Split longitude: ${SPLIT_LON}"
  log "Stagger seconds: ${STAGGER_SEC}"

  local carriers=("$@")
  if [ ${#carriers[@]} -eq 0 ]; then
    carriers=("tmobile" "att" "verizon")
  fi

  # For each carrier: start west, wait 2 min, start east; then move to next carrier.
  for carrier in "${carriers[@]}"; do
    launch_worker "$carrier" "west" || true
    log "Waiting ${STAGGER_SEC}s before launching ${carrier}_east..."
    sleep "${STAGGER_SEC}"
    launch_worker "$carrier" "east" || true
    log "Waiting ${STAGGER_SEC}s before next carrier..."
    sleep "${STAGGER_SEC}"
  done

  log "=== All regional workers launched ==="
  log "Monitor:"
  log "  tail -f logs/*_west.log logs/*_east.log"
  log "Check running:"
  log "  ps aux | grep 'python main.py' | grep -v grep"
  log "Stop all:"
  log "  pkill -f 'python main.py'"
}

main "$@"


