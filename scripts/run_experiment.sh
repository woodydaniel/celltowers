#!/usr/bin/env bash
#
# Experiment Runner - Systematic CAPTCHA/Bandwidth Testing
#
# Each test runs for 20 minutes with automatic metrics snapshots.
# Output is automatically saved to experiment_results/<test>_<timestamp>.txt
#
# Usage:
#   ./scripts/run_experiment.sh test-a    # Run Test A (20 min)
#   ./scripts/run_experiment.sh test-b    # Run Test B (20 min)
#   ./scripts/run_experiment.sh test-c    # Run Test C (20 min)
#   ./scripts/run_experiment.sh test-d    # Run Test D (workers on Hetzner direct)
#   ./scripts/run_experiment.sh test-e    # Run Test E (persistent harvester + workers direct)
#   ./scripts/run_experiment.sh baseline-b # Run Phase 2 baseline (Baseline B) (20 min)
#   ./scripts/run_experiment.sh test-b1   # Run Phase 2 b1 (20 min)
#   ./scripts/run_experiment.sh test-b2   # Run Phase 2 b2 (20 min)
#   ./scripts/run_experiment.sh test-b3   # Run Phase 2 b3 (20 min)
#   ./scripts/run_experiment.sh test-b4   # Run Phase 2 b4 (20 min)
#   ./scripts/run_experiment.sh test-b5   # Run Phase 2 b5 (20 min)
#   ./scripts/run_experiment.sh reset     # Reset between tests
#   ./scripts/run_experiment.sh status    # Show current status
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Activate venv if it exists (for Hetzner server)
if [ -f "${ROOT_DIR}/venv/bin/activate" ]; then
    source "${ROOT_DIR}/venv/bin/activate"
fi

# Duration of each test in seconds (20 minutes)
TEST_DURATION=${TEST_DURATION:-1200}

# Results output directory
RESULTS_DIR="${ROOT_DIR}/experiment_results"
mkdir -p "$RESULTS_DIR"

# Check if running on Hetzner or locally
check_environment() {
    if command -v docker &> /dev/null; then
        echo "Docker is available"
    else
        echo "ERROR: Docker is not available"
        exit 1
    fi
    
    # Check Redis connection
    if docker compose exec -T redis redis-cli PING &> /dev/null; then
        echo "Redis is reachable"
    else
        echo "WARNING: Redis not reachable. Start services first."
    fi
}

# Take a metrics snapshot
snapshot() {
    local label="$1"
    echo "Taking snapshot: $label"
    python3 scripts/snapshot_metrics.py --label "$label"
}

# Show current metrics
show_status() {
    echo ""
    echo "=== Current Pool Status ==="
    docker compose exec -T redis redis-cli KEYS "cellmapper:cookie:*" 2>/dev/null | wc -l | xargs echo "Cookie pool size:"
    
    echo ""
    echo "=== Recent Counters ==="
    docker compose exec -T redis redis-cli GET "cellmapper:counters:api_requests_ok" 2>/dev/null | xargs echo "API OK:"
    docker compose exec -T redis redis-cli GET "cellmapper:counters:api_need_recaptcha" 2>/dev/null | xargs echo "CAPTCHA hits:"
    docker compose exec -T redis redis-cli GET "cellmapper:counters:harvest_success" 2>/dev/null | xargs echo "Harvest success:"
    
    echo ""
    echo "=== Stored Snapshots ==="
    python3 scripts/snapshot_metrics.py --show-all 2>/dev/null || echo "(none)"
}

# Reset between tests
reset_state() {
    echo "=== Resetting State ==="
    
    # Stop all services except Redis
    echo "Stopping services..."
    docker compose down
    
    # Flush Redis
    echo "Flushing Redis..."
    docker compose up -d redis
    sleep 3
    docker compose exec -T redis redis-cli FLUSHALL
    
    # Run clean start
    echo "Running clean_start.sh..."
    ./scripts/clean_start.sh
    
    echo "=== Reset Complete ==="
}

# Run a specific test (internal, called by run_test_with_logging)
_run_test_inner() {
    local test_name="$1"
    local compose_override=""
    
    case "$test_name" in
        baseline)
            compose_override=""
            echo "=== Running BASELINE: Current production config (all via Decodo proxies) ==="
            ;;
        baseline-b)
            compose_override=""
            echo "=== Running BASELINE-B: PersistentHarvester baseline (Phase 2) ==="
            ;;
        test-a)
            compose_override="docker-compose.test-a.yml"
            echo "=== Running Test A: Harvester on Hetzner Direct IP ==="
            ;;
        test-b)
            compose_override="docker-compose.test-b.yml"
            echo "=== Running Test B: Persistent Playwright Profile ==="
            ;;
        test-b1)
            compose_override="docker-compose.test-b1.yml"
            echo "=== Running Test B1: Mints per context = 25 ==="
            ;;
        test-b2)
            compose_override="docker-compose.test-b2.yml"
            echo "=== Running Test B2: Mints per context = 50 ==="
            ;;
        test-b3)
            compose_override="docker-compose.test-b3.yml"
            echo "=== Running Test B3: MAX_COOKIE_REUSE = 20 ==="
            ;;
        test-b4)
            compose_override="docker-compose.test-b4.yml"
            echo "=== Running Test B4: Webshare proxies (rotating) ==="
            ;;
        test-b5)
            compose_override="docker-compose.test-b5.yml"
            echo "=== Running Test B5: Best combination ==="
            ;;
        test-c)
            compose_override="docker-compose.test-c.yml"
            echo "=== Running Test C: Combined (A + B) ==="
            ;;
        test-d)
            compose_override="docker-compose.test-d.yml"
            echo "=== Running Test D: Workers on Hetzner Direct IP (Harvester unchanged) ==="
            ;;
        test-e)
            compose_override="docker-compose.test-e.yml"
            echo "=== Running Test E: Persistent Harvester + Workers on Hetzner Direct IP ==="
            ;;
        *)
            echo "Unknown test: $test_name"
            echo "Valid tests: baseline, baseline-b, test-a, test-b, test-b1, test-b2, test-b3, test-b4, test-b5, test-c, test-d, test-e"
            exit 1
            ;;
    esac
    
    echo ""
    echo "Test duration: $((TEST_DURATION / 60)) minutes"
    echo "Started at: $(date)"
    echo ""
    
    # Take start snapshot
    snapshot "${test_name}_start"
    
    # Start services with override (or base config for baseline)
    echo ""
    if [ -n "$compose_override" ]; then
        echo "Starting services with $compose_override..."
        docker compose -f docker-compose.yml -f "$compose_override" up -d
    else
        echo "Starting services with base config (no overrides)..."
        docker compose up -d --scale harvester=3
    fi
    
    echo ""
    echo "Test running for $((TEST_DURATION / 60)) minutes..."
    echo "Monitor with: docker compose logs -f harvester"
    echo ""
    
    # Wait for test duration with progress updates
    local elapsed=0
    local interval=300  # 5 minute updates
    
    while [ $elapsed -lt $TEST_DURATION ]; do
        sleep $interval
        elapsed=$((elapsed + interval))
        echo ""
        echo "=== $((elapsed / 60)) minutes elapsed ($(date)) ==="
        show_status
    done
    
    # Take end snapshot
    echo ""
    echo "Test complete at: $(date)"
    snapshot "${test_name}_end"
    
    # Compare snapshots
    echo ""
    echo "============================================================"
    echo "FINAL RESULTS: $test_name"
    echo "============================================================"
    python3 scripts/snapshot_metrics.py --compare "${test_name}_start" "${test_name}_end"
    
    echo ""
    echo "=== Test $test_name Complete ==="
    echo ""
    echo "Results saved to snapshots: ${test_name}_start, ${test_name}_end"
    echo "View comparison anytime with:"
    echo "  python3 scripts/snapshot_metrics.py --compare ${test_name}_start ${test_name}_end"
}

# Run a test with automatic logging to file
run_test() {
    local test_name="$1"
    local timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
    local log_file="${RESULTS_DIR}/${test_name}_${timestamp}.txt"
    
    echo "============================================================"
    echo "Starting $test_name"
    echo "Output will be saved to: $log_file"
    echo "============================================================"
    
    # Run the test and tee output to both screen and file
    _run_test_inner "$test_name" 2>&1 | tee "$log_file"
    
    echo ""
    echo "============================================================"
    echo "Results saved to: $log_file"
    echo "============================================================"
}

# Main
case "${1:-}" in
    baseline|baseline-b|test-a|test-b|test-b1|test-b2|test-b3|test-b4|test-b5|test-c|test-d|test-e)
        check_environment
        run_test "$1"
        ;;
    reset)
        check_environment
        reset_state
        ;;
    status)
        check_environment
        show_status
        ;;
    snapshot)
        if [ -z "${2:-}" ]; then
            echo "Usage: $0 snapshot <label>"
            exit 1
        fi
        snapshot "$2"
        ;;
    compare)
        if [ -z "${2:-}" ] || [ -z "${3:-}" ]; then
            echo "Usage: $0 compare <before_label> <after_label>"
            exit 1
        fi
        python3 scripts/snapshot_metrics.py --compare "$2" "$3"
        ;;
    *)
        echo "CellMapper Experiment Runner"
        echo ""
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  baseline  Run BASELINE: Current production config (20 min)"
        echo "  baseline-b Run BASELINE-B: PersistentHarvester baseline (Phase 2) (20 min)"
        echo "  test-a    Run Test A: Harvester on Hetzner Direct IP (20 min)"
        echo "  test-b    Run Test B: Persistent Playwright Profile (20 min)"
        echo "  test-b1   Run Test B1: Mints per context = 25 (20 min)"
        echo "  test-b2   Run Test B2: Mints per context = 50 (20 min)"
        echo "  test-b3   Run Test B3: MAX_COOKIE_REUSE = 20 (20 min)"
        echo "  test-b4   Run Test B4: Webshare proxies (rotating) (20 min)"
        echo "  test-b5   Run Test B5: Best combination (20 min)"
        echo "  test-c    Run Test C: Combined A + B (20 min)"
        echo "  test-d    Run Test D: Workers on Hetzner Direct IP (20 min)"
        echo "  test-e    Run Test E: Persistent Harvester + Workers Direct (20 min)"
        echo "  reset     Reset state between tests (flush Redis, clean start)"
        echo "  status    Show current pool and counter status"
        echo "  snapshot <label>  Take a metrics snapshot"
        echo "  compare <before> <after>  Compare two snapshots"
        echo ""
        echo "Results are automatically saved to: experiment_results/<test>_<timestamp>.txt"
        echo ""
        echo "Typical workflow:"
        echo "  1. $0 reset"
        echo "  2. $0 test-d    # Your hypothesis - workers direct"
        echo "  3. $0 reset"
        echo "  4. $0 test-a    # If D fails, try harvester direct"
        echo "  5. $0 reset"
        echo "  6. $0 test-e    # If D works, try combined (best case)"
        echo ""
        exit 1
        ;;
esac

