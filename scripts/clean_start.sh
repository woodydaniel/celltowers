#!/usr/bin/env bash
set -euo pipefail

# Clean-start helper:
# - archives logs (and a few run artifacts)
# - resets local progress/tower outputs
# - clears Redis counters + cookie pool + proxy bandwidth keys (when possible)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TS="$(date +"%Y-%m-%d_%H-%M-%S")"
ARCHIVE_DIR="logs/archive/${TS}"

mkdir -p "$ARCHIVE_DIR"

echo "==> Archiving logs/artifacts to: ${ARCHIVE_DIR}"

archive_if_exists() {
  local src="$1"
  local dst="$2"
  if [[ -e "$src" ]]; then
    mkdir -p "$(dirname "$dst")"
    cp -a "$src" "$dst"
  fi
}

# Logs
archive_if_exists "logs/scraper.log" "${ARCHIVE_DIR}/scraper.log"

# Run artifacts (handy when debugging regressions)
archive_if_exists "data/scrape_stats.json" "${ARCHIVE_DIR}/data/scrape_stats.json"
archive_if_exists "data/progress.json" "${ARCHIVE_DIR}/data/progress.json"
archive_if_exists "data/test_results.json" "${ARCHIVE_DIR}/data/test_results.json"

echo "==> Resetting local progress + outputs"

# Progress files (carrier-specific too)
rm -f data/progress*.json || true

# Stats snapshot
rm -f data/scrape_stats.json || true

# Tower outputs
rm -f data/towers/* || true

echo "==> Resetting Redis counters/cookies/bandwidth (if available)"

docker_has_redis=false
if command -v docker >/dev/null 2>&1; then
  if docker compose ps -q redis >/dev/null 2>&1; then
    if [[ -n "$(docker compose ps -q redis 2>/dev/null)" ]]; then
      docker_has_redis=true
    fi
  fi
fi

run_redis_cli() {
  # Usage: run_redis_cli <redis-cli args...>
  if [[ "$docker_has_redis" == "true" ]]; then
    docker compose exec -T redis redis-cli "$@"
  elif command -v redis-cli >/dev/null 2>&1; then
    # Fall back to local redis-cli if present. Respect REDIS_URL if set.
    local redis_url="${REDIS_URL:-redis://localhost:6379/0}"
    redis-cli -u "$redis_url" "$@"
  else
    return 127
  fi
}

delete_pattern() {
  local pattern="$1"
  local count
  count="$(run_redis_cli --scan --pattern "$pattern" 2>/dev/null | wc -l | tr -d ' ' || true)"
  if [[ "$count" == "0" ]]; then
    echo "  - ${pattern}: 0 keys"
    return 0
  fi
  echo "  - ${pattern}: deleting ${count} keys"
  # Use xargs -r (GNU) if available; otherwise emulate.
  if run_redis_cli --scan --pattern "$pattern" 2>/dev/null | xargs -r -n 200 run_redis_cli DEL >/dev/null 2>&1; then
    return 0
  fi
  # macOS xargs doesn't support -r; re-run safely
  run_redis_cli --scan --pattern "$pattern" 2>/dev/null | while read -r key; do
    [[ -z "$key" ]] && continue
    run_redis_cli DEL "$key" >/dev/null
  done
}

if run_redis_cli PING >/dev/null 2>&1; then
  delete_pattern "cellmapper:counters:*"
  delete_pattern "cellmapper:cookie:*"
  delete_pattern "proxy:bytes:*"
  echo "==> Redis reset complete."
else
  echo "==> Skipped Redis reset (no reachable redis-cli)."
  echo "    If using Docker, start services first: docker compose up -d redis"
  echo "    Or install redis-cli locally and set REDIS_URL."
fi

echo "==> Done."
echo "Next suggested start:"
echo "  docker compose up -d --scale harvester=3"


