#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${REDIS_URL:-}" ]]; then
  echo "[worker] REDIS_URL is not set; cannot wait for cookie pool"
  exit 2
fi

REQUIRED_COOKIES="${REQUIRED_COOKIES:-1}"
if ! [[ "$REQUIRED_COOKIES" =~ ^[0-9]+$ ]]; then
  echo "[worker] REQUIRED_COOKIES must be an integer; got: $REQUIRED_COOKIES"
  exit 2
fi

echo "[worker] waiting for cookie pool >= ${REQUIRED_COOKIES}…"

# Block until at least one cookie key exists.
# We use SCAN (not KEYS) to keep Redis load low.
until [[ "$(redis-cli -u "$REDIS_URL" --scan --pattern 'cellmapper:cookie:*' | wc -l | tr -d ' ')" -ge "$REQUIRED_COOKIES" ]]; do
  sleep 3
done

echo "[worker] cookie found → starting: $*"
exec "$@"
