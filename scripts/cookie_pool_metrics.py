#!/usr/bin/env python3
"""
Export Cookie Pool health metrics from Redis.

Primary use: pipe into a Prometheus Node Exporter textfile collector to drive alerts.

Metrics (Prometheus format):
  - cellmapper_cookie_pool_count
  - cellmapper_harvest_success_total
  - cellmapper_turnstile_hits_total

Usage:
  REDIS_URL=redis://localhost:6379/0 python scripts/cookie_pool_metrics.py --prom
"""

from __future__ import annotations

import argparse
import os

import redis

COOKIE_PATTERN = "cellmapper:cookie:*"
TURNSTILE_HITS_KEY = "cellmapper:counters:turnstile_hits"
HARVEST_SUCCESS_KEY = "cellmapper:counters:harvest_success"
COOKIES_CHECKED_OUT_KEY = "cellmapper:counters:cookies_checked_out"
API_REQUESTS_OK_KEY = "cellmapper:counters:api_requests_ok"


def _to_int(v) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except Exception:
        return 0


def _count_keys_scan(r: redis.Redis, pattern: str) -> int:
    # SCAN is safer than KEYS for production Redis.
    count = 0
    for _key in r.scan_iter(match=pattern, count=1000):
        count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Export CellMapper cookie pool metrics from Redis")
    parser.add_argument("--prom", action="store_true", help="Output Prometheus text format")
    args = parser.parse_args()

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    pool_count = _count_keys_scan(r, COOKIE_PATTERN)
    hits_raw, success_raw, checked_out_raw, api_ok_raw = r.mget(
        TURNSTILE_HITS_KEY,
        HARVEST_SUCCESS_KEY,
        COOKIES_CHECKED_OUT_KEY,
        API_REQUESTS_OK_KEY,
    )
    hits = _to_int(hits_raw)
    success = _to_int(success_raw)
    checked_out = _to_int(checked_out_raw)
    api_ok = _to_int(api_ok_raw)
    req_per_cookie = (api_ok / checked_out) if checked_out > 0 else 0.0

    if args.prom:
        print("# HELP cellmapper_cookie_pool_count Number of available cookies in Redis pool (cellmapper:cookie:*).")
        print("# TYPE cellmapper_cookie_pool_count gauge")
        print(f"cellmapper_cookie_pool_count {pool_count}")
        print("# HELP cellmapper_harvest_success_total Total successful harvests recorded by harvester.")
        print("# TYPE cellmapper_harvest_success_total counter")
        print(f"cellmapper_harvest_success_total {success}")
        print("# HELP cellmapper_turnstile_hits_total Total Turnstile/CAPTCHA-type blocks seen by harvester.")
        print("# TYPE cellmapper_turnstile_hits_total counter")
        print(f"cellmapper_turnstile_hits_total {hits}")
        print("# HELP cellmapper_cookies_checked_out_total Total cookies checked out from the Redis pool.")
        print("# TYPE cellmapper_cookies_checked_out_total counter")
        print(f"cellmapper_cookies_checked_out_total {checked_out}")
        print("# HELP cellmapper_api_requests_ok_total Total successful API responses (HTTP 200 and not NEED_RECAPTCHA).")
        print("# TYPE cellmapper_api_requests_ok_total counter")
        print(f"cellmapper_api_requests_ok_total {api_ok}")
        print("# HELP cellmapper_requests_per_cookie Average successful API requests per checked-out cookie.")
        print("# TYPE cellmapper_requests_per_cookie gauge")
        print(f"cellmapper_requests_per_cookie {req_per_cookie:.6f}")
        return 0

    print(f"redis_url={redis_url}")
    print(f"cookie_pool_count={pool_count}")
    print(f"{TURNSTILE_HITS_KEY}={hits}")
    print(f"{HARVEST_SUCCESS_KEY}={success}")
    print(f"{COOKIES_CHECKED_OUT_KEY}={checked_out}")
    print(f"{API_REQUESTS_OK_KEY}={api_ok}")
    print(f"requests_per_cookie={req_per_cookie:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


