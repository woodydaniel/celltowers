#!/usr/bin/env python3
"""
Show harvester Turnstile/CAPTCHA and worker API metrics from Redis.

Keys:
  - cellmapper:counters:turnstile_hits (harvester CAPTCHA blocks)
  - cellmapper:counters:harvest_success (successful cookie mints)
  - cellmapper:counters:harvest_rejected (failed validations)
  - cellmapper:counters:harvest_validated (cookies that passed validation)
  - cellmapper:counters:api_requests_ok (successful worker API calls)
  - cellmapper:counters:api_need_recaptcha (NEED_RECAPTCHA responses)
  - cellmapper:counters:cookies_checked_out (cookies consumed from pool)
  - cellmapper:counters:cookie_reuse_limit (cookies discarded for hitting max reuse)

Usage:
  REDIS_URL=redis://localhost:6379/0 python scripts/show_metrics.py
"""

from __future__ import annotations

import os

import redis

# Harvester metrics
TURNSTILE_HITS_KEY = "cellmapper:counters:turnstile_hits"
HARVEST_SUCCESS_KEY = "cellmapper:counters:harvest_success"
HARVEST_REJECTED_KEY = "cellmapper:counters:harvest_rejected"
HARVEST_VALIDATED_KEY = "cellmapper:counters:harvest_validated"

# Worker API metrics
API_REQUESTS_OK_KEY = "cellmapper:counters:api_requests_ok"
API_NEED_RECAPTCHA_KEY = "cellmapper:counters:api_need_recaptcha"
COOKIES_CHECKED_OUT_KEY = "cellmapper:counters:cookies_checked_out"
COOKIE_REUSE_LIMIT_KEY = "cellmapper:counters:cookie_reuse_limit"


def _to_int(v) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except Exception:
        return 0


def main() -> int:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    # Fetch all counters
    (
        turnstile_raw,
        harvest_success_raw,
        harvest_rejected_raw,
        harvest_validated_raw,
        api_ok_raw,
        api_recaptcha_raw,
        cookies_out_raw,
        cookie_reuse_limit_raw,
    ) = r.mget(
        TURNSTILE_HITS_KEY,
        HARVEST_SUCCESS_KEY,
        HARVEST_REJECTED_KEY,
        HARVEST_VALIDATED_KEY,
        API_REQUESTS_OK_KEY,
        API_NEED_RECAPTCHA_KEY,
        COOKIES_CHECKED_OUT_KEY,
        COOKIE_REUSE_LIMIT_KEY,
    )

    turnstile = _to_int(turnstile_raw)
    harvest_success = _to_int(harvest_success_raw)
    harvest_rejected = _to_int(harvest_rejected_raw)
    harvest_validated = _to_int(harvest_validated_raw)
    api_ok = _to_int(api_ok_raw)
    api_recaptcha = _to_int(api_recaptcha_raw)
    cookies_out = _to_int(cookies_out_raw)
    cookie_reuse_limit = _to_int(cookie_reuse_limit_raw)

    # Calculate rates
    total_harvest = turnstile + harvest_success
    turnstile_pct = (turnstile / total_harvest * 100.0) if total_harvest > 0 else 0.0
    
    validation_total = harvest_validated + harvest_rejected
    rejection_pct = (harvest_rejected / validation_total * 100.0) if validation_total > 0 else 0.0
    
    api_total = api_ok + api_recaptcha
    recaptcha_pct = (api_recaptcha / api_total * 100.0) if api_total > 0 else 0.0

    print("=" * 80)
    print("CELLMAPPER SCRAPER METRICS")
    print("=" * 80)
    print(f"Redis: {redis_url}")
    print()
    
    print("HARVESTER METRICS")
    print("-" * 80)
    print(f"  Turnstile Hits:        {turnstile:>8,} ({turnstile_pct:>5.1f}% of harvest attempts)")
    print(f"  Harvest Success:       {harvest_success:>8,}")
    print(f"  Total Harvest Attempts:{total_harvest:>8,}")
    print()
    print(f"  Validated Cookies:     {harvest_validated:>8,}")
    print(f"  Rejected Cookies:      {harvest_rejected:>8,} ({rejection_pct:>5.1f}% rejection rate)")
    print()
    
    print("WORKER API METRICS")
    print("-" * 80)
    print(f"  Successful Requests:   {api_ok:>8,}")
    print(f"  NEED_RECAPTCHA Hits:   {api_recaptcha:>8,} ({recaptcha_pct:>5.1f}% failure rate)")
    print(f"  Total API Calls:       {api_total:>8,}")
    print()
    
    print("COOKIE POOL METRICS")
    print("-" * 80)
    print(f"  Cookies Checked Out:   {cookies_out:>8,}")
    print(f"  Discarded (reuse cap): {cookie_reuse_limit:>8,}")
    print()
    
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


