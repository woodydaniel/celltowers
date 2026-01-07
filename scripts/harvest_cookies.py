#!/usr/bin/env python3
"""
Cookie Harvester - Continuously mints fresh CellMapper session cookies.

This script runs as a standalone service (or in Docker) and:
1. Mints cookies using a configured engine (default: Playwright w/ stealth).
2. Validates cookies implicitly via CookieManager (real getTowers probe).
3. Pushes validated cookies to Redis with a TTL.
4. Workers pull cookies from Redis; they never spawn browsers themselves.

Environment Variables:
    REDIS_URL: Redis connection URL (default: redis://localhost:6379/0)
    FLARESOLVERR_URL: FlareSolverr endpoint (default: http://localhost:8191)
    CAPTCHA_PROVIDER: "capmonster" for fallback CAPTCHA solving
    CAPTCHA_API_KEY: CapMonster API key
    HARVEST_PROXY: Single proxy URL for cookie minting (optional)
    HARVEST_PROXY_FILE: Path to a proxy list file; harvester will rotate through proxies in file (optional)
    COOKIE_TTL_SECONDS: Cookie TTL in seconds (default: 1500 = 25 min)
    HARVEST_INTERVAL_SECONDS: Time between harvests (default: 600 = 10 min)
    HARVEST_ENGINE: "playwright" | "flaresolverr" (default: playwright)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

# Redis counters (async)
import redis.asyncio as redis

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper.cookie_pool import CookiePool
from scraper.captcha.providers import (
    FlareSolverrProvider,
    get_captcha_provider,
    CaptchaSolveError,
)
from scraper.cookie_manager import CookieManager, PersistentHarvester
from scraper.notifier import get_notifier

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("harvester")

# Configuration
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://localhost:8191")
HARVEST_PROXY = os.environ.get("HARVEST_PROXY", "").strip() or None
HARVEST_PROXY_FILE = os.environ.get("HARVEST_PROXY_FILE", "").strip() or None
COOKIE_TTL_SECONDS = int(os.environ.get("COOKIE_TTL_SECONDS", "1500"))
HARVEST_INTERVAL_SECONDS = int(os.environ.get("HARVEST_INTERVAL_SECONDS", "600"))
TARGET_POOL_SIZE = int(os.environ.get("TARGET_POOL_SIZE", "5"))
HARVEST_ENGINE = os.environ.get("HARVEST_ENGINE", "playwright").strip().lower()
CHECK_EVERY_SEC = float(os.environ.get("CHECK_EVERY_SEC", "20") or "20")
# Small pause between individual harvest attempts when pool is below target.
BACKOFF_SEC = float(os.environ.get("BACKOFF_SEC", "2") or "2")

# Experiment flag: use persistent browser context (Test B)
USE_PERSISTENT_HARVESTER = os.environ.get("USE_PERSISTENT_HARVESTER", "false").lower() == "true"
# How many cookies to mint before rotating context (anti-fingerprinting)
PERSISTENT_MINTS_PER_CONTEXT = int(os.environ.get("PERSISTENT_MINTS_PER_CONTEXT", "10") or "10")

# Metrics counters (Redis keys)
TURNSTILE_HITS_KEY = "cellmapper:counters:turnstile_hits"
HARVEST_SUCCESS_KEY = "cellmapper:counters:harvest_success"
HARVEST_REJECTED_KEY = "cellmapper:counters:harvest_rejected"
HARVEST_VALIDATED_KEY = "cellmapper:counters:harvest_validated"
HARVEST_BYTES_TOTAL_KEY = "cellmapper:counters:harvest_bytes_total"

# Worker -> Harvester stall gating (bandwidth failsafe)
WORKERS_LAST_SUCCESS_TS_KEY = os.environ.get(
    "WORKERS_LAST_SUCCESS_TS_KEY", "cellmapper:workers:last_success_ts"
)
WORKERS_LAST_SNAPSHOT_TS_KEY = os.environ.get(
    "WORKERS_LAST_SNAPSHOT_TS_KEY", "cellmapper:workers:last_snapshot_ts"
)
HARVESTER_WORKER_STALL_THRESHOLD_SEC = int(
    os.environ.get("HARVESTER_WORKER_STALL_THRESHOLD_SEC", "1800") or "1800"
)
HARVESTER_WORKER_HEARTBEAT_STALE_SEC = int(
    os.environ.get("HARVESTER_WORKER_HEARTBEAT_STALE_SEC", "900") or "900"
)
HARVESTER_STARTUP_GRACE_SEC = int(
    os.environ.get("HARVESTER_STARTUP_GRACE_SEC", "600") or "600"
)

# Bandwidth guardrail:
# The /map route loads a large SPA bundle and map assets. For cookie minting we prefer the
# lightweight homepage unless explicitly overridden.
CELLMAPPER_URL = os.environ.get("HARVEST_URL", "https://www.cellmapper.net/")

def _load_proxy_list() -> list[str]:
    proxies: list[str] = []
    if HARVEST_PROXY_FILE:
        try:
            with open(HARVEST_PROXY_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    proxies.append(line)
        except Exception as e:
            logger.warning(f"Failed to read HARVEST_PROXY_FILE={HARVEST_PROXY_FILE}: {e}")
    if not proxies and HARVEST_PROXY:
        proxies.append(HARVEST_PROXY)
    return proxies

def _looks_like_turnstile_or_captcha(message: str) -> bool:
    """
    Heuristic detection of Turnstile/CAPTCHA in FlareSolverr failures.

    This is intentionally broad; the goal is to measure how often harvesting
    gets blocked by a human-verification step vs. transient network issues.
    """
    m = (message or "").lower()
    keywords = [
        "turnstile",
        "cf-turnstile",
        "challenges.cloudflare.com",
        "captcha",
        "verify you are human",
        "human verification",
        "attention required",
    ]
    return any(k in m for k in keywords)

async def _incr_counter(r: redis.Redis, key: str, amount: int = 1) -> None:
    try:
        await r.incrby(key, amount)
    except Exception as e:
        logger.debug(f"Failed to increment counter {key}: {e}")


async def _track_harvester_bytes(r: redis.Redis, estimated_bytes: int, day_key: str = None) -> None:
    """
    Track harvester bandwidth usage in Redis.
    
    Since Playwright doesn't easily expose exact bytes, we estimate based on:
    - Cold start (first page load): ~2MB
    - Warm (cached assets, persistent context): ~200KB
    
    The actual values can be refined by monitoring network traffic.
    """
    if day_key is None:
        from datetime import datetime
        day_key = datetime.now().strftime("%Y-%m-%d")
    
    try:
        # Total counter
        await r.incrby(HARVEST_BYTES_TOTAL_KEY, estimated_bytes)
        # Daily breakdown
        await r.hincrby(f"harvester:bytes:{day_key}", "total", estimated_bytes)
    except Exception as e:
        logger.debug(f"Failed to track harvester bytes: {e}")


async def _get_int(r: redis.Redis, key: str) -> int | None:
    """Best-effort Redis GET int parser."""
    try:
        v = await r.get(key)
    except Exception:
        return None
    if v is None:
        return None
    try:
        return int(float(v))
    except Exception:
        return None


async def _get_pause_reason(counters: redis.Redis, started_at: float, pool_size: int = 0) -> str | None:
    """
    Return a non-empty string reason if harvester should pause minting; otherwise None.

    Smart failsafe logic:
    1. If last_success_ts is recent → workers ARE making progress → keep minting
    2. If last_success_ts is old AND pool >= 5 → workers have cookies but aren't using them → pause
    3. If last_success_ts is old AND pool < 5 → workers might just be waiting for cookies → 
       use extended grace period before pausing (to avoid deadlock)
    
    This prevents the deadlock where:
    - Workers wait for cookies → can't make progress → heartbeats go stale
    - Harvester sees stale heartbeats → pauses minting → pool never fills → deadlock!
    """
    now = time.time()
    
    # Avoid pausing immediately during cold start when workers may not have written anything yet.
    if (now - started_at) < HARVESTER_STARTUP_GRACE_SEC:
        return None

    last_snapshot_ts = await _get_int(counters, WORKERS_LAST_SNAPSHOT_TS_KEY)
    last_success_ts = await _get_int(counters, WORKERS_LAST_SUCCESS_TS_KEY)
    
    # PRIMARY CHECK: If workers have had a recent successful tile completion, they're healthy.
    # Keep minting regardless of heartbeat/pool status - workers just need more cookies.
    if last_success_ts is not None and (now - last_success_ts) <= HARVESTER_WORKER_STALL_THRESHOLD_SEC:
        return None  # Workers are healthy, keep minting
    
    # At this point, last_success_ts is either None or old (> threshold).
    # Now we need to distinguish: are workers waiting for cookies, or truly broken?
    
    # If pool is low, workers might just be waiting for cookies. Use extended grace period.
    # Only pause if workers haven't progressed for 2x the normal threshold.
    extended_threshold = HARVESTER_WORKER_STALL_THRESHOLD_SEC * 2
    
    if pool_size < 5:
        # Pool is low - workers might be waiting for cookies
        if last_success_ts is None:
            # No success ever recorded - still in startup or truly broken
            # Give extra time before declaring failure
            return None  # Keep trying - pool needs filling
        
        # Workers had success before, but not recently. How long ago?
        success_age = int(now - last_success_ts)
        if success_age <= extended_threshold:
            # Within extended grace period - keep minting, workers might recover once pool fills
            return None
        else:
            # Extended period exceeded - something is truly wrong
            return (
                f"worker_progress_stalled_extended pool_size={pool_size} "
                f"last_success_age={success_age}s (extended_threshold={extended_threshold}s)"
            )
    
    # Pool is NOT low (>= 5) - workers have cookies available but aren't progressing.
    # This is a real stall - pause minting to save bandwidth.
    
    if last_snapshot_ts is None:
        return (
            f"no_worker_heartbeat_found (missing {WORKERS_LAST_SNAPSHOT_TS_KEY}); "
            f"pool_size={pool_size} (cookies available but no worker activity)"
        )

    if (now - last_snapshot_ts) > HARVESTER_WORKER_HEARTBEAT_STALE_SEC:
        return (
            f"worker_heartbeat_stale last_snapshot_age={int(now - last_snapshot_ts)}s "
            f"pool_size={pool_size} (stale_after={HARVESTER_WORKER_HEARTBEAT_STALE_SEC}s)"
        )

    if last_success_ts is not None:
        return (
            f"worker_progress_stalled last_success_age={int(now - last_success_ts)}s "
            f"pool_size={pool_size} (stall_after={HARVESTER_WORKER_STALL_THRESHOLD_SEC}s)"
        )

    return None


async def harvest_one_cookie_persistent(
    persistent_harvester: PersistentHarvester,
    pool: CookiePool,
    counters: redis.Redis,
    proxy_url: str | None = None,
) -> tuple[bool, str]:
    """
    Harvest one cookie using persistent Playwright context.
    
    This reuses the browser and context across mints for bandwidth efficiency.
    
    Returns (success, reason).
    reason is one of: "success" | "rejected" | "error"
    """
    logger.info(f"Harvesting cookie (persistent, proxy={proxy_url or 'none'})")
    
    try:
        cookie_string, user_agent = await persistent_harvester.mint_cookie(proxy_url=proxy_url)
        
        if not cookie_string:
            logger.warning("Persistent harvester returned no cookies")
            return False, "error"
        
        # Parse cookie string into dict
        cookies = {}
        for item in cookie_string.split(";"):
            item = item.strip()
            if "=" in item:
                key, value = item.split("=", 1)
                cookies[key.strip()] = value.strip()
        
        if not cookies:
            logger.warning("Failed to parse cookies from persistent harvester")
            return False, "error"
        
        # Validate cookies via API before storing
        from scraper.cookie_manager import CookieManager
        temp_manager = CookieManager(cookie_engine="playwright", use_redis_pool=False)
        if not await temp_manager._validate_cookies_via_api(cookie_string, proxy_url=proxy_url):
            await _incr_counter(counters, HARVEST_REJECTED_KEY, 1)
            logger.warning("Persistent harvester cookies rejected by API")
            return False, "rejected"
        
        # Store in Redis
        key = await pool.put(cookies=cookies, user_agent=user_agent or "", proxy_url=proxy_url, validated=True)
        if not key:
            await _incr_counter(counters, HARVEST_REJECTED_KEY, 1)
            logger.warning("Pool refused cookie insert (not stored)")
            return False, "rejected"
        
        await _incr_counter(counters, HARVEST_VALIDATED_KEY, 1)
        await _incr_counter(counters, HARVEST_SUCCESS_KEY, 1)
        
        # Estimate bandwidth - persistent harvester uses cached resources (~200KB per mint)
        # First mint is larger (~1MB), but we amortize over the context lifetime
        estimated_bytes = 200 * 1024  # 200KB estimate for cached navigation
        await _track_harvester_bytes(counters, estimated_bytes)
        
        logger.info(f"Successfully harvested cookie (persistent): {key} ({len(cookies)} cookies)")
        return True, "success"
        
    except Exception as e:
        logger.error(f"Unexpected error during persistent harvest: {e}")
        return False, "error"


async def harvest_one_cookie(
    flaresolverr: FlareSolverrProvider,
    cookie_manager: CookieManager,
    pool: CookiePool,
    counters: redis.Redis,
    proxy_url: str | None = None,
) -> tuple[bool, str]:
    """
    Attempt to harvest one cookie using FlareSolverr.
    
    Returns (success, reason).
    reason is one of: "success" | "rejected" | "error"
    """
    engine = HARVEST_ENGINE
    logger.info(f"Harvesting cookie (engine={engine}, proxy={proxy_url or 'none'})")
    
    try:
        cookie_string = ""
        user_agent = ""

        if engine == "playwright":
            # Use real Chromium (with stealth patches inside CookieManager).
            cookie_string = await cookie_manager.refresh_cookies(proxy_url=proxy_url)
            user_agent = cookie_manager.get_user_agent() or os.environ.get(
                "PLAYWRIGHT_UA",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            )
        else:
            # Legacy / fallback: FlareSolverr visit to obtain cookies. Note: may be rejected by API.
            cookie_string = await flaresolverr.get_cookies(CELLMAPPER_URL, proxy_url=proxy_url)
            user_agent = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            )
        
        if not cookie_string:
            logger.warning("Cookie mint returned no cookies")
            return False, "error"
        
        # Parse cookie string into dict
        cookies = {}
        for item in cookie_string.split(";"):
            item = item.strip()
            if "=" in item:
                key, value = item.split("=", 1)
                cookies[key.strip()] = value.strip()
        
        if not cookies:
            logger.warning("Failed to parse cookies from FlareSolverr response")
            return False, "error"
        
        # Store in Redis
        key = await pool.put(cookies=cookies, user_agent=user_agent, proxy_url=proxy_url, validated=True)
        if not key:
            await _incr_counter(counters, HARVEST_REJECTED_KEY, 1)
            logger.warning("Pool refused cookie insert (not stored)")
            return False, "rejected"
        await _incr_counter(counters, HARVEST_VALIDATED_KEY, 1)
        await _incr_counter(counters, HARVEST_SUCCESS_KEY, 1)
        
        # Estimate bandwidth - standard harvester loads full page each time (~2MB)
        estimated_bytes = 2 * 1024 * 1024  # 2MB estimate for cold page load
        await _track_harvester_bytes(counters, estimated_bytes)
        
        logger.info(f"Successfully harvested cookie: {key} ({len(cookies)} cookies)")
        return True, "success"
        
    except CaptchaSolveError as e:
        logger.warning(f"FlareSolverr challenge failed: {e}")

        # Record Turnstile/CAPTCHA-type harvest blocks for ROI analysis
        if _looks_like_turnstile_or_captcha(str(e)):
            await _incr_counter(counters, TURNSTILE_HITS_KEY, 1)
        
        # Try fallback CAPTCHA provider if available
        captcha_provider = get_captcha_provider()
        if captcha_provider and hasattr(captcha_provider, "solve"):
            logger.info("Attempting fallback CAPTCHA solve via configured provider")
            try:
                # For Turnstile, we need the site key - this would need to be extracted
                # from the page. For now, log that we'd need manual intervention.
                logger.warning(
                    "Fallback CAPTCHA solving requires site key extraction. "
                    "Consider using CapMonster with browser automation."
                )
            except Exception as fallback_error:
                logger.error(f"Fallback CAPTCHA solve failed: {fallback_error}")
        
        return False, "error"
        
    except Exception as e:
        logger.error(f"Unexpected error during harvest: {e}")
        return False, "error"


async def maintain_pool(pool: CookiePool, flaresolverr: FlareSolverrProvider, counters: redis.Redis) -> None:
    """
    Continuously maintain the cookie pool at target size.
    """
    mode = "persistent" if USE_PERSISTENT_HARVESTER else "standard"
    logger.info(
        f"Starting cookie pool maintenance (mode={mode}, target={TARGET_POOL_SIZE}, "
        f"check_every={CHECK_EVERY_SEC}s, backoff={BACKOFF_SEC}s, TTL={COOKIE_TTL_SECONDS}s)"
    )
    
    consecutive_failures = 0
    max_consecutive_failures = 5
    consecutive_rejections = 0
    proxy_list = _load_proxy_list()
    proxy_index = 0
    cookie_manager = CookieManager(cookie_engine="playwright", use_redis_pool=False)
    notifier = get_notifier()
    started_at = time.time()
    paused_for_stall = False
    
    # Initialize persistent harvester if enabled
    persistent_harvester: PersistentHarvester | None = None
    if USE_PERSISTENT_HARVESTER:
        persistent_harvester = PersistentHarvester(
            max_mints_per_context=PERSISTENT_MINTS_PER_CONTEXT,
            harvest_url=CELLMAPPER_URL,
        )
        logger.info(
            f"Using PersistentHarvester (mints_per_context={PERSISTENT_MINTS_PER_CONTEXT})"
        )
    
    while True:
        try:
            current_size = await pool.count()
            logger.info(f"Cookie pool status: {current_size}/{TARGET_POOL_SIZE}")

            # Drip-feed: harvest ONE cookie whenever below target, then re-check immediately.
            if current_size < TARGET_POOL_SIZE:
                pause_reason = await _get_pause_reason(counters, started_at, pool_size=current_size)
                if pause_reason:
                    if not paused_for_stall:
                        paused_for_stall = True
                        logger.warning(
                            f"🛑 Harvester pausing minting (workers stalled): {pause_reason}"
                        )
                        notifier.send_harvester_paused(
                            reason="WORKERS_STALLED",
                            details=pause_reason,
                        )
                    await asyncio.sleep(CHECK_EVERY_SEC)
                    continue
                elif paused_for_stall:
                    paused_for_stall = False
                    logger.info("Workers recovered; resuming harvester minting")

                proxy_url = None
                if proxy_list:
                    proxy_url = proxy_list[proxy_index % len(proxy_list)]
                    proxy_index += 1

                # Use persistent harvester if enabled, otherwise standard
                if persistent_harvester:
                    success, reason = await harvest_one_cookie_persistent(
                        persistent_harvester, pool, counters, proxy_url=proxy_url
                    )
                else:
                    success, reason = await harvest_one_cookie(
                        flaresolverr, cookie_manager, pool, counters, proxy_url=proxy_url
                    )

                if success:
                    consecutive_failures = 0
                    consecutive_rejections = 0
                    await asyncio.sleep(BACKOFF_SEC)
                    continue

                if reason == "rejected":
                    consecutive_rejections += 1
                    consecutive_failures = 0  # keep transport failures separate

                    # Exponential backoff capped at 30 minutes to protect proxy bandwidth.
                    # Base 60s, doubling each time: 60, 120, 240, ... up to 1800.
                    backoff = min(1800, 60 * (2 ** min(consecutive_rejections - 1, 6)))
                    logger.warning(
                        f"Cookie validation rejected (streak={consecutive_rejections}); "
                        f"backing off {backoff}s to avoid proxy bandwidth burn"
                    )
                    await asyncio.sleep(backoff)
                    continue

                # Transport / unexpected failure
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(
                        f"Too many consecutive failures ({consecutive_failures}), "
                        "backing off for 5 minutes"
                    )
                    await asyncio.sleep(300)
                    consecutive_failures = 0
                else:
                    await asyncio.sleep(max(10.0, BACKOFF_SEC))
                continue

            # Pool at/above target: short nap then re-check.
            await asyncio.sleep(CHECK_EVERY_SEC)

        except asyncio.CancelledError:
            logger.info("Harvester shutting down")
            break
        except Exception as e:
            logger.error(f"Error in maintenance loop: {e}")
            await asyncio.sleep(60)  # Back off on error
    
    # Cleanup persistent harvester
    if persistent_harvester:
        await persistent_harvester.close()


async def main() -> None:
    """Main entry point for the harvester."""
    logger.info("=" * 60)
    logger.info("CellMapper Cookie Harvester Starting")
    logger.info("=" * 60)
    logger.info(f"Redis URL: {REDIS_URL}")
    logger.info(f"FlareSolverr URL: {FLARESOLVERR_URL}")
    logger.info(f"Harvest Proxy: {HARVEST_PROXY or 'none'}")
    logger.info(f"Harvest Proxy File: {HARVEST_PROXY_FILE or 'none'}")
    logger.info(f"Cookie TTL: {COOKIE_TTL_SECONDS}s")
    logger.info(f"Harvest Interval: {HARVEST_INTERVAL_SECONDS}s")
    logger.info(f"Target Pool Size: {TARGET_POOL_SIZE}")
    logger.info(f"Persistent Harvester: {USE_PERSISTENT_HARVESTER}")
    if USE_PERSISTENT_HARVESTER:
        logger.info(f"  Mints per context: {PERSISTENT_MINTS_PER_CONTEXT}")
    logger.info("=" * 60)
    
    # Initialize components
    pool = CookiePool(redis_url=REDIS_URL, ttl_seconds=COOKIE_TTL_SECONDS)
    flaresolverr = FlareSolverrProvider(url=FLARESOLVERR_URL)
    counters = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    
    try:
        # Connect to Redis
        await pool.connect()
        await counters.ping()
        
        # Run maintenance loop
        await maintain_pool(pool, flaresolverr, counters)
        
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down")
    finally:
        await pool.close()
        await flaresolverr.close()
        try:
            await counters.aclose()
        except Exception:
            pass
        logger.info("Harvester stopped")


if __name__ == "__main__":
    asyncio.run(main())

