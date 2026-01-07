"""
Cookie Pool - Redis-backed shared cookie storage for workers.

The harvester process mints cookies via FlareSolverr + CapMonster and pushes them here.
Workers pull fresh cookies on startup and whenever their current cookie is "poisoned"
(i.e., receives NEED_RECAPTCHA).

Keys in Redis:
  - cellmapper:cookie:<timestamp> -> JSON {cookies: dict, user_agent: str}
  - Each key has a TTL (default 25 min) to auto-expire stale sessions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Optional
import hashlib

import redis.asyncio as redis

from config.settings import COOKIE_TTL_SECONDS, MAX_COOKIE_REUSE, REDIS_URL

logger = logging.getLogger(__name__)

COOKIE_KEY_PREFIX = "cellmapper:cookie:"
COOKIES_CHECKED_OUT_KEY = "cellmapper:counters:cookies_checked_out"
COOKIE_REUSE_LIMIT_KEY = "cellmapper:counters:cookie_reuse_limit"


@dataclass
class PooledCookie:
    """A cookie entry from the pool."""
    cookies: dict[str, str]
    user_agent: str
    proxy_url: Optional[str]
    key: str  # Redis key for deletion
    use_count: int = 0  # number of successful uses (incremented on put_back)
    created_at: Optional[float] = None  # original mint time (do not reset on reuse)


class CookiePool:
    """
    Async Redis-backed cookie pool.
    
    Usage:
        pool = CookiePool()
        await pool.connect()
        
        # Harvester:
        await pool.put(cookies={"JSESSIONID": "..."}, user_agent="Mozilla/5.0 ...")
        
        # Worker:
        cookie = await pool.get()
        if cookie:
            # use cookie.cookies and cookie.user_agent
            pass
        
        # On CAPTCHA hit:
        await pool.delete(cookie.key)
    """
    
    def __init__(self, redis_url: Optional[str] = None, ttl_seconds: Optional[int] = None):
        self.redis_url = redis_url or REDIS_URL
        self.ttl_seconds = ttl_seconds or COOKIE_TTL_SECONDS
        self._client: Optional[redis.Redis] = None
    
    async def connect(self) -> None:
        """Connect to Redis."""
        if self._client is None:
            self._client = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            try:
                await self._client.ping()
                logger.info(f"CookiePool connected to Redis at {self.redis_url}")
            except Exception as e:
                logger.error(f"CookiePool failed to connect to Redis: {e}")
                self._client = None
                raise
    
    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    def _proxy_id(self, proxy_url: Optional[str]) -> str:
        if not proxy_url:
            return "direct"
        return hashlib.sha1(proxy_url.encode("utf-8")).hexdigest()[:12]

    async def put(
        self,
        cookies: dict[str, str],
        user_agent: str,
        proxy_url: Optional[str] = None,
        *,
        validated: bool = False,
    ) -> str:
        """
        Store a new cookie in the pool with TTL.
        
        Returns the Redis key.
        """
        if not validated:
            # Safety guardrail: do not allow unknown/unchecked cookies into the shared pool,
            # otherwise one bad harvest run poisons all workers immediately.
            logger.warning("CookiePool.put called with validated=False; refusing to store cookie")
            return ""
        if not self._client:
            await self.connect()
        
        proxy_id = self._proxy_id(proxy_url)
        key = f"{COOKIE_KEY_PREFIX}{proxy_id}:{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        value = json.dumps({
            "cookies": cookies,
            "user_agent": user_agent,
            "proxy_url": proxy_url,
            "created_at": time.time(),
            "use_count": 0,
        })
        
        await self._client.setex(key, self.ttl_seconds, value)
        logger.info(f"CookiePool: stored cookie {key} (TTL={self.ttl_seconds}s)")
        return key
    
    async def get(self, proxy_url: Optional[str] = None) -> Optional[PooledCookie]:
        """
        Get a random cookie from the pool.

        IMPORTANT: This operation is *consuming* (one-time use) to avoid multiple workers
        sharing the same cookie concurrently and rapidly poisoning the shared pool.
        We atomically remove the cookie from Redis when returning it.
        
        Returns None if pool is empty.
        """
        if not self._client:
            await self.connect()
        
        # Get all cookie keys
        if proxy_url:
            proxy_id = self._proxy_id(proxy_url)
            pattern = f"{COOKIE_KEY_PREFIX}{proxy_id}:*"
        else:
            pattern = f"{COOKIE_KEY_PREFIX}*"

        keys = await self._client.keys(pattern)
        if not keys:
            logger.debug("CookiePool: no cookies available")
            return None
        
        # Pick a random key to spread load
        key = random.choice(keys)
        # Atomically consume the cookie so only one worker uses it.
        # Redis 6.2+ supports GETDEL.
        try:
            value = await self._client.getdel(key)  # type: ignore[attr-defined]
        except Exception:
            # Fallback for clients without GETDEL
            value = await self._client.get(key)
            if value:
                await self._client.delete(key)
        
        if not value:
            # Key expired (or was consumed) between keys() and get/getdel
            return await self.get()  # Try again

        # Best-effort metrics
        try:
            await self._client.incr(COOKIES_CHECKED_OUT_KEY)
        except Exception:
            pass
        
        try:
            data = json.loads(value)
            return PooledCookie(
                cookies=data["cookies"],
                user_agent=data["user_agent"],
                proxy_url=data.get("proxy_url"),
                # Key was already consumed; keep for observability only.
                key=key,
                use_count=int(data.get("use_count", 0) or 0),
                created_at=data.get("created_at"),
            )
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"CookiePool: invalid cookie data at {key}: {e}")
            return await self.get()
    
    async def delete(self, key: str) -> bool:
        """
        Delete a poisoned cookie from the pool.
        
        Call this when NEED_RECAPTCHA is received with this cookie.
        """
        if not self._client:
            await self.connect()
        
        deleted = await self._client.delete(key)
        if deleted:
            logger.info(f"CookiePool: deleted poisoned cookie {key}")
        return bool(deleted)

    async def put_back(self, cookie: PooledCookie, ttl_seconds: Optional[int] = None) -> bool:
        """
        Return a previously checked-out cookie back into the pool.

        IMPORTANT:
        - Cookies are checked out via GETDEL (exclusive use).
        - Only call put_back() when the caller is *done* using this cookie.
        - Do NOT put back cookies that triggered NEED_RECAPTCHA; delete/discard instead.

        Uses SET with NX to avoid overwriting if the key already exists.
        Returns True if the cookie was re-inserted, False otherwise.
        """
        if not self._client:
            await self.connect()

        ttl = int(ttl_seconds or self.ttl_seconds)
        # Cookie reuse is intentionally capped to avoid 15-20x reuse patterns that
        # trigger NEED_RECAPTCHA while minimizing additional harvester bandwidth.
        max_reuse = int(MAX_COOKIE_REUSE or 0)
        new_use_count = int(getattr(cookie, "use_count", 0) or 0) + 1
        if max_reuse > 0 and new_use_count >= max_reuse:
            # Do not return this cookie to the pool; discard it.
            try:
                await self._client.incr(COOKIE_REUSE_LIMIT_KEY)
            except Exception:
                pass
            logger.info(
                f"CookiePool: discard cookie at reuse limit {cookie.key} "
                f"(use_count={new_use_count}, max={max_reuse})"
            )
            return False

        value = json.dumps(
            {
                "cookies": cookie.cookies,
                "user_agent": cookie.user_agent,
                "proxy_url": cookie.proxy_url,
                # Preserve original mint time so logs like `cookie_age_s` stay meaningful.
                "created_at": cookie.created_at or time.time(),
                "last_used_at": time.time(),
                "reused": True,
                "use_count": new_use_count,
            }
        )

        # Re-insert under the same key for observability. NX ensures we never overwrite.
        try:
            ok = await self._client.set(cookie.key, value, ex=ttl, nx=True)
        except Exception as e:
            logger.warning(f"CookiePool: failed to put_back {cookie.key}: {e}")
            return False

        if ok:
            logger.info(f"CookiePool: returned cookie {cookie.key} (TTL={ttl}s)")
            return True

        # If the key already exists (unexpected), fall back to storing under a fresh key.
        try:
            proxy_id = self._proxy_id(cookie.proxy_url)
            new_key = f"{COOKIE_KEY_PREFIX}{proxy_id}:{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
            await self._client.setex(new_key, ttl, value)
            logger.info(f"CookiePool: returned cookie under new key {new_key} (TTL={ttl}s)")
            return True
        except Exception as e:
            logger.warning(f"CookiePool: failed to put_back fallback for {cookie.key}: {e}")
            return False
    
    async def count(self) -> int:
        """Get the number of cookies in the pool."""
        if not self._client:
            await self.connect()
        
        keys = await self._client.keys(f"{COOKIE_KEY_PREFIX}*")
        return len(keys)
    
    async def clear(self) -> int:
        """Clear all cookies from the pool. Returns count deleted."""
        if not self._client:
            await self.connect()
        
        keys = await self._client.keys(f"{COOKIE_KEY_PREFIX}*")
        if keys:
            deleted = await self._client.delete(*keys)
            logger.info(f"CookiePool: cleared {deleted} cookies")
            return deleted
        return 0


# Convenience: module-level singleton for simple usage
_pool: Optional[CookiePool] = None


async def get_pool() -> CookiePool:
    """Get or create the module-level CookiePool singleton."""
    global _pool
    if _pool is None:
        _pool = CookiePool()
        await _pool.connect()
    return _pool

