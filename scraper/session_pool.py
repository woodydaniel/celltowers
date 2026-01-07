"""
Session Pool Manager

Manages multiple proxy+session combinations for sustainable scraping.
Each proxy gets its own fresh session via Playwright, and we rotate
through them proactively to avoid triggering rate limits.

Strategy:
- Each proxy maintains its own JSESSIONID cookie
- Rotate every N requests (proactive, not reactive)
- Refresh session automatically when needed
- Track request counts per session for intelligent rotation
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Optional

from .proxy_manager import ProxyManager, Proxy
from .cookie_manager import CookieManager
from config.settings import CONFIG_DIR

logger = logging.getLogger(__name__)


@dataclass
class ProxySession:
    """A proxy with its associated session cookies."""
    
    proxy: Proxy
    cookies: dict = field(default_factory=dict)
    request_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_used: float = 0.0
    last_refresh: float = 0.0
    is_healthy: bool = True
    consecutive_failures: int = 0
    transport_failures: int = 0
    cool_until: float = 0.0
    needs_preflight: bool = False
    
    @property
    def needs_refresh(self) -> bool:
        """Check if session needs refresh based on failures or age."""
        # Refresh if too many consecutive failures
        if self.consecutive_failures >= 2:
            return True
        # Rotate away on repeated transport failures (don't permanently kill the session)
        if self.transport_failures >= 2:
            return True
        # Refresh if session is old (30 minutes)
        if time.time() - self.last_refresh > 1800:
            return True
        return False
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.success_count + self.failure_count
        return self.success_count / total * 100 if total > 0 else 100.0
    
    def __str__(self) -> str:
        return f"Session({self.proxy}, reqs={self.request_count}, ok={self.success_rate:.0f}%)"


class SessionPool:
    """
    Manages a pool of proxy+session combinations.
    (proxy-specific cookie caching added)
    """
    """
    Manages a pool of proxy+session combinations.
    
    Key features:
    - Proactive rotation every N requests
    - Automatic session refresh via Playwright
    - Health tracking per proxy
    - Cooldown between uses of same proxy
    """
    
    def __init__(
        self,
        proxy_manager: ProxyManager,
        requests_per_session: int = 2,  # Hardening v2: default lowered to 2 (one tile per sessid)
        cooldown_seconds: float = 30.0,  # Min time between reusing same proxy
        max_consecutive_failures: int = 3,
        carrier: str = None,  # Carrier for isolated cookies
        cookie_engine: Literal["auto", "playwright", "flaresolverr"] = "auto",
    ):
        """Initialize SessionPool.

        Args:
            proxy_manager: Proxy manager providing Proxy objects.
            requests_per_session: Rotate session after N successful requests.
            cooldown_seconds: Cool-down before reusing same proxy.
            max_consecutive_failures: Mark session unhealthy after this many failures.
            carrier: Optional carrier slug, used to namespace per-proxy cookie files.
        """
        self.proxy_manager = proxy_manager
        self.requests_per_session = requests_per_session
        self.cooldown_seconds = cooldown_seconds
        self.max_consecutive_failures = max_consecutive_failures
        self.carrier = carrier  # For carrier-specific cookie isolation
        self.cookie_engine: Literal["auto", "playwright", "flaresolverr"] = cookie_engine
        
        # Session tracking
        self.sessions: dict[str, ProxySession] = {}
        self.current_session: Optional[ProxySession] = None
        self._cookie_manager: Optional[CookieManager] = None
        
        # Statistics
        self.total_rotations = 0
        self.total_refreshes = 0
        self.captcha_triggers = 0  # Hardening v2: Track captcha events
        
        # Watchdog: Track time spent in all-proxies-cooling state
        self._cool_wait_secs = 0.0
        
        if carrier:
            logger.info(f"SessionPool initialized for carrier: {carrier}")
    
    async def initialize(self) -> None:
        """Initialize the session pool by creating sessions for all proxies."""
        logger.info(f"Initializing session pool with {self.proxy_manager.total_count} proxies")
        
        for proxy in self.proxy_manager.proxies:
            self.sessions[str(proxy)] = ProxySession(proxy=proxy)
        
        # Get first session ready
        if self.sessions:
            first_session = list(self.sessions.values())[0]
            await self._refresh_session(first_session)
            self.current_session = first_session
            logger.info(f"Session pool initialized with {len(self.sessions)} proxies")
    
    async def _get_cookie_manager(self) -> CookieManager:
        """Get or create the cookie manager (carrier-specific if configured)."""
        if self._cookie_manager is None:
            from .cookie_manager import get_cookie_manager
            self._cookie_manager = get_cookie_manager(carrier=self.carrier)
        return self._cookie_manager
    
    def _proxy_cookie_file(self, session: ProxySession) -> Path:
        """
        Return cookie file path for this proxy session.

        IMPORTANT: For Decodo sticky sessions, host/port are shared (gate.decodo.com:7000),
        so we key cookies by carrier + sessid (or a short non-sensitive username hash).
        """
        import re

        carrier_part = self.carrier or "shared"
        username = session.proxy.username or ""

        sessid = None
        if username:
            m = re.search(r"sessid-([a-z0-9]+)", username)
            if m:
                sessid = m.group(1)

        if sessid:
            key_part = sessid
        elif username:
            key_part = "u_" + hashlib.sha256(username.encode("utf-8")).hexdigest()[:8]
        else:
            key_part = "host"

        safe_host = session.proxy.host.replace(":", "_").replace("/", "_")
        filename = f"cookies_{carrier_part}_{key_part}_{safe_host}_{session.proxy.port}.txt"
        return CONFIG_DIR / filename

    async def _refresh_session(self, session: ProxySession, force_refresh: bool = False) -> bool:
        """
        Refresh cookies for a specific proxy session.
        
        If FlareSolverr is configured, uses that (best success rate).
        Otherwise tries to load existing cookies from file (fast).
        Falls back to Playwright if no cookies exist or force_refresh=True.
        """
        logger.info(f"Refreshing session for proxy {session.proxy}...")
        
        try:
            # Create a dedicated CookieManager per proxy with isolated cookie file
            proxy_cookie_file = self._proxy_cookie_file(session)
            cookie_manager = CookieManager(cookies_file=proxy_cookie_file, cookie_engine=self.cookie_engine)

            # Try to reuse cookies unless force_refresh or cookie file is too old (>30 min)
            cookie_age_ok = (
                proxy_cookie_file.exists()
                and (time.time() - os.path.getmtime(proxy_cookie_file)) < 1800  # 30 minutes
            )
            if not force_refresh and cookie_age_ok:
                existing_cookies = cookie_manager.load_cookies()
                if existing_cookies:
                    logger.info(f"✓ Loaded cached cookies for {session.proxy}")
                    session.cookies = self._parse_cookies(existing_cookies)
                    session.last_refresh = time.time()
                    session.consecutive_failures = 0
                    session.is_healthy = True
                    # Even cached cookies can go stale / drift from proxy IP; validate before heavy use.
                    session.needs_preflight = True
                    return True

            # Otherwise fetch fresh cookies through proxy
            logger.info(f"Fetching fresh cookies for {session.proxy} via CookieManager…")
            cookies = await cookie_manager.refresh_cookies(proxy_url=session.proxy.url)

            if cookies:
                session.cookies = self._parse_cookies(cookies)
                session.last_refresh = time.time()
                session.consecutive_failures = 0
                session.is_healthy = True
                session.transport_failures = 0
                session.cool_until = 0.0
                session.needs_preflight = True
                self.total_refreshes += 1
                # Persist proxy-specific cookies for future reuse
                cookie_manager.save_cookies(cookies)
                logger.info(f"✓ Session refreshed and cookies stored for {session.proxy}")
                return True
            else:
                logger.warning(f"✗ No cookies returned for {session.proxy}")
                session.is_healthy = False
                return False
                
        except Exception as e:
            logger.error(f"Failed to refresh session for {session.proxy}: {e}")
            session.is_healthy = False
            return False
    
    def _parse_cookies(self, cookie_string: str) -> dict:
        """Parse cookie string into dict."""
        if not cookie_string:
            return {}
        
        cookies = {}
        for item in cookie_string.split(";"):
            item = item.strip()
            if "=" in item:
                key, value = item.split("=", 1)
                cookies[key.strip()] = value.strip()
        return cookies
    
    async def get_session(self) -> Optional[ProxySession]:
        """
        Get the best available session, rotating as needed.
        
        Returns a session that:
        - Has valid cookies
        - Hasn't been used too many times
        - Has had enough cooldown time
        - Proxy is not cooling from CAPTCHA
        """
        if not self.sessions:
            return None
        
        # Check if current session needs rotation
        if self.current_session:
            # Also check if current proxy is cooling
            current_proxy_cooling = self.current_session.proxy.is_cooling
            should_rotate = (
                self.current_session.request_count >= self.requests_per_session or
                self.current_session.needs_refresh or
                not self.current_session.is_healthy or
                current_proxy_cooling
            )
            
            if not should_rotate:
                return self.current_session
        
        # Find best available session
        now = time.time()
        candidates = []
        
        for session in self.sessions.values():
            # Skip unhealthy sessions
            if not session.is_healthy and session.consecutive_failures >= self.max_consecutive_failures:
                continue
            
            # Skip sessions whose PROXY is cooling (critical fix!)
            if session.proxy.is_cooling:
                continue
            
            # Check session-level cooldown
            if session.cool_until and now < session.cool_until:
                continue
            
            # Check usage cooldown (skip for current session to allow continuation)
            time_since_used = now - session.last_used
            if time_since_used < self.cooldown_seconds and session != self.current_session:
                continue
            
            candidates.append(session)
        
        # EMERGENCY FALLBACK: All proxies cooling - wait for shortest cooldown
        if not candidates:
            wait_time = self.proxy_manager.get_shortest_cooldown_wait()
            if wait_time > 0:
                # Cap wait at 60 seconds per iteration to stay responsive
                actual_wait = min(wait_time, 60.0)
                
                # Watchdog: Track cumulative wait time
                self._cool_wait_secs += actual_wait
                if self._cool_wait_secs > 1800:  # 30 minutes
                    logger.error(
                        f"🛑 Cooling watchdog tripped! Spent {self._cool_wait_secs:.0f}s "
                        f"waiting for proxies. Exiting to save resources."
                    )
                    import sys
                    sys.exit(0)
                
                logger.warning(
                    f"⏳ All proxies cooling. Waiting {actual_wait:.0f}s "
                    f"(next available in {wait_time:.0f}s, total wait: {self._cool_wait_secs:.0f}s)..."
                )
                await asyncio.sleep(actual_wait)
                # Retry after waiting
                return await self.get_session()
        
        if not candidates:
            # Still no candidates after wait - try to find any healthy one
            candidates = [
                s for s in self.sessions.values() 
                if s.is_healthy and not s.proxy.is_cooling
            ]
        
        if not candidates:
            # No healthy non-cooling sessions, try to refresh one that's not cooling
            logger.warning("No healthy sessions available, attempting refresh...")
            for session in self.sessions.values():
                if not session.proxy.is_cooling:
                    if await self._refresh_session(session):
                        candidates.append(session)
                        break
        
        if not candidates:
            logger.error("All sessions exhausted and refresh failed")
            return None
        
        # Pick session with lowest request count (least used)
        candidates.sort(key=lambda s: (s.request_count, s.consecutive_failures))
        new_session = candidates[0]
        
        # Always force fresh cookies when rotating to a new proxy
        # CellMapper ties JSESSIONID to IP, so we need new cookies per proxy
        if new_session != self.current_session:
            old_id = self._extract_proxy_id(self.current_session.proxy) if self.current_session else "none"
            new_id = self._extract_proxy_id(new_session.proxy)
            logger.info(f"🔄 ROTATING [{old_id}] → [{new_id}] - fetching fresh cookies...")
            if not await self._refresh_session(new_session, force_refresh=True):
                # Try next candidate if refresh failed
                if len(candidates) > 1:
                    new_session = candidates[1]
                    new_id = self._extract_proxy_id(new_session.proxy)
                    logger.info(f"Fallback to [{new_id}]")
                    await self._refresh_session(new_session, force_refresh=True)
            new_session.request_count = 0
            self.total_rotations += 1
            logger.info(f"✓ Now using [{new_id}] (rotation #{self.total_rotations})")
        elif new_session.needs_refresh or not new_session.cookies:
            # Same session but needs refresh - FORCE new cookies to break CAPTCHA loop
            if not await self._refresh_session(new_session, force_refresh=True):
                logger.warning(f"Session refresh failed for {new_session.proxy}")
        
        # Reset cooling watchdog counter when we successfully get a session
        self._cool_wait_secs = 0.0
        
        self.current_session = new_session
        return new_session
    
    def _extract_proxy_id(self, proxy: Proxy) -> str:
        """Extract readable session ID from proxy URL (e.g., tm001 from sessid-tm001)."""
        proxy_url = proxy.url
        # Look for sessid-XXX pattern in Decodo URLs
        import re
        match = re.search(r'sessid-([a-z0-9]+)', proxy_url)
        if match:
            return match.group(1)
        # Fallback: use last 6 chars of host:port
        return f"{proxy.host[-4:]}:{proxy.port}"

    def mark_request(self, session: ProxySession, success: bool) -> None:
        """Record a request result for a session."""
        session.request_count += 1
        session.last_used = time.time()
        
        if success:
            session.success_count += 1
            session.consecutive_failures = 0
            session.transport_failures = 0
            session.cool_until = 0.0
        else:
            session.failure_count += 1
            session.consecutive_failures += 1
            
            # Mark unhealthy if too many failures
            if session.consecutive_failures >= self.max_consecutive_failures:
                session.is_healthy = False
                logger.warning(f"Session {session.proxy} marked unhealthy after {session.consecutive_failures} failures")
        
        # Enhanced per-request logging for rotation debugging
        proxy_id = self._extract_proxy_id(session.proxy)
        status = "OK" if success else "FAIL"
        total_success = sum(s.success_count for s in self.sessions.values())
        logger.info(
            f"[{proxy_id}] req#{session.request_count}/{self.requests_per_session} {status} | "
            f"total_ok={total_success} rotations={self.total_rotations}"
        )

    def mark_result(self, session: ProxySession, response) -> None:
        """
        Record a request result with basic error classification.

        - CaptchaRequiredError is handled elsewhere (mark_captcha()).
        - Transport failures trigger cooldown/backoff but don't permanently kill the session.
        """
        success = bool(getattr(response, "success", False))
        error_code = getattr(response, "error_code", None)

        # Count this request against the session quota
        session.request_count += 1
        session.last_used = time.time()

        if success:
            session.success_count += 1
            session.consecutive_failures = 0
            session.transport_failures = 0
            session.cool_until = 0.0
            return

        session.failure_count += 1

        # Transport-ish: timeouts, connection errors, Cloudflare 52x, generic 5xx
        is_transport = False
        if error_code:
            if error_code.startswith("transport_"):
                is_transport = True
            elif error_code == "http_429":
                # Rate limiting is usually proxy/IP level. Cool down and rotate without permanently killing sessid.
                is_transport = True
            elif error_code in ("http_522", "http_520", "http_521", "http_523", "http_524"):
                is_transport = True
            elif error_code.startswith("http_5"):
                is_transport = True

        if is_transport:
            session.transport_failures += 1
            # Exponential backoff with cap
            if error_code == "http_429":
                backoff = min(300.0, (2 ** min(session.transport_failures, 6)) * 15.0)
            else:
                backoff = min(300.0, (2 ** min(session.transport_failures, 6)) * 5.0)
            session.cool_until = time.time() + backoff
            proxy_id = self._extract_proxy_id(session.proxy)
            logger.warning(f"[{proxy_id}] transport failure ({error_code}); cooldown {backoff:.0f}s")
            return

        # Default failure path: count toward unhealthy threshold
        session.consecutive_failures += 1
        if session.consecutive_failures >= self.max_consecutive_failures:
            session.is_healthy = False
            logger.warning(
                f"Session {session.proxy} marked unhealthy after {session.consecutive_failures} failures"
            )
    
    def mark_captcha(self, session: ProxySession) -> None:
        """Mark that a session hit CAPTCHA - needs refresh and proxy cooling."""
        session.consecutive_failures = self.max_consecutive_failures  # Force refresh
        session.is_healthy = False
        proxy_id = self._extract_proxy_id(session.proxy)
        
        # Hardening v2: Track CAPTCHA at proxy level for cooling
        proxy_marked_bad = self.proxy_manager.mark_captcha(session.proxy)
        self.captcha_triggers += 1
        
        status_msg = "MARKED_BAD" if proxy_marked_bad else "rotating"
        logger.warning(
            f"[{proxy_id}] CAPTCHA after {session.request_count} reqs - "
            f"{status_msg} (rotation #{self.total_rotations + 1})"
        )
    
    def get_stats(self) -> dict:
        """Get pool statistics including Hardening v2 metrics."""
        healthy = sum(1 for s in self.sessions.values() if s.is_healthy)
        total_requests = sum(s.request_count for s in self.sessions.values())
        total_success = sum(s.success_count for s in self.sessions.values())
        
        # Hardening v2: Include proxy cooling stats
        proxy_stats = self.proxy_manager.get_cooling_stats()
        
        # Count sessions whose proxies are actively cooling
        sessions_cooling = sum(1 for s in self.sessions.values() if s.proxy.is_cooling)
        
        return {
            "total_sessions": len(self.sessions),
            "healthy_sessions": healthy,
            "total_rotations": self.total_rotations,
            "total_refreshes": self.total_refreshes,
            "total_requests": total_requests,
            "total_success": total_success,
            "overall_success_rate": total_success / total_requests * 100 if total_requests > 0 else 0,
            # Hardening v2 stats
            "captcha_triggers": self.captcha_triggers,
            "bad_proxies": proxy_stats["bad_proxy_count"],
            "proxies_cooling": proxy_stats["currently_cooling"],
            "sessions_cooling": sessions_cooling,
            "total_captcha_hits": proxy_stats["total_captcha_hits"],
        }
    
    def report(self) -> str:
        """Generate a formatted stats report for logging."""
        stats = self.get_stats()
        return (
            f"SessionPool: {stats['healthy_sessions']}/{stats['total_sessions']} healthy | "
            f"rotations={stats['total_rotations']} | "
            f"success_rate={stats['overall_success_rate']:.1f}% | "
            f"captchas={stats['captcha_triggers']} | "
            f"bad_proxies={stats['bad_proxies']} (cooling={stats['proxies_cooling']})"
        )
    
    async def close(self) -> None:
        """Clean up resources."""
        if self._cookie_manager:
            await self._cookie_manager.close()




