"""
Cookie Manager - Auto-refresh CellMapper session cookies.

Supports multiple cookie sources:
1. Redis cookie pool (preferred) - cookies minted by harvester service
2. Playwright (headless Chromium) - fallback for self-minting
3. FlareSolverr - Docker-based Cloudflare bypass

Features:
- Redis cookie pool integration for high-throughput worker sharing
- Automatic CAPTCHA solving via 2Captcha, CapSolver, CapMonster, or FlareSolverr
- Stealth mode to avoid bot detection
- Session caching and refresh intervals
- Cookie poisoning support (delete from pool on CAPTCHA hit)

Install: pip install playwright && playwright install chromium

For CAPTCHA auto-solve:
    export CAPTCHA_PROVIDER=capsolver  # or 2captcha, capmonster, flaresolverr
    export CAPTCHA_API_KEY=your_api_key
    
For Redis cookie pool:
    export REDIS_URL=redis://localhost:6379/0
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Literal, Optional, TYPE_CHECKING
from urllib.parse import urlparse

from config.settings import MAX_COOKIE_REUSE, REQUESTS_PER_SESSION

if TYPE_CHECKING:
    from .captcha.providers import CaptchaProvider
    from .cookie_pool import CookiePool, PooledCookie

logger = logging.getLogger(__name__)


# =============================================================================
# PersistentHarvester - Reuse browser context across multiple cookie mints
# =============================================================================

class PersistentHarvester:
    """
    Persistent Playwright harvester that reuses browser context across mints.
    
    This reduces bandwidth by avoiding cold-start resource loads (~2MB per mint).
    Assets are cached in the browser after the first load.
    
    Usage:
        harvester = PersistentHarvester()
        
        # Mint multiple cookies with same browser instance
        for _ in range(10):
            cookies, ua = await harvester.mint_cookie(proxy_url=...)
            
        # Clean up when done
        await harvester.close()
    
    Safety:
        - Context is rotated every N mints to avoid fingerprinting detection
        - Browser is restarted on errors
    """
    
    def __init__(
        self,
        max_mints_per_context: int = 10,
        harvest_url: str = None,
    ):
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._mint_count = 0
        self._max_mints_per_context = max_mints_per_context
        self._current_proxy_url: Optional[str] = None
        self._harvest_url = harvest_url or os.environ.get(
            "HARVEST_URL", "https://www.cellmapper.net/"
        )
        
        # User agent - consistent with worker TLS fingerprint
        self._user_agent = os.environ.get(
            "PLAYWRIGHT_UA",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        )
        
        logger.info(
            f"PersistentHarvester initialized (max_mints_per_context={max_mints_per_context})"
        )
    
    async def _ensure_browser(self, proxy_url: Optional[str] = None) -> None:
        """
        Ensure browser is running. Creates new browser if needed.
        
        If proxy_url changes, we need a new context (proxies are context-level in Playwright).
        """
        from playwright.async_api import async_playwright
        
        # Start Playwright if not running
        if self._playwright is None:
            self._playwright = await async_playwright().start()
            logger.info("PersistentHarvester: Playwright started")
        
        # Start browser if not running
        if self._browser is None:
            self._browser = await self._playwright.chromium.launch(headless=True)
            logger.info("PersistentHarvester: Browser launched")
        
        # Check if we need a new context (proxy changed or first time)
        need_new_context = (
            self._context is None or
            self._current_proxy_url != proxy_url
        )
        
        if need_new_context:
            await self._new_context(proxy_url)
    
    async def _new_context(self, proxy_url: Optional[str] = None) -> None:
        """Create a new browser context with optional proxy."""
        from urllib.parse import urlparse
        from playwright_stealth import Stealth
        
        # Close existing context
        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass
        
        # Configure proxy
        context_options = {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": self._user_agent,
        }
        
        if proxy_url:
            parsed = urlparse(proxy_url)
            if parsed.hostname and parsed.port:
                server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
                proxy_cfg = {"server": server}
                if parsed.username:
                    proxy_cfg["username"] = parsed.username
                if parsed.password:
                    proxy_cfg["password"] = parsed.password
                context_options["proxy"] = proxy_cfg
                logger.info(f"PersistentHarvester: Proxy configured: {parsed.hostname}:{parsed.port}")
        
        self._context = await self._browser.new_context(**context_options)
        self._page = await self._context.new_page()
        
        # Block heavy resources to save bandwidth
        async def _route_block(route):
            req = route.request
            rtype = req.resource_type
            url = (req.url or "").lower()
            if rtype in {"image", "media", "font", "stylesheet"}:
                return await route.abort()
            if url.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", 
                           ".woff", ".woff2", ".ttf", ".otf")):
                return await route.abort()
            return await route.continue_()
        
        try:
            await self._page.route("**/*", _route_block)
        except Exception:
            pass
        
        # Apply stealth patches
        stealth = Stealth()
        await stealth.apply_stealth_async(self._page)
        
        self._current_proxy_url = proxy_url
        self._mint_count = 0
        
        logger.info("PersistentHarvester: New context created")
    
    async def mint_cookie(
        self,
        proxy_url: Optional[str] = None,
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Mint a new cookie using the persistent browser.
        
        Returns:
            Tuple of (cookie_string, user_agent) or (None, None) on failure.
        """
        # Ensure browser and context are ready
        await self._ensure_browser(proxy_url)
        
        # Check if we should rotate context (anti-fingerprinting)
        if self._mint_count >= self._max_mints_per_context:
            logger.info(
                f"PersistentHarvester: Rotating context after {self._mint_count} mints"
            )
            await self._new_context(proxy_url)
        
        try:
            # Navigate to harvest URL
            # On subsequent visits, many resources will be cached
            logger.info(f"PersistentHarvester: Navigating to {self._harvest_url}")
            
            await self._page.goto(
                self._harvest_url,
                wait_until="domcontentloaded",
                timeout=90000,
            )
            
            # Wait for session establishment
            await asyncio.sleep(2)
            
            # For /map routes, wait for map canvas (SPA handshake)
            if "/map" in self._harvest_url:
                try:
                    await self._page.wait_for_selector(
                        "canvas.leaflet-zoom-animated", 
                        timeout=20000
                    )
                    logger.info("PersistentHarvester: Map canvas rendered")
                except Exception:
                    try:
                        await self._page.wait_for_selector(
                            ".leaflet-container", 
                            timeout=15000
                        )
                        logger.info("PersistentHarvester: Map container visible")
                    except Exception:
                        logger.warning("PersistentHarvester: Map not confirmed rendered")
            
            # Extract cookies
            all_cookies = await self._context.cookies()
            cookies_dict = {c["name"]: c["value"] for c in all_cookies}
            
            if not cookies_dict:
                logger.warning("PersistentHarvester: No cookies received")
                return None, None
            
            cookie_string = "; ".join(f"{k}={v}" for k, v in cookies_dict.items())
            
            self._mint_count += 1
            logger.info(
                f"PersistentHarvester: Minted cookie ({len(cookies_dict)} cookies, "
                f"mint #{self._mint_count})"
            )
            
            return cookie_string, self._user_agent
            
        except Exception as e:
            logger.error(f"PersistentHarvester: Mint failed: {e}")
            # On error, restart browser to ensure clean state
            await self._restart_browser()
            return None, None
    
    async def _restart_browser(self) -> None:
        """Restart browser after an error."""
        logger.info("PersistentHarvester: Restarting browser")
        
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        
        self._browser = None
        self._context = None
        self._page = None
        self._mint_count = 0
    
    async def close(self) -> None:
        """Clean up all resources."""
        logger.info("PersistentHarvester: Shutting down")
        
        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass
        
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        
        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass
        
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None

# Cookie file location
COOKIES_FILE = Path(__file__).parent.parent / "config" / "cookies.txt"
CONFIG_DIR = Path(__file__).parent.parent / "config"


def get_carrier_cookies_file(carrier: str) -> Path:
    """Get carrier-specific cookie file path."""
    return CONFIG_DIR / f"cookies_{carrier}.txt"


class CookieManager:
    """
    Manages CellMapper session cookies with multiple sources.
    
    Priority Order:
        1. Redis cookie pool (if available) - fastest, shared across workers
        2. Playwright headless browser - fallback for self-minting
        3. FlareSolverr Docker container - Cloudflare bypass
    
    Features:
        - Redis pool integration for high-throughput scraping
        - Automatic cookie refresh via headless browser
        - CAPTCHA detection and auto-solving (with configured provider)
        - Session caching to reduce refresh overhead
        - Cookie poisoning (delete from pool on NEED_RECAPTCHA)
    
    Usage:
        manager = CookieManager(use_redis_pool=True)
        cookies = await manager.get_valid_cookies()
        
        # On CAPTCHA hit:
        await manager.mark_cookie_poisoned()
    """
    
    def __init__(
        self,
        cookies_file: Path = None,
        carrier: str = None,
        cookie_engine: Literal["auto", "playwright", "flaresolverr"] = "auto",
        use_redis_pool: bool = True,
    ):
        # Use carrier-specific cookie file if carrier is provided
        if carrier:
            self.cookies_file = get_carrier_cookies_file(carrier)
            self.carrier = carrier
        else:
            self.cookies_file = cookies_file or COOKIES_FILE
            self.carrier = None
        
        self._cached_cookies: Optional[str] = None
        self._cached_user_agent: Optional[str] = None
        self._cached_proxy_url: Optional[str] = None
        self._current_pool_key: Optional[str] = None  # Redis key of current cookie
        self._current_pooled_cookie: Optional["PooledCookie"] = None
        self._last_refresh: float = 0
        self._refresh_interval = 3600  # Refresh every hour to be safe
        self._captcha_provider: Optional["CaptchaProvider"] = None
        self._captcha_provider_loaded = False
        self.cookie_engine: Literal["auto", "playwright", "flaresolverr"] = cookie_engine
        
        # Redis cookie pool settings
        self.use_redis_pool = use_redis_pool
        self._cookie_pool: Optional["CookiePool"] = None
        self._pool_connect_attempted = False
        
        # Global request counter for proactive cookie rotation
        self._requests_on_cookie: int = 0
        
        if self.carrier:
            logger.info(f"CookieManager initialized for carrier: {self.carrier} -> {self.cookies_file}")
    
    def _get_captcha_provider(self) -> Optional["CaptchaProvider"]:
        """Lazy-load the configured CAPTCHA provider."""
        if not self._captcha_provider_loaded:
            self._captcha_provider_loaded = True
            try:
                from .captcha import get_captcha_provider
                self._captcha_provider = get_captcha_provider()
                if self._captcha_provider:
                    logger.info(f"CAPTCHA provider loaded: {type(self._captcha_provider).__name__}")
            except ImportError as e:
                logger.debug(f"CAPTCHA module not available: {e}")
        return self._captcha_provider
    
    async def _get_cookie_pool(self) -> Optional["CookiePool"]:
        """Lazy-connect to Redis cookie pool."""
        if not self.use_redis_pool:
            return None
        
        if self._cookie_pool is not None:
            return self._cookie_pool
        
        if self._pool_connect_attempted:
            return None  # Already tried and failed
        
        self._pool_connect_attempted = True
        
        try:
            from .cookie_pool import CookiePool
            pool = CookiePool()
            await pool.connect()
            self._cookie_pool = pool
            logger.info("CookieManager connected to Redis cookie pool")
            return pool
        except ImportError:
            logger.debug("cookie_pool module not available")
            return None
        except Exception as e:
            logger.warning(f"Failed to connect to Redis cookie pool: {e}")
            return None
    
    async def get_cookie_from_pool(self, proxy_url: Optional[str] = None) -> bool:
        """
        Try to get a cookie from the Redis pool.
        
        Returns True if a cookie was obtained and cached.
        """
        pool = await self._get_cookie_pool()
        if not pool:
            return False

        strict_pool = os.environ.get("STRICT_REDIS_POOL", "false").lower() == "true"
        wait_timeout_sec = float(os.environ.get("COOKIE_POOL_WAIT_TIMEOUT_SECONDS", "0") or "0")
        # Low-water mark: do not drain the pool to zero under contention.
        # This prevents stampedes where many workers empty the pool and then all stall.
        min_pool = int(os.environ.get("COOKIEPOOL_MIN", "3") or "3")
        start = time.time()
        
        while True:
            try:
                if strict_pool and min_pool > 0:
                    try:
                        pool_count = await pool.count()
                    except Exception:
                        pool_count = 0
                    if pool_count <= min_pool:
                        if wait_timeout_sec > 0 and (time.time() - start) >= wait_timeout_sec:
                            logger.warning("Cookie pool low and wait timeout exceeded")
                            return False
                        logger.info(
                            f"Cookie pool low (count={pool_count} <= min={min_pool}) - waiting for harvester..."
                        )
                        await asyncio.sleep(30)
                        continue

                cookie = await pool.get(proxy_url=proxy_url)
                if cookie:
                    # Enforce global cap on cookie reuse to reduce NEED_RECAPTCHA without
                    # increasing harvester load excessively.
                    max_reuse = int(MAX_COOKIE_REUSE or 0)
                    if max_reuse > 0 and getattr(cookie, "use_count", 0) >= max_reuse:
                        logger.info(
                            f"Discarding pooled cookie at reuse limit: {cookie.key} "
                            f"(use_count={getattr(cookie, 'use_count', 0)}, max={max_reuse})"
                        )
                        # CookiePool.get() already consumed it via GETDEL; just skip it.
                        continue

                    # Format cookies as string
                    self._cached_cookies = "; ".join(
                        f"{k}={v}" for k, v in cookie.cookies.items()
                    )
                    self._cached_user_agent = cookie.user_agent
                    self._cached_proxy_url = cookie.proxy_url
                    # CookiePool.get() consumes cookies (GETDEL). We can optionally put it back later
                    # (sequential reuse across workers) if we determine it is still good.
                    self._current_pool_key = cookie.key
                    self._current_pooled_cookie = cookie
                    self._last_refresh = time.time()
                    logger.info(f"Got cookie from pool: {cookie.key}")
                    return True

                if not strict_pool:
                    logger.debug("Cookie pool is empty")
                    return False

                # STRICT mode: wait for harvester to refill pool rather than falling back to Playwright.
                if wait_timeout_sec > 0 and (time.time() - start) >= wait_timeout_sec:
                    logger.warning("Cookie pool still empty after wait timeout")
                    return False

                logger.info("Cookie pool empty - waiting for harvester...")
                await asyncio.sleep(3)
            except Exception as e:
                logger.warning(f"Error getting cookie from pool: {e}")
                if not strict_pool:
                    return False
                if wait_timeout_sec > 0 and (time.time() - start) >= wait_timeout_sec:
                    return False
                await asyncio.sleep(3)
    
    async def mark_cookie_poisoned(self) -> bool:
        """
        Mark the current cookie as poisoned (CAPTCHA hit) and delete from pool.
        
        Call this when NEED_RECAPTCHA is received. The cookie will be removed
        from Redis so other workers don't use it.
        
        Returns True if the cookie was deleted.
        """
        # Cookies fetched from the pool are consumed (removed) at fetch time, so
        # poisoning is purely local state reset. Do NOT put the cookie back.
        if self._current_pool_key:
            logger.info(f"Poisoned cookie (consumed) marked bad: {self._current_pool_key}")
        self._current_pool_key = None
        self._current_pooled_cookie = None
        self._cached_cookies = None
        self._cached_user_agent = None
        self._cached_proxy_url = None
        return True

    async def return_cookie_to_pool(self) -> bool:
        """
        Return the currently checked-out pooled cookie back into Redis.

        This enables sequential re-use of *good* cookies across workers and reduces
        cookie harvesting pressure.

        Safety:
        - Only call this when the current cookie has NOT triggered NEED_RECAPTCHA.
        - If there is no pooled cookie, this is a no-op.
        """
        if not self._current_pooled_cookie:
            return False

        pool = await self._get_cookie_pool()
        if not pool:
            return False

        try:
            ok = await pool.put_back(self._current_pooled_cookie)
        except Exception as e:
            logger.warning(f"Failed to return cookie to pool: {e}")
            ok = False

        # Clear local state either way; callers should fetch a fresh cookie if needed.
        self._current_pool_key = None
        self._current_pooled_cookie = None
        self._cached_cookies = None
        self._cached_user_agent = None
        self._cached_proxy_url = None
        self._requests_on_cookie = 0  # Reset counter
        return ok

    async def report_success(self) -> bool:
        """
        Report a successful API request and rotate cookie if threshold reached.

        Call this after every successful API request (HTTP 200, not NEED_RECAPTCHA).
        Tracks requests globally (across all CellMapperClient instances) and proactively
        rotates the cookie before hitting the RECAPTCHA threshold.

        Returns:
            True if cookie was rotated (caller should recreate HTTP client with new cookie).
            False if no rotation needed.
        """
        self._requests_on_cookie += 1

        # Check if we should rotate
        if not self._should_rotate():
            return False

        # Rotation needed
        logger.info(
            f"CookieManager: rotating after {self._requests_on_cookie} requests "
            f"(threshold={REQUESTS_PER_SESSION})"
        )

        try:
            # Return current cookie to pool (it's still good, just nearing limit)
            await self.return_cookie_to_pool()

            # Get a fresh cookie from pool
            got_cookie = await self.get_cookie_from_pool()
            if got_cookie:
                logger.info("CookieManager: rotation complete - fresh cookie loaded")
                return True
            else:
                logger.warning("CookieManager: rotation failed - pool empty, continuing with current state")
                return False
        except Exception as e:
            logger.warning(f"CookieManager: rotation failed: {e}")
            return False

    def _should_rotate(self) -> bool:
        """Check if cookie should be rotated based on request count."""
        return (
            REQUESTS_PER_SESSION > 0
            and self._requests_on_cookie >= REQUESTS_PER_SESSION
        )
    
    def get_user_agent(self) -> Optional[str]:
        """Get the user agent associated with the current cookie (from pool)."""
        return self._cached_user_agent

    def get_proxy_url(self) -> Optional[str]:
        """Get the proxy URL associated with the current pooled cookie (if any)."""
        return self._cached_proxy_url
    
    def load_cookies(self) -> str:
        """Load cookies from file."""
        if self.cookies_file.exists():
            cookies = self.cookies_file.read_text().strip()
            if cookies:
                logger.info(f"Loaded cookies from {self.cookies_file}")
                return cookies
        return ""
    
    def save_cookies(self, cookies: str) -> None:
        """Save cookies to file."""
        self.cookies_file.parent.mkdir(parents=True, exist_ok=True)
        self.cookies_file.write_text(cookies)
        logger.info(f"Saved cookies to {self.cookies_file}")
    
    def get_cookies(self) -> str:
        """Get current cookies (from cache or file)."""
        if self._cached_cookies:
            return self._cached_cookies
        
        self._cached_cookies = self.load_cookies()
        return self._cached_cookies
    
    async def _validate_cookies_via_api(self, cookie_string: str, proxy_url: Optional[str] = None) -> bool:
        """Validate harvested cookies by making a lightweight CellMapper API call.

        Returns True if the API accepts the cookies (no NEED_RECAPTCHA).
        """
        if not cookie_string:
            return False

        try:
            # Lazily import to avoid circulars / heavy deps when not needed
            from .api_client import CellMapperClient, APIResponse
        except Exception as e:
            logger.warning(f"Cookie validation skipped (import error): {e}")
            return True  # fail-open so we don't block harvest if api_client unavailable

        # CRITICAL: Validate with getTowers (the actual endpoint workers use), not getFrequency
        # Use a tiny geographic bounds to minimize response size
        test_bounds = {
            "north": 40.01,
            "south": 40.00,
            "east": -74.00,
            "west": -74.01,
        }
        
        async with CellMapperClient(
            cookies=cookie_string,
            use_proxy=bool(proxy_url),
            proxy_url=proxy_url,
            rotate_proxies=False,
            fast_mode=False,
            cookie_manager=None,
        ) as cm:
            try:
                resp: APIResponse = await cm.get_towers(mcc=310, mnc=260, bounds=test_bounds, technology="LTE")
            except Exception as e:
                logger.debug(f"Cookie validation transport error: {e}")
                return False

        if not resp.success:
            return False
        # Check for NEED_RECAPTCHA in response
        if isinstance(resp.data, dict) and resp.data.get("statusCode") == "NEED_RECAPTCHA":
            return False
        return True

    async def refresh_cookies(self, proxy_url: Optional[str] = None) -> str:
        """
        Get fresh cookies using Playwright headless browser.
        
        If FlareSolverr is configured, uses that instead of Playwright.
        Otherwise uses Playwright with optional CAPTCHA solving.
        
        Args:
            proxy_url: Optional proxy URL (e.g., "http://user:pass@host:port")
                      If provided, cookies will be fetched through this proxy.
        
        Returns:
            Cookie string or empty string on failure
        """
        proxy_info = f" via proxy {proxy_url.split('@')[-1] if proxy_url and '@' in proxy_url else proxy_url}" if proxy_url else ""
        logger.info(f"Refreshing CellMapper session cookies{proxy_info}...")
        
        # Optional FlareSolverr path (only when explicitly forced, or when auto-detected in 'auto' mode)
        provider = self._get_captcha_provider()
        use_flaresolverr = (
            self.cookie_engine == "flaresolverr"
            or (self.cookie_engine == "auto" and provider and type(provider).__name__ == "FlareSolverrProvider")
        )

        if use_flaresolverr:
            if not provider or type(provider).__name__ != "FlareSolverrProvider":
                logger.error("Cookie engine is flaresolverr but FlareSolverrProvider is not configured")
            else:
                try:
                    max_attempts = int(os.environ.get("FLARESOLVERR_VALIDATE_ATTEMPTS", "2") or "2")
                    logger.info(f"Using FlareSolverr for cookie refresh (validate_attempts={max_attempts})...")

                    for attempt in range(1, max_attempts + 1):
                        # Pass proxy_url so cookies are bound to the correct exit IP (critical for session-pool use).
                        cookies = await provider.get_cookies("https://www.cellmapper.net/map", proxy_url=proxy_url)
                        if not cookies:
                            logger.warning(f"FlareSolverr returned no cookies (attempt {attempt}/{max_attempts})")
                            continue

                        # Validate cookies before accepting
                        if await self._validate_cookies_via_api(cookies, proxy_url=proxy_url):
                            self._cached_cookies = cookies
                            self._last_refresh = time.time()
                            if not proxy_url:
                                self.save_cookies(cookies)
                            return cookies

                        logger.warning(
                            f"FlareSolverr cookies rejected by API (attempt {attempt}/{max_attempts})"
                        )

                    logger.warning("FlareSolverr validation attempts exhausted, falling back to Playwright")
                except Exception as e:
                    logger.warning(f"FlareSolverr failed: {e}, falling back to Playwright")
        
        # Use Playwright (with optional CAPTCHA solving)
        try:
            cookies = await self._refresh_with_playwright(proxy_url=proxy_url)
            if cookies:
                self._cached_cookies = cookies
                self._last_refresh = time.time()
                # Only save to file if not proxy-specific
                if await self._validate_cookies_via_api(cookies, proxy_url=proxy_url):
                    if not proxy_url:
                        self.save_cookies(cookies)
                    return cookies
                logger.warning("Playwright cookies rejected by API")
        except ImportError:
            logger.error(
                "Playwright not installed. Run: pip install playwright && playwright install chromium"
            )
        except Exception as e:
            logger.error(f"Cookie refresh failed: {e}")
        
        return ""
    
    async def _refresh_with_playwright(self, proxy_url: Optional[str] = None) -> str:
        """
        Use Playwright to get cookies from CellMapper.
        
        Launches headless Chromium with stealth patches, visits the site, 
        detects and solves CAPTCHAs if configured, and extracts cookies.
        
        Args:
            proxy_url: Optional proxy URL for the browser to use
        """
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth
        import random
        
        cookies_dict = {}
        
        # Initialize stealth with all evasions enabled
        stealth = Stealth()
        
        # Day-1 hardening: keep UA consistent with worker tls-client fingerprint (chrome_125),
        # unless explicitly overridden. Randomizing UA is *not* worth mismatch risk.
        default_ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        )
        ua_override = os.environ.get("PLAYWRIGHT_UA", "").strip()
        chosen_ua = ua_override or default_ua
        
        max_captcha_attempts = int(os.environ.get("CAPTCHA_MAX_ATTEMPTS", "3"))
        
        async with async_playwright() as p:
            logger.info("Launching headless Chromium with stealth mode...")
            
            # Configure proxy if provided
            launch_options = {"headless": True}
            if proxy_url:
                # Playwright requires proxy auth to be passed separately from server.
                # Our proxy URLs are typically: http://user:pass@host:port
                parsed = urlparse(proxy_url)
                if not parsed.scheme or not parsed.hostname or not parsed.port:
                    raise ValueError(f"Invalid proxy_url format for Playwright: {proxy_url}")

                server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
                proxy_cfg = {"server": server}
                if parsed.username:
                    proxy_cfg["username"] = parsed.username
                if parsed.password:
                    proxy_cfg["password"] = parsed.password
                launch_options["proxy"] = proxy_cfg
                logger.info(f"Playwright proxy configured: {parsed.hostname}:{parsed.port}")
            
            browser = await p.chromium.launch(**launch_options)
            
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=chosen_ua,
            )
            # Persist the minted UA so harvesters can store it alongside cookies in Redis,
            # and workers can replay it exactly.
            self._cached_user_agent = chosen_ua
            
            page = await context.new_page()

            # Bandwidth guardrail: block heavy resources while still allowing JS challenges to run.
            # We allow scripts, documents, XHR/fetch because Cloudflare and session establishment
            # may require JavaScript execution.
            try:
                async def _route_block(route):
                    req = route.request
                    rtype = req.resource_type
                    url = (req.url or "").lower()
                    if rtype in {"image", "media", "font", "stylesheet"}:
                        return await route.abort()
                    if url.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".woff", ".woff2", ".ttf", ".otf")):
                        return await route.abort()
                    return await route.continue_()

                await page.route("**/*", _route_block)
            except Exception:
                # Routing isn't critical; continue without blocking if anything goes wrong.
                pass
            
            # Apply stealth patches to avoid bot detection
            await stealth.apply_stealth_async(page)
            
            try:
                # Visit the map page
                logger.info("Visiting cellmapper.net/map...")
                await page.goto(
                    "https://www.cellmapper.net/map",
                    wait_until="domcontentloaded",  # Faster than networkidle
                    timeout=90000,  # 90 seconds
                )
                
                # Wait for page to fully load and establish session
                logger.info("Waiting for session to establish...")
                await asyncio.sleep(3)
                
                # Check for CAPTCHA and attempt to solve
                captcha_solved = await self._detect_and_solve_captcha(
                    page, max_attempts=max_captcha_attempts
                )
                
                if captcha_solved is False:
                    # CAPTCHA detected but couldn't solve - will still try to get cookies
                    logger.warning("CAPTCHA detected but could not solve")
                
                # Try to interact with carrier selector to ensure session is valid
                try:
                    # Critical: ensure the Leaflet canvas is rendered (SPA XHR handshake done)
                    # before we snapshot cookies. This is where API-authorization tokens are
                    # often established.
                    await page.wait_for_selector("canvas.leaflet-zoom-animated", timeout=20000)
                    logger.info("Map canvas rendered (session handshake complete)")
                except Exception:
                    # Fall back to the container check (older layouts / slow devices).
                    try:
                        await page.wait_for_selector(".leaflet-container", timeout=15000)
                        logger.info("Map container visible (fallback)")
                    except Exception:
                        logger.warning("Map not confirmed rendered; continuing anyway")
                
                # Get all cookies
                all_cookies = await context.cookies()
                
                for cookie in all_cookies:
                    cookies_dict[cookie["name"]] = cookie["value"]
                
                logger.info(f"Got {len(cookies_dict)} cookies")
                
            finally:
                await browser.close()
        
        if not cookies_dict:
            logger.warning("No cookies received")
            return ""
        
        # Format as cookie string
        cookie_string = "; ".join(f"{k}={v}" for k, v in cookies_dict.items())
        
        # Log important cookies
        if "JSESSIONID" in cookies_dict:
            logger.info("✓ JSESSIONID found - session established")
        else:
            logger.warning("⚠ JSESSIONID not found - session may not work")
        
        return cookie_string
    
    async def _detect_and_solve_captcha(
        self, 
        page, 
        max_attempts: int = 3
    ) -> Optional[bool]:
        """
        Detect if a CAPTCHA is present and attempt to solve it.
        
        Supports:
        - hCaptcha (most common on CellMapper)
        - reCAPTCHA v2
        - Cloudflare Turnstile
        
        Args:
            page: Playwright page object
            max_attempts: Maximum solve attempts
            
        Returns:
            True if CAPTCHA was solved
            False if CAPTCHA detected but couldn't solve
            None if no CAPTCHA was detected
        """
        provider = self._get_captcha_provider()
        
        # Check for common CAPTCHA indicators
        captcha_info = await self._detect_captcha_type(page)
        
        if not captcha_info:
            logger.debug("No CAPTCHA detected")
            return None
        
        captcha_type, site_key = captcha_info
        logger.warning(f"CAPTCHA detected: {captcha_type}")
        
        if not provider:
            logger.error(
                "CAPTCHA detected but no provider configured. "
                "Set CAPTCHA_PROVIDER and CAPTCHA_API_KEY environment variables."
            )
            return False
        
        # FlareSolverr handles its own flow
        if type(provider).__name__ == "FlareSolverrProvider":
            logger.warning("FlareSolverr should be used via refresh_cookies(), not here")
            return False
        
        page_url = page.url
        
        for attempt in range(1, max_attempts + 1):
            logger.info(f"Solving CAPTCHA attempt {attempt}/{max_attempts}...")
            
            try:
                # Get solution token from provider
                token = await provider.solve(
                    site_key=site_key,
                    page_url=page_url,
                    captcha_type=captcha_type,
                )
                
                if not token:
                    logger.warning(f"Attempt {attempt}: No token returned")
                    continue
                
                logger.info(f"Got CAPTCHA token ({len(token)} chars)")
                
                # Inject the token into the page
                success = await self._inject_captcha_token(page, captcha_type, token)
                
                if success:
                    logger.info("✓ CAPTCHA solved and submitted")
                    # Wait for page to process
                    await asyncio.sleep(3)
                    return True
                else:
                    logger.warning(f"Attempt {attempt}: Token injection failed")
                    
            except Exception as e:
                logger.error(f"Attempt {attempt}: Solve error - {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(2)
        
        logger.error(f"Failed to solve CAPTCHA after {max_attempts} attempts")
        return False
    
    async def _detect_captcha_type(self, page) -> Optional[tuple]:
        """
        Detect what type of CAPTCHA is on the page and extract site key.
        
        Returns:
            Tuple of (captcha_type, site_key) or None if no CAPTCHA
        """
        # Check for hCaptcha
        hcaptcha = await page.query_selector('[data-sitekey], iframe[src*="hcaptcha.com"]')
        if hcaptcha:
            try:
                site_key = await page.evaluate('''() => {
                    const el = document.querySelector('[data-sitekey]');
                    if (el) return el.getAttribute('data-sitekey');
                    const iframe = document.querySelector('iframe[src*="hcaptcha.com"]');
                    if (iframe) {
                        const match = iframe.src.match(/sitekey=([^&]+)/);
                        return match ? match[1] : null;
                    }
                    return null;
                }''')
                if site_key:
                    return ("hcaptcha", site_key)
            except Exception as e:
                logger.debug(f"Error extracting hCaptcha key: {e}")
        
        # Check for reCAPTCHA
        recaptcha = await page.query_selector('.g-recaptcha, iframe[src*="recaptcha"]')
        if recaptcha:
            try:
                site_key = await page.evaluate('''() => {
                    const el = document.querySelector('.g-recaptcha[data-sitekey]');
                    if (el) return el.getAttribute('data-sitekey');
                    const iframe = document.querySelector('iframe[src*="recaptcha"]');
                    if (iframe) {
                        const match = iframe.src.match(/k=([^&]+)/);
                        return match ? match[1] : null;
                    }
                    return null;
                }''')
                if site_key:
                    return ("recaptcha", site_key)
            except Exception as e:
                logger.debug(f"Error extracting reCAPTCHA key: {e}")
        
        # Check for Cloudflare Turnstile
        turnstile = await page.query_selector('[data-cf-turnstile-sitekey], iframe[src*="challenges.cloudflare.com"]')
        if turnstile:
            try:
                site_key = await page.evaluate('''() => {
                    const el = document.querySelector('[data-cf-turnstile-sitekey]');
                    if (el) return el.getAttribute('data-cf-turnstile-sitekey');
                    return null;
                }''')
                if site_key:
                    return ("turnstile", site_key)
            except Exception as e:
                logger.debug(f"Error extracting Turnstile key: {e}")
        
        # Check for Cloudflare challenge page
        cf_challenge = await page.query_selector('#cf-challenge-running, .cf-browser-verification')
        if cf_challenge:
            logger.warning("Cloudflare challenge detected (not token-based)")
            return None  # Can't solve via token, need FlareSolverr
        
        return None
    
    async def _inject_captcha_token(self, page, captcha_type: str, token: str) -> bool:
        """
        Inject the CAPTCHA solution token into the page and submit.
        
        Args:
            page: Playwright page object
            captcha_type: Type of CAPTCHA
            token: Solution token from provider
            
        Returns:
            True if injection succeeded
        """
        try:
            if captcha_type == "hcaptcha":
                # Inject into hCaptcha response fields
                await page.evaluate(f'''(token) => {{
                    // Set response in textarea
                    const textarea = document.querySelector('[name="h-captcha-response"], textarea[name="g-recaptcha-response"]');
                    if (textarea) textarea.value = token;
                    
                    // Also set in hidden input if exists
                    const hidden = document.querySelector('input[name="h-captcha-response"]');
                    if (hidden) hidden.value = token;
                    
                    // Trigger callback if available
                    if (window.hcaptcha && window.hcaptcha.execute) {{
                        // Already have token, try to submit form
                    }}
                    
                    // Try to find and submit the form
                    const form = document.querySelector('form');
                    if (form) form.submit();
                }}''', token)
                return True
                
            elif captcha_type == "recaptcha":
                # Inject into reCAPTCHA response fields
                await page.evaluate(f'''(token) => {{
                    const textarea = document.querySelector('#g-recaptcha-response, textarea[name="g-recaptcha-response"]');
                    if (textarea) {{
                        textarea.value = token;
                        textarea.style.display = 'block';
                    }}
                    
                    // Trigger callback if available
                    if (window.grecaptcha && window.grecaptcha.callback) {{
                        window.grecaptcha.callback(token);
                    }}
                    
                    // Try to find and submit the form
                    const form = document.querySelector('form');
                    if (form) form.submit();
                }}''', token)
                return True
                
            elif captcha_type == "turnstile":
                # Inject Cloudflare Turnstile token
                await page.evaluate(f'''(token) => {{
                    const input = document.querySelector('[name="cf-turnstile-response"]');
                    if (input) input.value = token;
                    
                    // Try callback
                    if (window.turnstile && window.turnstile._callbacks) {{
                        Object.values(window.turnstile._callbacks).forEach(cb => cb(token));
                    }}
                    
                    const form = document.querySelector('form');
                    if (form) form.submit();
                }}''', token)
                return True
            
            else:
                logger.warning(f"Unknown captcha type: {captcha_type}")
                return False
                
        except Exception as e:
            logger.error(f"Token injection error: {e}")
            return False
    
    async def get_valid_cookies(self, force_refresh: bool = False, proxy_url: Optional[str] = None) -> str:
        """
        Get valid cookies, refreshing if needed.
        
        Priority:
            1. Return cached cookies if still valid
            2. Try Redis cookie pool (shared across workers)
            3. Load from file
            4. Refresh via Playwright/FlareSolverr
        
        Args:
            force_refresh: Force a refresh even if cookies exist
            
        Returns:
            Cookie string
        """
        # Check if we need to refresh
        needs_refresh = (
            force_refresh or
            not self._cached_cookies or
            (time.time() - self._last_refresh > self._refresh_interval)
        )
        
        if not needs_refresh:
            return self.get_cookies()
        
        # Priority 1: Try Redis cookie pool (fastest, shared)
        got_from_pool = await self.get_cookie_from_pool(proxy_url=proxy_url)
        if got_from_pool:
            return self._cached_cookies

        # In strict pool mode, do not fall back to local files or Playwright.
        if os.environ.get("STRICT_REDIS_POOL", "false").lower() == "true" and self.use_redis_pool:
            return ""  # Caller should treat as unavailable and back off.
        
        # Priority 2: Try to load from file
        cookies = self.load_cookies()
        if cookies and not force_refresh:
            self._cached_cookies = cookies
            self._last_refresh = time.time()
            return cookies
        
        # Priority 3: Refresh cookies via Playwright/FlareSolverr
        return await self.refresh_cookies()
    
    def invalidate(self) -> None:
        """Mark current cookies as invalid (will refresh on next get)."""
        self._cached_cookies = None
        self._last_refresh = 0
        logger.info("Cookies invalidated, will refresh on next request")
    
    async def close(self) -> None:
        """Clean up resources."""
        # Close CAPTCHA provider if it has resources
        if self._captcha_provider and hasattr(self._captcha_provider, 'close'):
            try:
                await self._captcha_provider.close()
            except Exception as e:
                logger.debug(f"Error closing captcha provider: {e}")
        
        # Close Redis cookie pool
        if self._cookie_pool:
            try:
                await self._cookie_pool.close()
            except Exception as e:
                logger.debug(f"Error closing cookie pool: {e}")
            self._cookie_pool = None


# Per-carrier cookie manager instances
_managers: dict[tuple, CookieManager] = {}
_default_manager: Optional[CookieManager] = None


def get_cookie_manager(
    carrier: str = None,
    cookie_engine: Literal["auto", "playwright", "flaresolverr"] = "auto",
    use_redis_pool: bool = True,
) -> CookieManager:
    """
    Get a cookie manager instance.
    
    Args:
        carrier: If provided, returns a carrier-specific manager with isolated cookies.
                 If None, returns a shared default manager.
        cookie_engine: Cookie refresh mechanism (auto, playwright, flaresolverr)
        use_redis_pool: Whether to use Redis cookie pool (default: True)
    
    Returns:
        CookieManager instance (carrier-specific or default)
    """
    global _default_manager, _managers
    
    if carrier:
        key = (carrier, cookie_engine, use_redis_pool)
        if key not in _managers:
            _managers[key] = CookieManager(
                carrier=carrier,
                cookie_engine=cookie_engine,
                use_redis_pool=use_redis_pool,
            )
        return _managers[key]
    else:
        if _default_manager is None:
            _default_manager = CookieManager(
                cookie_engine=cookie_engine,
                use_redis_pool=use_redis_pool,
            )
        return _default_manager

