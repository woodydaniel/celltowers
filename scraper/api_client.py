"""
CellMapper API Client

Handles HTTP requests to CellMapper's internal API with rate limiting,
retry logic, and session management.

API discovered via browser network inspection:
- Base: https://api.cellmapper.net/v6/
- Endpoints: getTowers, getFrequency, getSiteDetails

Transport Options:
- tls-client (default): Chrome-grade TLS fingerprint, bypasses fingerprint detection
- httpx: Fallback if tls-client is not available
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Optional, Union
from urllib.parse import urlencode

import httpx
try:
    import redis.asyncio as redis_async  # type: ignore
except Exception:  # pragma: no cover
    redis_async = None

# Try to import tls-client for Chrome TLS fingerprinting
try:
    import tls_client
    TLS_CLIENT_AVAILABLE = True
except ImportError:
    TLS_CLIENT_AVAILABLE = False

# Day-1 hardening: tls-client is mandatory for workers (fingerprint alignment).
# If tls-client isn't available, we fall back to httpx, but deployments should
# include tls-client in requirements.
USE_TLS_CLIENT = TLS_CLIENT_AVAILABLE


# =============================================================================
# Custom Exceptions
# =============================================================================

class CellMapperError(Exception):
    """Base exception for CellMapper API errors."""
    pass


class CaptchaRequiredError(CellMapperError):
    """
    Raised when CellMapper requires CAPTCHA/session verification.
    
    This typically means:
    - No cookies provided
    - Cookies have expired
    - Session was invalidated
    
    Solution: Get fresh cookies from a browser session.
    """
    pass


class RateLimitedError(CellMapperError):
    """Raised when we've been rate limited by CellMapper."""
    pass


class ProxyBlockedError(CellMapperError):
    """Raised when the current proxy appears to be blocked."""
    pass

from config.settings import (
    BROWSER_COOKIES,
    CONFIG_DIR,
    DEFAULT_HEADERS,
    ENABLE_PROXY_BYTES,
    FAST_MODE,
    FAST_MIN_PROXIES,
    FAST_REQUEST_DELAY_MAX,
    FAST_REQUEST_DELAY_MIN,
    MAX_RETRIES,
    PROXY_URL,
    REQUEST_DELAY_MAX,
    REQUEST_DELAY_MIN,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF_FACTOR,
    RETRY_INITIAL_DELAY,
    USE_PROXY,
)
from .proxy_manager import ProxyManager, Proxy
from .cookie_manager import CookieManager

# CellMapper API base URL (discovered via network inspection)
CELLMAPPER_API_BASE = "https://api.cellmapper.net/v6"

logger = logging.getLogger(__name__)


async def _track_proxy_bandwidth(
    proxy_url: str | None,
    bytes_sent: int,
    bytes_recv: int,
) -> None:
    """
    Track bandwidth usage per proxy endpoint in Redis.
    
    Stores daily counters in Redis hash: proxy:bytes:YYYY-MM-DD
    Fields: {host:port:sent, host:port:recv}
    """
    if not ENABLE_PROXY_BYTES or not proxy_url:
        return
    
    try:
        # Extract host:port from proxy URL
        from urllib.parse import urlparse
        parsed = urlparse(proxy_url)
        if not parsed.hostname or not parsed.port:
            return
        
        proxy_endpoint = f"{parsed.hostname}:{parsed.port}"
        
        # Get today's date for the Redis key
        from datetime import datetime
        date_key = datetime.utcnow().strftime("%Y-%m-%d")
        redis_key = f"proxy:bytes:{date_key}"
        
        # Get Redis connection
        r = await _get_metrics_redis()
        if not r:
            return
        
        # Increment sent/recv counters
        await r.hincrbyfloat(redis_key, f"{proxy_endpoint}:sent", float(bytes_sent))
        await r.hincrbyfloat(redis_key, f"{proxy_endpoint}:recv", float(bytes_recv))
        
        # Set expiry to 30 days
        await r.expire(redis_key, 30 * 24 * 3600)
    except Exception as e:
        logger.debug(f"Bandwidth tracking failed: {e}")

API_REQUESTS_OK_KEY = "cellmapper:counters:api_requests_ok"
API_NEED_RECAPTCHA_KEY = "cellmapper:counters:api_need_recaptcha"

_metrics_redis_client = None


async def _get_metrics_redis():
    global _metrics_redis_client
    if _metrics_redis_client is not None:
        return _metrics_redis_client
    if redis_async is None:
        return None
    redis_url = os.environ.get("REDIS_URL", "").strip()
    if not redis_url:
        return None
    try:
        _metrics_redis_client = redis_async.from_url(redis_url, encoding="utf-8", decode_responses=True)
        return _metrics_redis_client
    except Exception:
        return None


@dataclass
class APIResponse:
    """Wrapper for API responses."""
    
    success: bool
    status_code: int
    data: Optional[Union[dict, list]] = None
    error: Optional[str] = None
    # Machine-friendly error classification for retry/rotation/metrics.
    # Examples: "http_403", "http_429", "http_522", "transport_timeout", "transport_error"
    error_code: Optional[str] = None
    raw_text: Optional[str] = None
    request_time: float = 0.0


class RateLimiter:
    """Simple rate limiter with jitter."""
    
    def __init__(self, min_delay: float, max_delay: float):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.last_request_time = 0.0
        self.fast_mode_enabled = FAST_MODE
    
    async def wait(self) -> None:
        """Wait appropriate time before next request."""
        now = time.time()
        elapsed = now - self.last_request_time
        delay = random.uniform(self.min_delay, self.max_delay)
        
        if elapsed < delay:
            wait_time = delay - elapsed
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)
        
        self.last_request_time = time.time()


# Thread pool for tls-client (blocking calls)
_tls_executor: Optional[ThreadPoolExecutor] = None


def _get_tls_executor() -> ThreadPoolExecutor:
    """Get or create the thread pool for tls-client calls."""
    global _tls_executor
    if _tls_executor is None:
        _tls_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="tls-client")
    return _tls_executor


@dataclass
class TLSResponse:
    """Wrapper to normalize tls-client response to match httpx interface."""
    status_code: int
    text: str
    headers: dict
    
    def json(self) -> Any:
        return json.loads(self.text)


class CellMapperClient:
    """
    HTTP client for CellMapper API with rate limiting and retry logic.
    
    API Base: https://api.cellmapper.net/v6/
    
    Discovered endpoints:
    - getTowers: Get towers within geographic bounds
    - getFrequency: Get frequency info for a channel
    - getSiteDetails: Get detailed site information
    
    IMPORTANT: CellMapper requires a valid browser session to avoid CAPTCHA.
    You need to:
    1. Visit cellmapper.net in your browser
    2. Extract cookies (especially JSESSIONID)
    3. Pass them via CELLMAPPER_COOKIES env var or cookies parameter
    """
    
    def __init__(
        self,
        base_url: str = CELLMAPPER_API_BASE,
        headers: Optional[dict] = None,
        cookies: Optional[str] = None,
        use_proxy: bool = USE_PROXY,
        proxy_url: Optional[str] = PROXY_URL,
        proxy_manager: Optional[ProxyManager] = None,
        cookie_manager: Optional[CookieManager] = None,
        rotate_proxies: bool = True,
        fast_mode: Optional[bool] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = headers or DEFAULT_HEADERS.copy()
        # Determine delay mode
        self.fast_mode = FAST_MODE if fast_mode is None else bool(fast_mode)
        if self.fast_mode and proxy_manager and proxy_manager.total_count >= FAST_MIN_PROXIES:
            self.rate_limiter = RateLimiter(FAST_REQUEST_DELAY_MIN, FAST_REQUEST_DELAY_MAX)
            logger.info(
                f"Fast mode enabled: delay {FAST_REQUEST_DELAY_MIN}-{FAST_REQUEST_DELAY_MAX}s "
                f"(proxies={proxy_manager.total_count})"
            )
        else:
            self.rate_limiter = RateLimiter(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            if self.fast_mode and proxy_manager:
                logger.info(
                    f"Fast mode requested but proxy pool too small (proxies={proxy_manager.total_count}, "
                    f"need >= {FAST_MIN_PROXIES}); using safe delays"
                )
        
        # Cookie manager for Redis pool integration
        self.cookie_manager = cookie_manager
        
        # Parse cookies from string or env var
        self.cookies = self._parse_cookies(cookies or BROWSER_COOKIES)
        if self.cookies:
            logger.info(f"Loaded {len(self.cookies)} cookies from configuration")
        elif not cookie_manager:
            logger.warning(
                "No browser cookies configured. API may return NEED_RECAPTCHA. "
                "Set CELLMAPPER_COOKIES env var with your browser cookies."
            )
        
        # Proxy configuration
        self.proxy_manager = proxy_manager
        self.rotate_proxies = rotate_proxies
        self.current_proxy: Optional[Proxy] = None
        self._forced_proxy_url: Optional[str] = None
        
        # Legacy single proxy support
        if not proxy_manager and use_proxy and proxy_url:
            self.proxy_config = proxy_url
        else:
            self.proxy_config = None
        
        # HTTP client (created in async context)
        self._client: Optional[httpx.AsyncClient] = None
        
        # TLS-client session (Chrome fingerprint)
        self._tls_session: Optional["tls_client.Session"] = None
        self._use_tls_client = USE_TLS_CLIENT
        if self._use_tls_client:
            logger.info("Using tls-client with Chrome TLS fingerprint")
        else:
            logger.info("Using httpx for HTTP requests")
        
        # Request statistics
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.proxy_rotations = 0
        self.captcha_recoveries = 0  # Count of successful CAPTCHA recovery via pool
    
    def _parse_cookies(self, cookie_string: str) -> dict:
        """Parse cookie string into dict format."""
        if not cookie_string:
            return {}
        
        cookies = {}
        for item in cookie_string.split(";"):
            item = item.strip()
            if "=" in item:
                key, value = item.split("=", 1)
                cookies[key.strip()] = value.strip()
        return cookies
    
    def _get_proxy_url(self) -> Optional[str]:
        """Get proxy URL, rotating if proxy manager is configured."""
        if self._forced_proxy_url:
            return self._forced_proxy_url
        if self.proxy_manager and self.proxy_manager.total_count > 0:
            if self.rotate_proxies or self.current_proxy is None:
                self.current_proxy = self.proxy_manager.get_next()
                self.proxy_rotations += 1
            if self.current_proxy:
                return self.current_proxy.url
        return self.proxy_config
    
    async def __aenter__(self) -> "CellMapperClient":
        """Async context manager entry."""
        # If a CookieManager is provided, prefer pulling an initial cookie from Redis pool
        # before creating the HTTP clients. This keeps workers browser-free in v2.
        if self.cookie_manager:
            try:
                desired_proxy = self._get_proxy_url()
                cookie_str = await self.cookie_manager.get_valid_cookies(force_refresh=False, proxy_url=desired_proxy)
                if cookie_str:
                    self.cookies = self._parse_cookies(cookie_str)
                    ua = self.cookie_manager.get_user_agent()
                    if ua:
                        self.headers["User-Agent"] = ua
                    # If cookie was minted via a proxy, force using the same proxy for API requests.
                    pooled_proxy = self.cookie_manager.get_proxy_url()
                    if pooled_proxy:
                        self._forced_proxy_url = pooled_proxy
            except Exception as e:
                logger.warning(f"Initial cookie pool fetch failed, continuing: {e}")

        await self._create_client(proxy_url=self._get_proxy_url())
        return self

    async def _sync_cookie_from_manager(self) -> None:
        """
        Ensure this client has an active cookie + UA loaded from CookieManager.

        This is used both on startup and after we intentionally clear cookies
        (e.g., after returning a good cookie back to the pool for reuse).
        """
        if not self.cookie_manager:
            return

        desired_proxy = self._get_proxy_url()
        cookie_str = await self.cookie_manager.get_valid_cookies(
            force_refresh=False, proxy_url=desired_proxy
        )
        if not cookie_str:
            return

        self.cookies = self._parse_cookies(cookie_str)

        ua = self.cookie_manager.get_user_agent()
        if ua:
            self.headers["User-Agent"] = ua

        pooled_proxy = self.cookie_manager.get_proxy_url()
        if pooled_proxy:
            self._forced_proxy_url = pooled_proxy

        await self._create_client(proxy_url=self._get_proxy_url())
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
    
    async def _create_client(self, proxy_url: Optional[str] = None) -> None:
        """Create the HTTP client with optional proxy."""
        resolved_proxy = proxy_url or self._get_proxy_url()
        
        # Create tls-client session if enabled
        if self._use_tls_client:
            self._create_tls_session(resolved_proxy)
        
        # Also create httpx client as fallback
        if self._client:
            await self._client.aclose()
        
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self.headers,
            cookies=self.cookies,
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
            proxy=resolved_proxy,
        )
        
        proxy_info = f" (proxy: {self.current_proxy})" if self.current_proxy else ""
        transport = "tls-client" if self._use_tls_client else "httpx"
        logger.info(f"HTTP client initialized ({transport}){proxy_info}")
    
    def _create_tls_session(self, proxy_url: Optional[str] = None) -> None:
        """Create a tls-client session with Chrome TLS fingerprint."""
        if not TLS_CLIENT_AVAILABLE:
            return
        
        self._tls_session = tls_client.Session(
            # Must match Playwright harvester Chrome major version to avoid session invalidation.
            client_identifier=os.environ.get("TLS_CLIENT_IDENTIFIER", "chrome_125"),
            random_tls_extension_order=True,
        )
        
        # Set proxy if provided
        if proxy_url:
            self._tls_session.proxies = {
                "http": proxy_url,
                "https": proxy_url,
            }
        
        logger.debug(f"TLS-client session created (chrome_120)")
    
    def _tls_request_sync(
        self,
        method: str,
        url: str,
        headers: dict,
        cookies: dict,
    ) -> TLSResponse:
        """
        Synchronous tls-client request (runs in thread pool).
        
        This is called via run_in_executor to not block the event loop.
        """
        if not self._tls_session:
            raise RuntimeError("TLS session not initialized")
        
        response = self._tls_session.execute_request(
            method=method.upper(),
            url=url,
            headers=headers,
            cookies=cookies,
        )
        
        return TLSResponse(
            status_code=response.status_code,
            text=response.text,
            headers=dict(response.headers),
        )
    
    async def _tls_request(
        self,
        method: str,
        url: str,
    ) -> TLSResponse:
        """Make an async request using tls-client via thread executor."""
        loop = asyncio.get_running_loop()
        executor = _get_tls_executor()
        
        # Build full URL
        full_url = f"{self.base_url}{url}" if not url.startswith("http") else url
        
        return await loop.run_in_executor(
            executor,
            self._tls_request_sync,
            method,
            full_url,
            self.headers,
            self.cookies,
        )
    
    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("HTTP client closed")

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> APIResponse:
        """
        Make an HTTP request with retry logic and exponential backoff.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL (relative to base_url)
            **kwargs: Additional arguments passed to httpx
            
        Returns:
            APIResponse object with results
            
        Raises:
            CaptchaRequiredError: If session is invalid and needs fresh cookies
        """
        if not self._client:
            await self._create_client()
        
        last_error = None
        retry_delay = RETRY_INITIAL_DELAY
        consecutive_proxy_failures = 0

        # If we've cleared cookies (e.g., after returning a pooled cookie),
        # fetch a fresh one before issuing requests.
        if self.cookie_manager and not self.cookies:
            try:
                await self._sync_cookie_from_manager()
            except Exception as e:
                logger.warning(f"Cookie sync failed: {e}")
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                # Apply rate limiting
                await self.rate_limiter.wait()
                
                self.total_requests += 1
                start_time = time.time()
                
                logger.debug(f"Request {method} {url} (attempt {attempt + 1})")
                
                # Use tls-client if available (Chrome TLS fingerprint)
                if self._use_tls_client and self._tls_session:
                    try:
                        response = await self._tls_request(method, url)
                    except Exception as tls_err:
                        # Fall back to httpx on tls-client error
                        logger.warning(f"TLS-client error, falling back to httpx: {tls_err}")
                        response = await self._client.request(method, url, **kwargs)
                else:
                    response = await self._client.request(method, url, **kwargs)
                
                request_time = time.time() - start_time
                
                # Track bandwidth per proxy if enabled
                if ENABLE_PROXY_BYTES:
                    try:
                        # Estimate bytes sent (URL + headers + body)
                        # Rough estimate: ~500 bytes for headers + method/URL
                        bytes_sent = len(url) + 500
                        if kwargs.get("data"):
                            bytes_sent += len(str(kwargs["data"]))
                        if kwargs.get("json"):
                            bytes_sent += len(json.dumps(kwargs["json"]))
                        
                        # Bytes received (response content + headers estimate)
                        bytes_recv = len(response.content) if hasattr(response, 'content') else len(response.text)
                        bytes_recv += 300  # Header overhead estimate
                        
                        # Track with current proxy URL
                        proxy_url = self._get_proxy_url()
                        await _track_proxy_bandwidth(proxy_url, bytes_sent, bytes_recv)
                    except Exception as e:
                        logger.debug(f"Bandwidth tracking error: {e}")
                
                # Check for success
                if response.status_code == 200:
                    self.successful_requests += 1
                    
                    # Mark proxy as working if we have one
                    if self.proxy_manager and self.current_proxy:
                        self.proxy_manager.mark_success(self.current_proxy, latency_ms=request_time * 1000.0)
                    consecutive_proxy_failures = 0
                    
                    try:
                        data = response.json()
                        
                        # Check for CAPTCHA requirement in response body
                        if isinstance(data, dict):
                            status_code = data.get("statusCode", "")
                            if status_code == "NEED_RECAPTCHA":
                                # High-signal diagnostics for tuning:
                                # - Is this happening immediately (bad cookie/proxy) or after many requests (rate limit)?
                                # - Are we respecting cookie->proxy affinity?
                                try:
                                    current_proxy = self._get_proxy_url()
                                except Exception:
                                    current_proxy = None

                                cookie_age_s = None
                                pooled_proxy = None
                                pooled_key = None
                                if self.cookie_manager:
                                    try:
                                        last_refresh = getattr(self.cookie_manager, "_last_refresh", 0.0) or 0.0
                                        if last_refresh > 0:
                                            cookie_age_s = max(0.0, time.time() - float(last_refresh))
                                    except Exception:
                                        cookie_age_s = None
                                    try:
                                        pooled_proxy = self.cookie_manager.get_proxy_url()
                                    except Exception:
                                        pooled_proxy = None
                                    try:
                                        pooled_key = getattr(self.cookie_manager, "_current_pool_key", None)
                                    except Exception:
                                        pooled_key = None

                                logger.warning(
                                    "API returned NEED_RECAPTCHA - attempting cookie recovery "
                                    f"(proxy={current_proxy}, pooled_proxy={pooled_proxy}, "
                                    f"cookie_age_s={cookie_age_s}, pooled_key={pooled_key}, "
                                    f"total_requests={self.total_requests}, successful_requests={self.successful_requests})"
                                )
                                
                                # Track NEED_RECAPTCHA occurrences in Redis for monitoring
                                try:
                                    r = await _get_metrics_redis()
                                    if r:
                                        await r.incr(API_NEED_RECAPTCHA_KEY)
                                except Exception:
                                    pass
                                
                                # Try to recover by getting fresh cookie from pool
                                if self.cookie_manager:
                                    # Mark current cookie as poisoned
                                    await self.cookie_manager.mark_cookie_poisoned()
                                    
                                    # Try to get a fresh cookie from pool (may be proxy-bound)
                                    if await self.cookie_manager.get_cookie_from_pool():
                                        # Update our cookies and recreate client
                                        new_cookies = self.cookie_manager.get_cookies()
                                        self.cookies = self._parse_cookies(new_cookies)
                                        
                                        # Update User-Agent if pool provides one
                                        ua = self.cookie_manager.get_user_agent()
                                        if ua:
                                            self.headers["User-Agent"] = ua

                                        pooled_proxy = self.cookie_manager.get_proxy_url()
                                        if pooled_proxy:
                                            self._forced_proxy_url = pooled_proxy

                                        await self._create_client(proxy_url=self._get_proxy_url())
                                        self.captcha_recoveries += 1
                                        logger.info("CAPTCHA recovery successful - retrying with fresh cookie")
                                        continue  # Retry with new cookie
                                    else:
                                        logger.error("Cookie pool empty - cannot recover from CAPTCHA")
                                
                                # No recovery possible - raise exception
                                raise CaptchaRequiredError(
                                    "Session expired or invalid. Cookie pool empty or not configured."
                                )

                        # Global cookie rotation via CookieManager
                        # Tracks requests across all client instances and rotates proactively
                        if self.cookie_manager:
                            try:
                                rotated = await self.cookie_manager.report_success()
                                if rotated:
                                    # CookieManager rotated the cookie - update client state
                                    new_cookies = self.cookie_manager.get_cookies()
                                    self.cookies = self._parse_cookies(new_cookies)
                                    ua = self.cookie_manager.get_user_agent()
                                    if ua:
                                        self.headers["User-Agent"] = ua
                                    pooled_proxy = self.cookie_manager.get_proxy_url()
                                    if pooled_proxy:
                                        self._forced_proxy_url = pooled_proxy
                                    await self._create_client(proxy_url=self._get_proxy_url())
                            except Exception as e:
                                logger.warning(f"Cookie rotation check failed: {e}")

                        # Best-effort metrics
                        try:
                            r = await _get_metrics_redis()
                            if r:
                                await r.incr(API_REQUESTS_OK_KEY)
                        except Exception:
                            pass
                        
                        return APIResponse(
                            success=True,
                            status_code=response.status_code,
                            data=data,
                            raw_text=response.text,
                            request_time=request_time,
                        )
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON decode error: {e}")
                        return APIResponse(
                            success=True,
                            status_code=response.status_code,
                            raw_text=response.text,
                            error="JSON decode failed",
                            error_code="json_decode",
                            request_time=request_time,
                        )
                
                # Handle rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", retry_delay))
                    logger.warning(f"Rate limited (429). Waiting {retry_after}s")
                    
                    # Rotate proxy if available (this IP might be flagged)
                    if self.proxy_manager and self.current_proxy:
                        self.proxy_manager.mark_failed(self.current_proxy)
                        await self._rotate_proxy()
                    
                    # If this is the last retry, raise the exception
                    if attempt >= MAX_RETRIES:
                        raise RateLimitedError(
                            f"Rate limited after {MAX_RETRIES} retries. "
                            f"Consider reducing request frequency."
                        )
                    
                    await asyncio.sleep(retry_after)
                    retry_delay *= RETRY_BACKOFF_FACTOR
                    continue
                
                # Handle 403 Forbidden - likely blocked
                if response.status_code == 403:
                    logger.warning(f"Forbidden (403) - possibly blocked")
                    if self.proxy_manager and self.current_proxy:
                        self.proxy_manager.mark_failed(self.current_proxy)
                        await self._rotate_proxy()
                    
                    # If no proxies available and on last retry, raise
                    if attempt >= MAX_RETRIES:
                        if self.proxy_manager and self.proxy_manager.available_count == 0:
                            raise ProxyBlockedError(
                                "All proxies appear to be blocked. "
                                "Add more proxies or wait before retrying."
                            )
                    
                    await asyncio.sleep(retry_delay)
                    retry_delay *= RETRY_BACKOFF_FACTOR
                    continue
                
                # Handle server errors (5xx) - retry with proxy rotation
                if response.status_code >= 500:
                    logger.warning(f"Server error {response.status_code}. Rotating proxy and retrying...")
                    
                    # Rotate proxy on 5xx - this endpoint may be having issues
                    if self.proxy_manager and self.current_proxy:
                        self.proxy_manager.mark_failed(self.current_proxy)
                        await self._rotate_proxy()
                    
                    await asyncio.sleep(retry_delay)
                    retry_delay *= RETRY_BACKOFF_FACTOR
                    continue
                
                # Client errors (4xx) - don't retry (except 429, 403 above)
                self.failed_requests += 1
                return APIResponse(
                    success=False,
                    status_code=response.status_code,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                    error_code=f"http_{response.status_code}",
                    request_time=request_time,
                )
                
            except httpx.TimeoutException as e:
                last_error = f"Timeout: {e}"
                logger.warning(f"Request timeout (attempt {attempt + 1})")
                consecutive_proxy_failures += 1
                
                # Rotate proxy on repeated timeouts
                if consecutive_proxy_failures >= 2 and self.proxy_manager:
                    if self.current_proxy:
                        self.proxy_manager.mark_failed(self.current_proxy)
                    await self._rotate_proxy()
                    consecutive_proxy_failures = 0
                
                await asyncio.sleep(retry_delay)
                retry_delay *= RETRY_BACKOFF_FACTOR
                
            except httpx.RequestError as e:
                last_error = f"Request error: {e}"
                logger.warning(f"Request error (attempt {attempt + 1}): {e}")
                consecutive_proxy_failures += 1
                
                # Rotate proxy on connection errors
                if self.proxy_manager and self.current_proxy:
                    self.proxy_manager.mark_failed(self.current_proxy)
                    await self._rotate_proxy()
                    consecutive_proxy_failures = 0
                
                await asyncio.sleep(retry_delay)
                retry_delay *= RETRY_BACKOFF_FACTOR
        
        # All retries exhausted
        self.failed_requests += 1
        return APIResponse(
            success=False,
            status_code=0,
            error=last_error or "Max retries exceeded",
            error_code=(
                "transport_timeout" if (last_error or "").lower().startswith("timeout")
                else "transport_error"
            ),
        )
    
    async def _rotate_proxy(self) -> None:
        """Rotate to a new proxy and recreate the HTTP client."""
        if not self.proxy_manager:
            return
        
        old_proxy = self.current_proxy
        self.current_proxy = self.proxy_manager.get_next()
        self.proxy_rotations += 1
        
        if self.current_proxy:
            logger.info(f"Rotating proxy: {old_proxy} -> {self.current_proxy}")
            await self._create_client()
        else:
            logger.warning("No working proxies available")
    
    async def get_towers(
        self,
        mcc: int,
        mnc: int,
        bounds: dict[str, float],
        technology: str = "LTE",
    ) -> APIResponse:
        """
        Fetch cell towers within geographic bounds.
        
        API: GET /getTowers
        
        Args:
            mcc: Mobile Country Code (310, 311, etc.)
            mnc: Mobile Network Code (260 for T-Mobile, 410 for AT&T, etc.)
            bounds: Dict with 'north', 'south', 'east', 'west' lat/lon bounds
            technology: Radio Access Technology (LTE, NR for 5G, CDMA, GSM)
            
        Returns:
            APIResponse with tower data
        """
        # CellMapper uses NE/SW corner format for bounds
        params = {
            "MCC": mcc,
            "MNC": mnc,
            "RAT": technology,
            "boundsNELatitude": bounds["north"],
            "boundsNELongitude": bounds["east"],
            "boundsSWLatitude": bounds["south"],
            "boundsSWLongitude": bounds["west"],
            "filterFrequency": "false",
            "showOnlyMine": "false",
            "showUnverifiedOnly": "false",
            "showENDCOnly": "false",
            "cache": int(time.time() * 1000),  # Cache buster timestamp
        }
        
        endpoint = f"/getTowers?{urlencode(params)}"
        
        logger.info(f"Fetching towers: MCC={mcc}, MNC={mnc}, RAT={technology}")
        return await self._request_with_retry("GET", endpoint)
    
    async def get_site_details(
        self,
        site_id: int,
        mcc: int,
        mnc: int,
        technology: str = "LTE",
    ) -> APIResponse:
        """
        Fetch detailed information for a specific tower site.
        
        API: GET /getSiteDetails
        
        Args:
            site_id: The site identifier
            mcc: Mobile Country Code
            mnc: Mobile Network Code
            technology: Radio Access Technology
            
        Returns:
            APIResponse with site details
        """
        params = {
            "MCC": mcc,
            "MNC": mnc,
            "RAT": technology,
            "siteID": site_id,
        }
        
        endpoint = f"/getSiteDetails?{urlencode(params)}"
        
        logger.debug(f"Fetching site details: {site_id}")
        return await self._request_with_retry("GET", endpoint)
    
    async def get_frequency(
        self,
        channel: int,
        mcc: int,
        mnc: int,
        technology: str = "LTE",
    ) -> APIResponse:
        """
        Fetch frequency information for a channel/EARFCN.
        
        API: GET /getFrequency
        
        Args:
            channel: EARFCN (E-UTRA Absolute Radio Frequency Channel Number)
            mcc: Mobile Country Code
            mnc: Mobile Network Code
            technology: Radio Access Technology
            
        Returns:
            APIResponse with frequency data (band, frequency MHz, etc.)
        """
        params = {
            "Channel": channel,
            "RAT": technology,
            "MCC": mcc,
            "MNC": mnc,
        }
        
        endpoint = f"/getFrequency?{urlencode(params)}"
        
        logger.debug(f"Fetching frequency: channel={channel}")
        return await self._request_with_retry("GET", endpoint)
    
    async def get_towers_5g(
        self,
        mcc: int,
        mnc: int,
        bounds: dict[str, float],
    ) -> APIResponse:
        """
        Fetch 5G NR towers within geographic bounds.
        
        Convenience method that sets RAT=NR for 5G New Radio.
        
        Args:
            mcc: Mobile Country Code
            mnc: Mobile Network Code
            bounds: Dict with 'north', 'south', 'east', 'west' lat/lon bounds
            
        Returns:
            APIResponse with 5G tower data
        """
        return await self.get_towers(mcc, mnc, bounds, technology="NR")
    
    def get_stats(self) -> dict[str, int]:
        """Get request statistics."""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "captcha_recoveries": self.captcha_recoveries,
            "success_rate": (
                self.successful_requests / self.total_requests * 100
                if self.total_requests > 0
                else 0
            ),
        }

