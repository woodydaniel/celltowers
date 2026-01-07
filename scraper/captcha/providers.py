"""
CAPTCHA Provider Implementations

Supports multiple backends for automatic CAPTCHA solving:
- 2Captcha: Human solvers, reliable, ~15-30s solve time
- CapSolver: AI-based, fastest ~5-12s, best for hCaptcha/Turnstile
- CapMonster: Reliable premium solver, ~8-20s typical
- FlareSolverr: Free, runs locally via Docker, handles Cloudflare challenges

Environment Variables:
    CAPTCHA_PROVIDER: "2captcha" | "capsolver" | "capmonster" | "flaresolverr"
    CAPTCHA_API_KEY: API key for 2Captcha or CapSolver
    FLARESOLVERR_URL: URL for FlareSolverr (default: http://localhost:8191)
"""

from __future__ import annotations

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)


class CaptchaProvider(ABC):
    """Abstract base class for CAPTCHA solving providers."""
    
    @abstractmethod
    async def solve(
        self,
        site_key: str,
        page_url: str,
        captcha_type: str = "hcaptcha",
    ) -> str:
        """
        Solve a CAPTCHA and return the response token.
        
        Args:
            site_key: The sitekey from the CAPTCHA element
            page_url: The URL where the CAPTCHA appears
            captcha_type: Type of captcha ("hcaptcha", "recaptcha", "turnstile")
            
        Returns:
            The solved CAPTCHA token to inject
            
        Raises:
            CaptchaSolveError: If solving fails
        """
        pass
    
    @abstractmethod
    async def get_cookies(self, url: str) -> str:
        """
        Get cookies for a URL by solving any challenges.
        
        This is used by FlareSolverr which handles the full flow.
        Other providers return empty string (use Playwright flow instead).
        
        Args:
            url: The URL to get cookies for
            
        Returns:
            Cookie string or empty string if not supported
        """
        pass


class CaptchaSolveError(Exception):
    """Raised when CAPTCHA solving fails."""
    pass


class TwoCaptchaProvider(CaptchaProvider):
    """
    2Captcha.com API provider.
    
    Uses human workers to solve CAPTCHAs.
    Supports hCaptcha, reCAPTCHA v2/v3, and more.
    
    Cost: ~$1.50 per 1000 solves
    Speed: 15-30 seconds typical
    """
    
    API_BASE = "http://2captcha.com"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._http: Optional["aiohttp.ClientSession"] = None
    
    async def _get_http(self) -> "aiohttp.ClientSession":
        """Lazy-load aiohttp session."""
        # Tests may inject an AsyncMock into self._http; AsyncMock.closed is truthy.
        # Only treat the session as closed when it's literally True.
        if self._http is None or getattr(self._http, "closed", False) is True:
            import aiohttp
            self._http = aiohttp.ClientSession()
        return self._http
    
    async def solve(
        self,
        site_key: str,
        page_url: str,
        captcha_type: str = "hcaptcha",
    ) -> str:
        logger.info(f"2Captcha: Solving {captcha_type} for {page_url}")
        
        http = await self._get_http()
        
        # Map captcha type to 2Captcha method
        method_map = {
            "hcaptcha": "hcaptcha",
            "recaptcha": "userrecaptcha",
            "turnstile": "turnstile",
        }
        method = method_map.get(captcha_type, "hcaptcha")
        
        # Step 1: Submit the captcha
        submit_params = {
            "key": self.api_key,
            "method": method,
            "sitekey": site_key,
            "pageurl": page_url,
            "json": 1,
        }
        
        try:
            req = http.get(
                f"{self.API_BASE}/in.php",
                params=submit_params,
                timeout=30,
            )
            if asyncio.iscoroutine(req):
                req = await req
            async with req as resp:
                data = await resp.json()
                
            if data.get("status") != 1:
                error = data.get("request", "Unknown error")
                raise CaptchaSolveError(f"2Captcha submit failed: {error}")
            
            task_id = data["request"]
            logger.info(f"2Captcha: Task submitted, ID={task_id}")
            
            # Step 2: Poll for result (max 120 seconds)
            for attempt in range(24):  # 24 * 5s = 120s max
                await asyncio.sleep(5)
                
                req = http.get(
                    f"{self.API_BASE}/res.php",
                    params={
                        "key": self.api_key,
                        "action": "get",
                        "id": task_id,
                        "json": 1,
                    },
                    timeout=30,
                )
                if asyncio.iscoroutine(req):
                    req = await req
                async with req as resp:
                    result = await resp.json()
                
                if result.get("status") == 1:
                    token = result["request"]
                    logger.info("2Captcha: Solved successfully")
                    return token
                elif result.get("request") == "CAPCHA_NOT_READY":
                    logger.debug(f"2Captcha: Not ready, attempt {attempt + 1}/24")
                    continue
                else:
                    error = result.get("request", "Unknown error")
                    raise CaptchaSolveError(f"2Captcha solve failed: {error}")
            
            raise CaptchaSolveError("2Captcha: Timeout waiting for solution")
            
        except asyncio.TimeoutError:
            raise CaptchaSolveError("2Captcha: Request timeout")
        except Exception as e:
            if isinstance(e, CaptchaSolveError):
                raise
            raise CaptchaSolveError(f"2Captcha error: {e}")
    
    async def get_cookies(self, url: str) -> str:
        """2Captcha doesn't fetch cookies directly."""
        return ""
    
    async def close(self) -> None:
        """Close HTTP session."""
        if self._http and not self._http.closed:
            await self._http.close()


class CapSolverProvider(CaptchaProvider):
    """
    CapSolver.com API provider.
    
    AI-powered CAPTCHA solving - fastest option.
    Excellent for hCaptcha, Cloudflare Turnstile.
    
    Cost: ~$1.20 per 1000 solves
    Speed: 5-12 seconds typical
    """
    
    API_BASE = "https://api.capsolver.com"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._http: Optional["aiohttp.ClientSession"] = None
    
    async def _get_http(self) -> "aiohttp.ClientSession":
        """Lazy-load aiohttp session."""
        if self._http is None or getattr(self._http, "closed", False) is True:
            import aiohttp
            self._http = aiohttp.ClientSession()
        return self._http
    
    async def solve(
        self,
        site_key: str,
        page_url: str,
        captcha_type: str = "hcaptcha",
    ) -> str:
        logger.info(f"CapSolver: Solving {captcha_type} for {page_url}")
        
        http = await self._get_http()
        
        # Map captcha type to CapSolver task type
        task_type_map = {
            "hcaptcha": "HCaptchaTaskProxyLess",
            "recaptcha": "ReCaptchaV2TaskProxyLess",
            "turnstile": "AntiTurnstileTaskProxyLess",
        }
        task_type = task_type_map.get(captcha_type, "HCaptchaTaskProxyLess")
        
        # Create task
        payload = {
            "clientKey": self.api_key,
            "task": {
                "type": task_type,
                "websiteURL": page_url,
                "websiteKey": site_key,
            },
        }
        
        try:
            req = http.post(
                f"{self.API_BASE}/createTask",
                json=payload,
                timeout=30,
            )
            if asyncio.iscoroutine(req):
                req = await req
            async with req as resp:
                data = await resp.json()
            
            if data.get("errorId", 0) != 0:
                error = data.get("errorDescription", "Unknown error")
                raise CaptchaSolveError(f"CapSolver create failed: {error}")
            
            task_id = data["taskId"]
            logger.info(f"CapSolver: Task created, ID={task_id}")
            
            # Poll for result (max 60 seconds - CapSolver is fast)
            for attempt in range(20):  # 20 * 3s = 60s max
                await asyncio.sleep(3)
                
                req = http.post(
                    f"{self.API_BASE}/getTaskResult",
                    json={"clientKey": self.api_key, "taskId": task_id},
                    timeout=30,
                )
                if asyncio.iscoroutine(req):
                    req = await req
                async with req as resp:
                    result = await resp.json()
                
                if result.get("errorId", 0) != 0:
                    error = result.get("errorDescription", "Unknown error")
                    raise CaptchaSolveError(f"CapSolver failed: {error}")
                
                status = result.get("status")
                if status == "ready":
                    solution = result.get("solution", {})
                    token = solution.get("gRecaptchaResponse") or solution.get("token")
                    if token:
                        logger.info("CapSolver: Solved successfully")
                        return token
                    raise CaptchaSolveError("CapSolver: No token in response")
                elif status == "processing":
                    logger.debug(f"CapSolver: Processing, attempt {attempt + 1}/20")
                    continue
                else:
                    raise CaptchaSolveError(f"CapSolver: Unexpected status {status}")
            
            raise CaptchaSolveError("CapSolver: Timeout waiting for solution")
            
        except asyncio.TimeoutError:
            raise CaptchaSolveError("CapSolver: Request timeout")
        except Exception as e:
            if isinstance(e, CaptchaSolveError):
                raise
            raise CaptchaSolveError(f"CapSolver error: {e}")
    
    async def get_cookies(self, url: str) -> str:
        """CapSolver doesn't fetch cookies directly."""
        return ""
    
    async def close(self) -> None:
        """Close HTTP session."""
        if self._http and not self._http.closed:
            await self._http.close()


class CapMonsterProvider(CaptchaProvider):
    """
    CapMonster.cloud API provider.

    Similar flow to CapSolver: createTask + poll getTaskResult.

    Cost: varies by CAPTCHA type
    Speed: ~8-20 seconds typical
    """

    API_BASE = "https://api.capmonster.cloud"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._http: Optional["aiohttp.ClientSession"] = None

    async def _get_http(self) -> "aiohttp.ClientSession":
        """Lazy-load aiohttp session."""
        if self._http is None or getattr(self._http, "closed", False) is True:
            import aiohttp
            self._http = aiohttp.ClientSession()
        return self._http

    async def solve(
        self,
        site_key: str,
        page_url: str,
        captcha_type: str = "hcaptcha",
    ) -> str:
        logger.info(f"CapMonster: Solving {captcha_type} for {page_url}")

        http = await self._get_http()

        task_type_map = {
            "hcaptcha": "HCaptchaTaskProxyless",
            "recaptcha": "RecaptchaV2TaskProxyless",
            "turnstile": "TurnstileTaskProxyless",
        }
        task_type = task_type_map.get(captcha_type, "HCaptchaTaskProxyless")

        payload = {
            "clientKey": self.api_key,
            "task": {
                "type": task_type,
                "websiteURL": page_url,
                "websiteKey": site_key,
            },
        }

        try:
            req = http.post(f"{self.API_BASE}/createTask", json=payload, timeout=30)
            if asyncio.iscoroutine(req):
                req = await req
            async with req as resp:
                data = await resp.json()

            if data.get("errorId", 0) != 0:
                raise CaptchaSolveError(f"CapMonster create failed: {data.get('errorDescription', 'Unknown error')}")

            task_id = data.get("taskId")
            if not task_id:
                raise CaptchaSolveError("CapMonster: Missing taskId")

            # Poll up to ~120s (CapMonster can be slower than CapSolver)
            for attempt in range(40):  # 40 * 3s = 120s
                await asyncio.sleep(3)
                req = http.post(
                    f"{self.API_BASE}/getTaskResult",
                    json={"clientKey": self.api_key, "taskId": task_id},
                    timeout=30,
                )
                if asyncio.iscoroutine(req):
                    req = await req
                async with req as resp:
                    result = await resp.json()

                if result.get("errorId", 0) != 0:
                    raise CaptchaSolveError(f"CapMonster failed: {result.get('errorDescription', 'Unknown error')}")

                status = result.get("status")
                if status == "ready":
                    solution = result.get("solution", {}) or {}
                    token = (
                        solution.get("gRecaptchaResponse")
                        or solution.get("token")
                        or solution.get("captchaSolve")
                    )
                    if token:
                        logger.info("CapMonster: Solved successfully")
                        return str(token)
                    raise CaptchaSolveError("CapMonster: No token in response")
                if status == "processing":
                    continue
                raise CaptchaSolveError(f"CapMonster: Unexpected status {status}")

            raise CaptchaSolveError("CapMonster: Timeout waiting for solution")

        except asyncio.TimeoutError:
            raise CaptchaSolveError("CapMonster: Request timeout")
        except Exception as e:
            if isinstance(e, CaptchaSolveError):
                raise
            raise CaptchaSolveError(f"CapMonster error: {e}")

    async def get_cookies(self, url: str) -> str:
        """CapMonster doesn't fetch cookies directly."""
        return ""

    async def close(self) -> None:
        if self._http and not self._http.closed:
            await self._http.close()


class FlareSolverrProvider(CaptchaProvider):
    """
    FlareSolverr provider - runs locally via Docker.
    
    Uses a headful browser (puppeteer) to solve Cloudflare challenges.
    Free but requires Docker container running.
    
    Docker: docker run -d -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest
    
    Cost: Free (self-hosted)
    Speed: 4-10 seconds typical
    """
    
    def __init__(self, url: str = "http://localhost:8191"):
        self.url = url.rstrip("/")
        self._http: Optional["aiohttp.ClientSession"] = None
    
    async def _get_http(self) -> "aiohttp.ClientSession":
        """Lazy-load aiohttp session."""
        if self._http is None or getattr(self._http, "closed", False) is True:
            import aiohttp
            self._http = aiohttp.ClientSession()
        return self._http
    
    async def solve(
        self,
        site_key: str,
        page_url: str,
        captcha_type: str = "hcaptcha",
    ) -> str:
        """
        FlareSolverr doesn't return tokens - it solves the full challenge.
        Use get_cookies() instead for the full flow.
        """
        raise CaptchaSolveError(
            "FlareSolverr doesn't support token extraction. "
            "Use get_cookies() for full challenge bypass."
        )
    
    async def get_cookies(self, url: str, proxy_url: Optional[str] = None) -> str:
        """
        Use FlareSolverr to get cookies after solving any challenges.
        
        Args:
            url: The URL to visit and get cookies from
            
        Returns:
            Cookie string formatted for HTTP headers
        """
        logger.info(f"FlareSolverr: Getting cookies for {url}")
        
        http = await self._get_http()
        
        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": 60000,  # 60 seconds
        }
        # When provided, run the challenge through the SAME proxy as scraping.
        # This is important because clearance cookies are bound to the exit IP.
        if proxy_url:
            from urllib.parse import urlparse
            parsed = urlparse(proxy_url)
            if parsed.scheme and parsed.hostname and parsed.port:
                proxy_cfg = {"url": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
                if parsed.username:
                    proxy_cfg["username"] = parsed.username
                if parsed.password:
                    proxy_cfg["password"] = parsed.password
                payload["proxy"] = proxy_cfg
            else:
                logger.warning(f"FlareSolverr: invalid proxy_url, ignoring: {proxy_url}")
        
        try:
            req = http.post(
                f"{self.url}/v1",
                json=payload,
                timeout=90,
            )
            if asyncio.iscoroutine(req):
                req = await req
            async with req as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise CaptchaSolveError(f"FlareSolverr HTTP {resp.status}: {text}")
                
                data = await resp.json()
            
            if data.get("status") != "ok":
                message = data.get("message", "Unknown error")
                raise CaptchaSolveError(f"FlareSolverr failed: {message}")
            
            solution = data.get("solution", {})
            cookies = solution.get("cookies", [])
            
            if not cookies:
                logger.warning("FlareSolverr: No cookies returned")
                return ""
            
            # Format cookies as header string
            cookie_string = "; ".join(
                f"{c['name']}={c['value']}" for c in cookies
            )
            
            logger.info(f"FlareSolverr: Got {len(cookies)} cookies")
            return cookie_string
            
        except asyncio.TimeoutError:
            raise CaptchaSolveError("FlareSolverr: Request timeout")
        except Exception as e:
            if isinstance(e, CaptchaSolveError):
                raise
            raise CaptchaSolveError(f"FlareSolverr error: {e}")
    
    async def close(self) -> None:
        """Close HTTP session."""
        if self._http and not self._http.closed:
            await self._http.close()


def get_captcha_provider() -> Optional[CaptchaProvider]:
    """
    Get the configured CAPTCHA provider based on environment variables.
    
    Environment Variables:
        CAPTCHA_PROVIDER: "2captcha" | "capsolver" | "capmonster" | "flaresolverr"
        CAPTCHA_API_KEY: API key (for 2captcha/capsolver/capmonster)
        FLARESOLVERR_URL: URL for FlareSolverr (default: http://localhost:8191)
    
    Returns:
        Configured provider instance or None if not configured
    """
    provider_name = os.environ.get("CAPTCHA_PROVIDER", "").lower().strip()
    api_key = os.environ.get("CAPTCHA_API_KEY", "").strip()
    flaresolverr_url = os.environ.get("FLARESOLVERR_URL", "http://localhost:8191")
    
    if not provider_name:
        logger.debug("No CAPTCHA_PROVIDER configured")
        return None
    
    if provider_name == "2captcha":
        if not api_key:
            logger.warning("2Captcha selected but no CAPTCHA_API_KEY set")
            return None
        logger.info("Using 2Captcha provider")
        return TwoCaptchaProvider(api_key)
    
    elif provider_name == "capsolver":
        if not api_key:
            logger.warning("CapSolver selected but no CAPTCHA_API_KEY set")
            return None
        logger.info("Using CapSolver provider")
        return CapSolverProvider(api_key)

    elif provider_name == "capmonster":
        if not api_key:
            logger.warning("CapMonster selected but no CAPTCHA_API_KEY set")
            return None
        logger.info("Using CapMonster provider")
        return CapMonsterProvider(api_key)
    
    elif provider_name == "flaresolverr":
        logger.info(f"Using FlareSolverr provider at {flaresolverr_url}")
        return FlareSolverrProvider(flaresolverr_url)
    
    else:
        logger.warning(f"Unknown CAPTCHA_PROVIDER: {provider_name}")
        return None

