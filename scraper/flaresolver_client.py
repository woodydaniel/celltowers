"""
FlareSolverr Client

Provides a small async wrapper around FlareSolverr's HTTP API.

We use this in two places:
- Cookie refresh flows (proxy-bound clearance cookies)
- Conservative fallback for Cloudflare-ish 403 responses (optional)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)


@dataclass
class FlareSolverrResult:
    """Normalized result for a FlareSolverr request."""

    ok: bool
    status_code: int = 0
    response_text: str = ""
    cookies: dict[str, str] = None
    user_agent: str = ""
    error: str = ""


def _proxy_payload(proxy_url: str) -> dict[str, Any]:
    """
    Build a FlareSolverr proxy object from a proxy URL.

    FlareSolverr supports proxy configuration for requests so challenge
    cookies (e.g. cf_clearance) are bound to the same exit IP we use for scraping.
    """
    parsed = urlparse(proxy_url)
    if not parsed.scheme or not parsed.hostname or not parsed.port:
        raise ValueError(f"Invalid proxy_url: {proxy_url}")

    payload: dict[str, Any] = {"url": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        payload["username"] = parsed.username
    if parsed.password:
        payload["password"] = parsed.password
    return payload


class FlareSolverrClient:
    def __init__(self, base_url: str, timeout_sec: float = 90.0):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self._http: Optional[httpx.AsyncClient] = None

    async def _get_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self.timeout_sec)
        return self._http

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def request_get(
        self,
        url: str,
        *,
        proxy_url: Optional[str] = None,
        max_timeout_ms: int = 60_000,
    ) -> FlareSolverrResult:
        """
        Issue FlareSolverr `request.get` and return normalized cookies/response.
        """
        http = await self._get_http()

        payload: dict[str, Any] = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": max_timeout_ms,
        }
        if proxy_url:
            try:
                payload["proxy"] = _proxy_payload(proxy_url)
            except Exception as e:
                return FlareSolverrResult(ok=False, error=f"proxy_payload_error: {e}")

        try:
            resp = await http.post(f"{self.base_url}/v1", json=payload)
        except Exception as e:
            return FlareSolverrResult(ok=False, error=f"flaresolverr_http_error: {e}")

        if resp.status_code != 200:
            return FlareSolverrResult(
                ok=False,
                status_code=resp.status_code,
                error=f"flaresolverr_http_{resp.status_code}: {resp.text[:200]}",
            )

        try:
            data = resp.json()
        except Exception as e:
            return FlareSolverrResult(ok=False, error=f"flaresolverr_json_error: {e}")

        if data.get("status") != "ok":
            return FlareSolverrResult(ok=False, error=f"flaresolverr_status_not_ok: {data.get('message', '')}")

        solution = data.get("solution", {}) or {}
        cookies_list = solution.get("cookies", []) or []
        cookies: dict[str, str] = {}
        for c in cookies_list:
            name = c.get("name")
            value = c.get("value")
            if name and value is not None:
                cookies[str(name)] = str(value)

        return FlareSolverrResult(
            ok=True,
            status_code=int(solution.get("status", 0) or 0),
            response_text=str(solution.get("response", "") or ""),
            cookies=cookies,
            user_agent=str(solution.get("userAgent", "") or ""),
        )


