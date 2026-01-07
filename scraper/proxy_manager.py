"""
Proxy Manager for rotating proxies during scraping.

Supports:
- Webshare.io proxy lists
- Random rotation
- Automatic failover on proxy errors
- CAPTCHA-based cooling (Hardening v2)
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from config.settings import (
    DECODO_HOST,
    DECODO_PASSWORD,
    DECODO_PORTS,
    DECODO_USERNAME,
    PROXY_CAPTCHA_THRESHOLD,
    PROXY_COOLDOWN_HOURS,
    PROXY_FAIL_BASE_COOLDOWN_SEC,
    PROXY_FAIL_MAX_COOLDOWN_SEC,
    PROXY_LIST,
    PROXY_PROVIDER,
    PROXY_SELECTION_STRATEGY,
    PROXY_LATENCY_ALPHA,
)

logger = logging.getLogger(__name__)


@dataclass
class Proxy:
    """Proxy configuration with CAPTCHA cooling support (Hardening v2)."""
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    # Hardening v2: CAPTCHA cooling
    captcha_hits: int = field(default=0, compare=False)
    cool_until: float = field(default=0.0, compare=False)
    is_bad: bool = field(default=False, compare=False)
    
    @property
    def url(self) -> str:
        """Get proxy URL for httpx."""
        if self.username and self.password:
            return f"http://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"http://{self.host}:{self.port}"
    
    @property
    def is_cooling(self) -> bool:
        """Check if proxy is in cooldown period."""
        return self.cool_until > time.time()
    
    @property
    def is_available(self) -> bool:
        """Check if proxy is available for use."""
        return not self.is_bad or not self.is_cooling
    
    def __str__(self) -> str:
        # Include username for uniqueness when using sticky sessions (e.g., sessid-tm001)
        if self.username:
            return f"{self.username}@{self.host}:{self.port}"
        return f"{self.host}:{self.port}"


class ProxyManager:
    """
    Manages a pool of proxies with rotation, failover, and CAPTCHA cooling.
    
    Hardening v2: Proxies that trigger consecutive CAPTCHAs are marked BAD
    and cooled for hours to avoid wasting requests.
    """
    
    def __init__(
        self,
        proxy_file: Optional[Path] = None,
        captcha_threshold: int = PROXY_CAPTCHA_THRESHOLD,
        cooldown_hours: float = PROXY_COOLDOWN_HOURS,
    ):
        self.proxies: list[Proxy] = []
        # Temporary failure cooling (connection/429/etc). Map proxy key -> fail_until epoch seconds.
        self.failed_proxies: dict[str, float] = {}
        self._fail_counts: dict[str, int] = {}
        self.current_index = 0
        self.selection_strategy = (PROXY_SELECTION_STRATEGY or "round_robin").lower().strip()
        self.latency_alpha = float(PROXY_LATENCY_ALPHA)
        
        # Hardening v2: CAPTCHA cooling config
        self.captcha_threshold = captcha_threshold
        self.cooldown_hours = cooldown_hours
        
        # Stats
        self.total_captcha_hits = 0
        self.bad_proxy_count = 0
        self.total_fail_cools = 0
        # Latency/success tracking (used by "fastest" strategy)
        self._latency_ema_ms: dict[str, float] = {}
        self._successes: dict[str, int] = {}
        self._failures: dict[str, int] = {}
        self._last_used: dict[str, float] = {}
        
        if proxy_file and proxy_file.exists():
            self.load_from_file(proxy_file)

        # Optional: add proxies from env/provider generators
        self._load_from_env()
    
    def load_from_file(self, filepath: Path) -> int:
        """
        Load proxies from a file.
        
        Supported formats:
        - IP:PORT
        - IP:PORT:USER:PASS
        - http://user:pass@host:port
        
        Returns number of proxies loaded.
        """
        count = 0
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue
                
                proxy = self._parse_proxy_line(line)
                if proxy:
                    self.proxies.append(proxy)
                    count += 1
        
        logger.info(f"Loaded {count} proxies from {filepath}")
        return count

    def _load_from_env(self) -> int:
        """
        Load proxies from environment variables.

        - PROXY_LIST: newline/comma separated proxies in supported formats
        - PROXY_PROVIDER=decodo with DECODO_* settings to generate a pool
        """
        added = 0

        # Raw proxy list
        if PROXY_LIST:
            for raw in PROXY_LIST.replace(",", "\n").splitlines():
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                proxy = self._parse_proxy_line(line)
                if proxy:
                    self._add_unique(proxy)
                    added += 1

        # Provider generator(s)
        if PROXY_PROVIDER == "decodo":
            gen = self._generate_decodo_pool()
            for p in gen:
                self._add_unique(p)
                added += 1

        if added:
            logger.info(f"Loaded {added} proxies from environment/provider config")
        return added

    def _add_unique(self, proxy: Proxy) -> None:
        """Add proxy if not already present (by string key)."""
        key = str(proxy)
        if any(str(p) == key for p in self.proxies):
            return
        self.proxies.append(proxy)

    def _parse_ports(self, spec: str) -> list[int]:
        spec = (spec or "").strip()
        if not spec:
            return []
        ports: list[int] = []
        for part in spec.split(","):
            part = part.strip()
            if not part:
                continue
            if "-" in part:
                a, b = part.split("-", 1)
                a_i, b_i = int(a), int(b)
                if a_i > b_i:
                    a_i, b_i = b_i, a_i
                ports.extend(list(range(a_i, b_i + 1)))
            else:
                ports.append(int(part))
        return sorted(set(ports))

    def _generate_decodo_pool(self) -> list[Proxy]:
        """
        Generate Decodo gateway proxies from env.

        This supports the common pattern used in this repo:
        - host: gate.decodo.com
        - auth: username/password
        - multiple sticky ports (10001-10030 etc)
        """
        if not DECODO_USERNAME or not DECODO_PASSWORD:
            logger.warning("PROXY_PROVIDER=decodo but DECODO_USERNAME/DECODO_PASSWORD not set")
            return []
        ports = self._parse_ports(DECODO_PORTS)
        if not ports:
            logger.warning("PROXY_PROVIDER=decodo but DECODO_PORTS not set")
            return []
        return [Proxy(DECODO_HOST, port, DECODO_USERNAME, DECODO_PASSWORD) for port in ports]
    
    def _parse_proxy_line(self, line: str) -> Optional[Proxy]:
        """Parse a single proxy line."""
        try:
            # Handle URL format
            if line.startswith("http://") or line.startswith("https://"):
                line = line.split("://", 1)[1]
                if "@" in line:
                    auth, hostport = line.rsplit("@", 1)
                    username, password = auth.split(":", 1)
                    host, port = hostport.split(":", 1)
                    return Proxy(host, int(port), username, password)
                else:
                    host, port = line.split(":", 1)
                    return Proxy(host, int(port))
            
            # Handle IP:PORT or IP:PORT:USER:PASS format
            parts = line.split(":")
            if len(parts) == 2:
                return Proxy(parts[0], int(parts[1]))
            elif len(parts) == 4:
                return Proxy(parts[0], int(parts[1]), parts[2], parts[3])
            else:
                logger.warning(f"Invalid proxy format: {line}")
                return None
                
        except (ValueError, IndexError) as e:
            logger.warning(f"Failed to parse proxy '{line}': {e}")
            return None
    
    def add_proxy(self, host: str, port: int, username: str = None, password: str = None) -> None:
        """Add a proxy to the pool."""
        self.proxies.append(Proxy(host, port, username, password))
    
    def get_next(self) -> Optional[Proxy]:
        """Get the next proxy, skipping cooling proxies."""
        if not self.proxies:
            return None

        # Strategy: choose among available proxies by score (EMA latency) when enabled.
        if self.selection_strategy == "fastest":
            now = time.time()
            candidates: list[Proxy] = []
            for p in self.proxies:
                if self._is_temporarily_failed(p) or p.is_cooling:
                    continue
                # If proxy was marked BAD but cooldown expired, it can be retried.
                if p.is_bad and not p.is_cooling:
                    p.is_bad = False
                    p.captcha_hits = 0
                candidates.append(p)

            if not candidates:
                logger.warning("All proxies are cooling or failed")
                return None

            # Sample to keep this O(1) even with big pools.
            sample_n = min(8, len(candidates))
            sample = random.sample(candidates, k=sample_n) if len(candidates) > sample_n else candidates

            def score(proxy: Proxy) -> float:
                key = str(proxy)
                latency = self._latency_ema_ms.get(key, 10_000.0)  # unknown proxies start worst
                failures = self._failures.get(key, 0)
                # Slightly prefer proxies that haven't been used very recently
                last = self._last_used.get(key, 0.0)
                recency_penalty = 0.0 if (now - last) > 30 else 250.0
                return latency + (failures * 200.0) + recency_penalty

            best = min(sample, key=score)
            self._last_used[str(best)] = now
            return best
        
        # Find next working proxy
        attempts = 0
        while attempts < len(self.proxies):
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            
            # Skip failed proxies
            if self._is_temporarily_failed(proxy):
                attempts += 1
                continue
            
            # Skip ANY cooling proxy (regardless of is_bad flag)
            if proxy.is_cooling:
                attempts += 1
                continue
            
            # Proxy that finished cooling can be retried
            if proxy.is_bad and not proxy.is_cooling:
                proxy.is_bad = False
                proxy.captcha_hits = 0
                logger.info(f"Proxy {proxy} cooldown expired, resetting")
            
            return proxy
        
        # All proxies failed/cooling - DO NOT reset, return None to trigger fallback
        logger.warning("All proxies are cooling or failed")
        return None
    
    def get_random(self) -> Optional[Proxy]:
        """Get a random working proxy, skipping cooling proxies."""
        if not self.proxies:
            return None
        
        # Reset any proxies that have finished cooling first
        for p in self.proxies:
            if p.is_bad and not p.is_cooling:
                p.is_bad = False
                p.captcha_hits = 0
                logger.info(f"Proxy {p} cooldown expired, resetting")
        
        # Filter out failed or cooling proxies (regardless of is_bad)
        available = [
            p for p in self.proxies
            if not self._is_temporarily_failed(p)
            and not p.is_cooling
        ]
        
        if not available:
            # All proxies cooling - return None to trigger fallback wait
            logger.warning("All proxies are cooling or failed")
            return None
        
        return random.choice(available)
    
    def _reset_all_cooling(self) -> None:
        """Reset all failed and cooling proxies when none are available."""
        self.failed_proxies.clear()
        self._fail_counts.clear()
        for p in self.proxies:
            p.is_bad = False
            p.captcha_hits = 0
            p.cool_until = 0.0
        logger.warning("Reset all proxy cooling/failure states")
    
    def mark_failed(self, proxy: Proxy) -> None:
        """
        Mark a proxy as failed (temporary cooldown).

        Unlike CAPTCHA cooling (which is explicit and longer), this is used for
        transient issues: timeouts, connection resets, rate limiting, etc.
        """
        key = str(proxy)
        n = self._fail_counts.get(key, 0) + 1
        self._fail_counts[key] = n
        self._failures[key] = self._failures.get(key, 0) + 1
        # Exponential backoff with cap
        base = max(1.0, float(PROXY_FAIL_BASE_COOLDOWN_SEC))
        max_cool = max(base, float(PROXY_FAIL_MAX_COOLDOWN_SEC))
        cool = min(max_cool, base * (2 ** min(n - 1, 6)))
        # Add jitter (up to 20%)
        cool = cool * random.uniform(1.0, 1.2)
        until = time.time() + cool
        self.failed_proxies[key] = until
        self.total_fail_cools += 1
        logger.warning(
            f"Marked proxy {proxy} as failed (temp {cool:.0f}s, streak={n}) "
            f"({self.temporarily_failed_count}/{len(self.proxies)} cooling)"
        )
    
    def mark_success(self, proxy: Proxy, latency_ms: Optional[float] = None) -> None:
        """Mark a proxy as working (remove from failed list and reset captcha counter)."""
        key = str(proxy)
        self.failed_proxies.pop(key, None)
        self._fail_counts.pop(key, None)
        self._successes[key] = self._successes.get(key, 0) + 1
        if latency_ms is not None and latency_ms > 0:
            prev = self._latency_ema_ms.get(key)
            if prev is None:
                self._latency_ema_ms[key] = float(latency_ms)
            else:
                a = min(max(self.latency_alpha, 0.01), 0.9)
                self._latency_ema_ms[key] = (a * float(latency_ms)) + ((1.0 - a) * prev)
        # Reset captcha counter on success
        proxy.captcha_hits = 0
    
    def mark_captcha(self, proxy: Proxy) -> bool:
        """
        Mark that a proxy triggered a CAPTCHA (Hardening v2).
        
        Applies immediate 15-minute cooldown on first CAPTCHA.
        Returns True if proxy was marked BAD (threshold reached).
        """
        proxy.captcha_hits += 1
        self.total_captcha_hits += 1
        
        # Immediate 15-minute cooldown on ANY captcha to prevent rapid re-trigger
        captcha_cooldown_sec = 900  # 15 minutes
        proxy.cool_until = time.time() + captcha_cooldown_sec
        
        if proxy.captcha_hits >= self.captcha_threshold:
            proxy.is_bad = True
            # Extended cooldown when marked BAD
            proxy.cool_until = time.time() + (self.cooldown_hours * 3600)
            self.bad_proxy_count += 1
            
            hours_str = f"{self.cooldown_hours:.1f}h"
            logger.warning(
                f"Proxy {proxy} marked BAD after {proxy.captcha_hits} CAPTCHAs, "
                f"cooling for {hours_str}"
            )
            return True
        
        logger.info(
            f"Proxy {proxy} CAPTCHA #{proxy.captcha_hits}/{self.captcha_threshold}, "
            f"cooling 15m"
        )
        return False
    
    def get_cooling_stats(self) -> dict:
        """Get CAPTCHA cooling statistics."""
        now = time.time()
        cooling = sum(1 for p in self.proxies if p.is_cooling)
        cooling_bad = sum(1 for p in self.proxies if p.is_bad and p.is_cooling)
        recovered = sum(1 for p in self.proxies if p.is_bad and not p.is_cooling)
        
        return {
            "total_captcha_hits": self.total_captcha_hits,
            "bad_proxy_count": self.bad_proxy_count,
            "currently_cooling": cooling,
            "cooling_bad": cooling_bad,
            "ready_to_recover": recovered,
        }
    
    def get_shortest_cooldown_wait(self) -> float:
        """Get seconds until the next proxy becomes available (0 if one is ready now)."""
        now = time.time()
        
        # Check if any proxy is available now
        for p in self.proxies:
            if not self._is_temporarily_failed(p) and not p.is_cooling:
                return 0.0
        
        # Find the shortest wait time
        min_wait = float('inf')
        for p in self.proxies:
            # Consider both temp-fail cooldown and captcha cooling.
            fail_until = self.failed_proxies.get(str(p), 0.0)
            if fail_until > now:
                wait = fail_until - now
                if wait < min_wait:
                    min_wait = wait
            if p.cool_until > now:
                wait = p.cool_until - now
                if wait < min_wait:
                    min_wait = wait
        
        return min_wait if min_wait != float('inf') else 60.0  # Default 60s if all failed
    
    @property
    def available_count(self) -> int:
        """Number of proxies not currently in temporary-fail cooldown."""
        now = time.time()
        return sum(1 for p in self.proxies if self.failed_proxies.get(str(p), 0.0) <= now)

    @property
    def temporarily_failed_count(self) -> int:
        """Number of proxies currently in temporary-fail cooldown."""
        now = time.time()
        return sum(1 for until in self.failed_proxies.values() if until > now)
    
    @property
    def total_count(self) -> int:
        """Total number of proxies."""
        return len(self.proxies)

    def _is_temporarily_failed(self, proxy: Proxy) -> bool:
        """Check whether a proxy is currently in temporary-fail cooldown."""
        until = self.failed_proxies.get(str(proxy), 0.0)
        return until > time.time()
    
    def __bool__(self) -> bool:
        """True if we have any proxies."""
        return len(self.proxies) > 0
    
    def __len__(self) -> int:
        return len(self.proxies)


def load_webshare_proxies(api_key: str) -> list[Proxy]:
    """
    Load proxies from Webshare API.
    
    Note: This requires the 'requests' package and network access.
    For simplicity, prefer loading from a downloaded file instead.
    """
    import requests
    
    url = "https://proxy.webshare.io/api/v2/proxy/list/"
    headers = {"Authorization": f"Token {api_key}"}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    proxies = []
    for item in response.json().get("results", []):
        proxies.append(Proxy(
            host=item["proxy_address"],
            port=item["port"],
            username=item["username"],
            password=item["password"],
        ))
    
    logger.info(f"Loaded {len(proxies)} proxies from Webshare API")
    return proxies




