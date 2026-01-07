"""
CellMapper Scraper Configuration Settings

This file contains all configurable parameters for the scraper.
Adjust these values based on your needs and server capabilities.
"""

import os
from pathlib import Path

# =============================================================================
# Base Paths
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
TOWERS_DIR = DATA_DIR / "towers"
CONFIG_DIR = BASE_DIR / "config"
LOGS_DIR = BASE_DIR / "logs"

# Ensure directories exist
TOWERS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CellMapper API Configuration
# =============================================================================
# API Base URL (discovered via browser network inspection)
CELLMAPPER_API_URL = "https://api.cellmapper.net/v6"

# API endpoints
API_ENDPOINTS = {
    "towers": "/getTowers",
    "site_details": "/getSiteDetails",
    "frequency": "/getFrequency",
}

# Request headers to mimic browser behavior
# Based on actual browser request headers from network inspection
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://www.cellmapper.net",
    "Referer": "https://www.cellmapper.net/",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# =============================================================================
# Rate Limiting Configuration
# =============================================================================
# Requests per second (be conservative to avoid bans)
RATE_LIMIT_RPS = 0.2  # 1 request every 5 seconds (conservative)

# Delay between requests (in seconds)
# Increased significantly to avoid triggering CAPTCHA
# CellMapper is aggressive with bot detection
REQUEST_DELAY_MIN = float(os.environ.get("REQUEST_DELAY_MIN", "12.0"))   # Minimum delay
REQUEST_DELAY_MAX = float(os.environ.get("REQUEST_DELAY_MAX", "20.0"))  # Maximum delay with jitter

# Optional speed mode (opt-in): when enabled and you have a sufficiently large proxy pool,
# the client can use lower inter-request jitter. Defaults are conservative; you should
# monitor `api_success_rate`, `captcha_hits_last_5m`, and 429/403 rates when enabling.
FAST_MODE = os.environ.get("FAST_MODE", "false").lower() == "true"
FAST_MIN_PROXIES = int(os.environ.get("FAST_MIN_PROXIES", "10"))
FAST_REQUEST_DELAY_MIN = float(os.environ.get("FAST_REQUEST_DELAY_MIN", "1.0"))
FAST_REQUEST_DELAY_MAX = float(os.environ.get("FAST_REQUEST_DELAY_MAX", "2.5"))

# Batch processing delays
BATCH_DELAY = 10.0  # Delay between batches of requests

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2.0  # Exponential backoff multiplier
RETRY_INITIAL_DELAY = 10.0  # Initial retry delay in seconds

# =============================================================================
# Session Pool Configuration
# =============================================================================
# Number of requests before rotating to a different proxy+session
# CellMapper rate-limits per IP after ~2-4 requests, so rotate early
REQUESTS_PER_SESSION = 1  # Rotate after 1 request per proxy

# Max number of successful uses per pooled cookie before discarding it.
# This prevents long cookie reuse streaks (15-20x) which tend to trigger NEED_RECAPTCHA,
# without requiring more harvesters (bandwidth blow-ups).
MAX_COOKIE_REUSE = int(os.environ.get("MAX_COOKIE_REUSE", "3") or "3")

# Minimum cooldown before reusing same proxy (seconds)
SESSION_COOLDOWN = 60.0

# Randomize tile order to avoid geographic patterns
RANDOMIZE_TILES = True

# =============================================================================
# Tile Retry / Defer Queue Configuration (Hardening v2)
# =============================================================================
# Maximum retries per tile before deferring to avoid blocking the run
MAX_RETRIES_PER_TILE = 3

# Seconds to wait before re-attempting deferred tiles
DEFER_COOLDOWN_SEC = 300  # 5 minutes

# =============================================================================
# Proxy CAPTCHA Cooling Configuration (Hardening v2)
# =============================================================================
# Number of immediate CAPTCHAs before marking proxy as BAD
PROXY_CAPTCHA_THRESHOLD = 2

# Hours to cool a BAD proxy before retrying
PROXY_COOLDOWN_HOURS = 6

# =============================================================================
# Geographic Configuration
# =============================================================================
# Continental US bounding box
US_BOUNDS = {
    "north": 49.384358,   # Northern border (Canada)
    "south": 24.396308,   # Southern border (Florida Keys)
    "east": -66.934570,   # Eastern border (Maine)
    "west": -124.848974,  # Western border (Washington State)
}

# Grid configuration for dividing US into tiles
# Smaller tiles = more API calls but better data granularity
# 
# HasMore Strategy: CellMapper caps responses at ~50 towers per tile.
# Dense metros (LA, NYC, Chicago) can have 100+ towers in a 0.5° tile,
# causing `hasMore=true` truncation. Using 0.25° reduces this dramatically
# while keeping request patterns geographically diverse (good for anti-bot).
#
# Trade-off: 0.25° = ~4x more tiles than 0.5° (~10,000 vs ~2,500 for US)
# but eliminates data loss from truncation in urban areas.
GRID_SIZE_LAT = 0.25   # Degrees latitude per tile (~28km)
GRID_SIZE_LON = 0.25   # Degrees longitude per tile (~20-25km depending on latitude)

# Zoom level for tile calculations (affects data density)
DEFAULT_ZOOM = 12

# =============================================================================
# Target Networks
# =============================================================================
# Carriers to scrape (keys must match networks.json)
TARGET_CARRIERS = ["verizon", "att", "tmobile"]

# =============================================================================
# Data Storage Configuration
# =============================================================================
# Output file formats
OUTPUT_FORMAT = "jsonl"  # Options: "jsonl", "csv", "sqlite"

# Output file paths (these are defaults; carrier-specific paths are set at runtime)
OUTPUT_FILES = {
    "jsonl": TOWERS_DIR / "towers.jsonl",
    "csv": TOWERS_DIR / "towers.csv",
    "sqlite": TOWERS_DIR / "towers.db",
}

# Progress tracking file (carrier-specific files are created at runtime)
PROGRESS_FILE = DATA_DIR / "progress.json"


def get_carrier_output_files(carrier: str, run_tag: str = "") -> dict:
    """Get carrier(+worker)-specific output file paths for parallel execution."""
    suffix = f"_{run_tag}" if run_tag else ""
    return {
        "jsonl": TOWERS_DIR / f"towers_{carrier}{suffix}.jsonl",
        "csv": TOWERS_DIR / f"towers_{carrier}{suffix}.csv",
        "sqlite": TOWERS_DIR / f"towers_{carrier}{suffix}.db",
    }

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_FILE = LOGS_DIR / "scraper.log"
LOG_LEVEL = "INFO"  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# =============================================================================
# Redis Configuration (Cookie Pool)
# =============================================================================
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# Cookie TTL in seconds (25 min default to stay under ~30 min session expiry)
COOKIE_TTL_SECONDS = int(os.environ.get("COOKIE_TTL_SECONDS", "1500"))

# =============================================================================
# Performance Tuning
# =============================================================================
# Number of concurrent connections (keep low to avoid bans)
MAX_CONCURRENT_REQUESTS = 1

# Session timeout (seconds)
REQUEST_TIMEOUT = 60

# Maximum towers per response (for pagination)
MAX_TOWERS_PER_REQUEST = 1000

# =============================================================================
# Debug Configuration
# =============================================================================
DEBUG_MODE = os.environ.get("DEBUG", "false").lower() == "true"
SAVE_RAW_RESPONSES = DEBUG_MODE  # Save raw API responses for debugging

# =============================================================================
# Browser Cookie Configuration
# =============================================================================
# CellMapper requires a valid session to avoid CAPTCHA
# Priority: 1) CELLMAPPER_COOKIES env var, 2) config/cookies.txt file
# Generate cookies with: python scripts/get_cookies.py

def _load_cookies() -> str:
    """Load cookies from env var or config file."""
    # Check environment variable first
    env_cookies = os.environ.get("CELLMAPPER_COOKIES", "")
    if env_cookies:
        return env_cookies
    
    # Fall back to config/cookies.txt
    cookie_file = CONFIG_DIR / "cookies.txt"
    if cookie_file.exists():
        try:
            return cookie_file.read_text().strip()
        except Exception:
            pass
    
    return ""

BROWSER_COOKIES = _load_cookies()
COOKIES_FILE = CONFIG_DIR / "cookies.txt"
# Example: "JSESSIONID=node0abc123...; visited=yes"

# =============================================================================
# Proxy Configuration (optional, for IP rotation)
# =============================================================================
USE_PROXY = False
PROXY_URL = os.environ.get("PROXY_URL", None)  # e.g., "socks5://user:pass@host:port"

# Proxy rotation settings
PROXY_ROTATION_ENABLED = False
PROXY_LIST_FILE = CONFIG_DIR / "proxies.txt"

# Optional: provide proxies via environment (newline or comma separated).
# This lets you run without managing config/proxies*.txt files.
PROXY_LIST = os.environ.get("PROXY_LIST", "").strip()

# Optional: generate proxies for a known provider (keeps existing file format working).
# Supported: "decodo" (Smartproxy/Decodo gateway + ports)
PROXY_PROVIDER = os.environ.get("PROXY_PROVIDER", "").lower().strip()

# Decodo / Smartproxy / "Decodo" gateway settings (example: gate.decodo.com)
DECODO_HOST = os.environ.get("DECODO_HOST", "gate.decodo.com").strip()
DECODO_USERNAME = os.environ.get("DECODO_USERNAME", "").strip()
DECODO_PASSWORD = os.environ.get("DECODO_PASSWORD", "").strip()
# Ports list/range, e.g. "10001-10030" or "10001,10002,10003"
DECODO_PORTS = os.environ.get("DECODO_PORTS", "").strip()

# Temporary failure cooling (non-CAPTCHA errors). Keeps bad exits from burning retries.
PROXY_FAIL_BASE_COOLDOWN_SEC = float(os.environ.get("PROXY_FAIL_BASE_COOLDOWN_SEC", "30"))
PROXY_FAIL_MAX_COOLDOWN_SEC = float(os.environ.get("PROXY_FAIL_MAX_COOLDOWN_SEC", "900"))

# Proxy selection strategy:
# - "round_robin": current behavior
# - "fastest": prefer lower-latency proxies (EMA), still respects cooldowns
PROXY_SELECTION_STRATEGY = os.environ.get("PROXY_SELECTION_STRATEGY", "round_robin").lower().strip()
PROXY_LATENCY_ALPHA = float(os.environ.get("PROXY_LATENCY_ALPHA", "0.2"))

# =============================================================================
# Email Notifications (Resend)
# =============================================================================
# API key from Resend dashboard - set as environment variable on server
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")

# Email address to receive alerts
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "info@shefa7.com")

# Enable notifications if API key is set
ALERT_ENABLED = bool(RESEND_API_KEY)

# Number of consecutive failures before sending warning email
WARNING_THRESHOLD = 5

# =============================================================================
# CAPTCHA Auto-Solve Configuration
# =============================================================================
# Provider: "2captcha" | "capsolver" | "capmonster" | "flaresolverr" | "" (disabled)
# - 2captcha: Human solvers, ~$1.50/1k, 15-30s solve time
# - capsolver: AI-powered, ~$1.20/1k, 5-12s (recommended)
# - capmonster: Premium provider, solid reliability
# - flaresolverr: Free, self-hosted Docker, 4-10s
CAPTCHA_PROVIDER = os.environ.get("CAPTCHA_PROVIDER", "")

# API key for 2Captcha, CapSolver, or CapMonster
CAPTCHA_API_KEY = os.environ.get("CAPTCHA_API_KEY", "")

# FlareSolverr URL (if using flaresolverr provider)
# Run: docker run -d -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://localhost:8191")

# NOTE: FLARESOLVERR_API_FALLBACK has been removed.
# Per-request browser spawns are now replaced by a central cookie-harvester pattern.
# Workers obtain cookies from Redis; the harvester uses FlareSolverr + CapMonster.

# Maximum attempts to solve CAPTCHA before giving up
CAPTCHA_MAX_ATTEMPTS = int(os.environ.get("CAPTCHA_MAX_ATTEMPTS", "3"))

# =============================================================================
# Worker Metrics Configuration
# =============================================================================
# Interval between periodic metrics snapshots (seconds)
# Set to 0 to disable automatic snapshot writing
METRICS_INTERVAL_SEC = 300  # 5 minutes

# Threshold for considering a worker "stalled" (no tile completions)
STALL_THRESHOLD_SEC = 600  # 10 minutes

# =============================================================================
# Proxy Bandwidth Tracking
# =============================================================================
# Track bytes sent/received per proxy endpoint (stored in Redis)
ENABLE_PROXY_BYTES = os.environ.get("ENABLE_PROXY_BYTES", "false").lower() == "true"

