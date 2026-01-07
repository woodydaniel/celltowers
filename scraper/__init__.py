"""
CellMapper Scraper Package

A Python package for scraping cell tower data from CellMapper.net.
For personal/research use only.
"""

from .api_client import (
    CellMapperClient,
    CaptchaRequiredError,
    CellMapperError,
    RateLimitedError,
    ProxyBlockedError,
)
from .geo_utils import GeoGrid, USBounds
from .parser import TowerParser, BoundedSet
from .storage import DataStorage
from .proxy_manager import ProxyManager, Proxy
from .session_pool import SessionPool
from .cookie_manager import CookieManager, get_cookie_manager

__version__ = "1.0.0"
__all__ = [
    # Client
    "CellMapperClient",
    # Exceptions
    "CellMapperError",
    "CaptchaRequiredError", 
    "RateLimitedError",
    "ProxyBlockedError",
    # Utilities
    "GeoGrid", 
    "USBounds", 
    "TowerParser",
    "BoundedSet",
    "DataStorage",
    "ProxyManager",
    "Proxy",
    "SessionPool",
    "CookieManager",
    "get_cookie_manager",
]

