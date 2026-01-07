"""
CAPTCHA Solving Module

Provides pluggable CAPTCHA solving via:
- 2Captcha (token-based, paid)
- CapSolver (token-based, paid, fastest)
- FlareSolverr (headful browser, free, requires Docker)

Usage:
    from scraper.captcha import get_captcha_provider
    
    provider = get_captcha_provider()
    if provider:
        token = await provider.solve(site_key, page_url)
"""

from .providers import (
    CaptchaProvider,
    TwoCaptchaProvider,
    CapSolverProvider,
    CapMonsterProvider,
    FlareSolverrProvider,
    get_captcha_provider,
)

__all__ = [
    "CaptchaProvider",
    "TwoCaptchaProvider",
    "CapSolverProvider",
    "CapMonsterProvider",
    "FlareSolverrProvider",
    "get_captcha_provider",
]

