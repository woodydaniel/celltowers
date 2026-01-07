"""
Tests for CAPTCHA solving providers and integration.

Run: python -m pytest tests/test_captcha.py -v
"""

import asyncio
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestCaptchaProviders:
    """Tests for individual CAPTCHA provider implementations."""
    
    def test_get_provider_returns_none_when_not_configured(self):
        """Should return None when no provider is configured."""
        with patch.dict(os.environ, {}, clear=True):
            # Clear any existing env vars
            for key in ["CAPTCHA_PROVIDER", "CAPTCHA_API_KEY", "FLARESOLVERR_URL"]:
                os.environ.pop(key, None)
            
            from scraper.captcha.providers import get_captcha_provider
            provider = get_captcha_provider()
            assert provider is None
    
    def test_get_provider_2captcha(self):
        """Should return TwoCaptchaProvider when configured."""
        with patch.dict(os.environ, {
            "CAPTCHA_PROVIDER": "2captcha",
            "CAPTCHA_API_KEY": "test_key_123",
        }):
            from scraper.captcha.providers import get_captcha_provider, TwoCaptchaProvider
            provider = get_captcha_provider()
            assert isinstance(provider, TwoCaptchaProvider)
            assert provider.api_key == "test_key_123"
    
    def test_get_provider_capsolver(self):
        """Should return CapSolverProvider when configured."""
        with patch.dict(os.environ, {
            "CAPTCHA_PROVIDER": "capsolver",
            "CAPTCHA_API_KEY": "CAP-test_key",
        }):
            from scraper.captcha.providers import get_captcha_provider, CapSolverProvider
            provider = get_captcha_provider()
            assert isinstance(provider, CapSolverProvider)
            assert provider.api_key == "CAP-test_key"
    
    def test_get_provider_flaresolverr(self):
        """Should return FlareSolverrProvider when configured."""
        with patch.dict(os.environ, {
            "CAPTCHA_PROVIDER": "flaresolverr",
            "FLARESOLVERR_URL": "http://myserver:8191",
        }):
            from scraper.captcha.providers import get_captcha_provider, FlareSolverrProvider
            provider = get_captcha_provider()
            assert isinstance(provider, FlareSolverrProvider)
            assert provider.url == "http://myserver:8191"
    
    def test_get_provider_requires_api_key(self):
        """Should return None when API key is missing for token providers."""
        with patch.dict(os.environ, {
            "CAPTCHA_PROVIDER": "capsolver",
            "CAPTCHA_API_KEY": "",
        }):
            from scraper.captcha.providers import get_captcha_provider
            provider = get_captcha_provider()
            assert provider is None


class TestTwoCaptchaProvider:
    """Tests for 2Captcha provider."""
    
    @pytest.mark.asyncio
    async def test_solve_success(self):
        """Should successfully solve captcha with mocked API."""
        from scraper.captcha.providers import TwoCaptchaProvider
        
        provider = TwoCaptchaProvider("test_api_key")
        
        # Mock the HTTP session
        mock_http = AsyncMock()
        provider._http = mock_http
        
        # Mock submit response
        submit_response = AsyncMock()
        submit_response.json = AsyncMock(return_value={"status": 1, "request": "task123"})
        
        # Mock result response (first not ready, then ready)
        result_not_ready = AsyncMock()
        result_not_ready.json = AsyncMock(return_value={"status": 0, "request": "CAPCHA_NOT_READY"})
        
        result_ready = AsyncMock()
        result_ready.json = AsyncMock(return_value={"status": 1, "request": "solved_token_abc123"})
        
        mock_http.get = AsyncMock(side_effect=[
            AsyncMock(__aenter__=AsyncMock(return_value=submit_response)),
            AsyncMock(__aenter__=AsyncMock(return_value=result_not_ready)),
            AsyncMock(__aenter__=AsyncMock(return_value=result_ready)),
        ])
        
        token = await provider.solve(
            site_key="test_sitekey",
            page_url="https://example.com",
            captcha_type="hcaptcha"
        )
        
        assert token == "solved_token_abc123"


class TestCapSolverProvider:
    """Tests for CapSolver provider."""
    
    @pytest.mark.asyncio
    async def test_solve_success(self):
        """Should successfully solve captcha with mocked API."""
        from scraper.captcha.providers import CapSolverProvider
        
        provider = CapSolverProvider("CAP-test_key")
        
        # Mock the HTTP session
        mock_http = AsyncMock()
        provider._http = mock_http
        
        # Mock create task response
        create_response = AsyncMock()
        create_response.json = AsyncMock(return_value={"errorId": 0, "taskId": "task456"})
        
        # Mock result response
        result_response = AsyncMock()
        result_response.json = AsyncMock(return_value={
            "errorId": 0,
            "status": "ready",
            "solution": {"gRecaptchaResponse": "capsolver_token_xyz"}
        })
        
        mock_http.post = AsyncMock(side_effect=[
            AsyncMock(__aenter__=AsyncMock(return_value=create_response)),
            AsyncMock(__aenter__=AsyncMock(return_value=result_response)),
        ])
        
        token = await provider.solve(
            site_key="test_sitekey",
            page_url="https://example.com",
            captcha_type="hcaptcha"
        )
        
        assert token == "capsolver_token_xyz"


class TestFlareSolverrProvider:
    """Tests for FlareSolverr provider."""
    
    @pytest.mark.asyncio
    async def test_get_cookies_success(self):
        """Should successfully get cookies via FlareSolverr."""
        from scraper.captcha.providers import FlareSolverrProvider
        
        provider = FlareSolverrProvider("http://localhost:8191")
        
        # Mock the HTTP session
        mock_http = AsyncMock()
        provider._http = mock_http
        
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "status": "ok",
            "solution": {
                "cookies": [
                    {"name": "JSESSIONID", "value": "node0abc123"},
                    {"name": "visited", "value": "true"},
                ]
            }
        })
        
        mock_http.post = AsyncMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_response)
        ))
        
        cookies = await provider.get_cookies("https://www.cellmapper.net/map")
        
        assert "JSESSIONID=node0abc123" in cookies
        assert "visited=true" in cookies
    
    @pytest.mark.asyncio
    async def test_solve_raises_error(self):
        """FlareSolverr should raise error when solve() is called directly."""
        from scraper.captcha.providers import FlareSolverrProvider, CaptchaSolveError
        
        provider = FlareSolverrProvider("http://localhost:8191")
        
        with pytest.raises(CaptchaSolveError):
            await provider.solve("sitekey", "https://example.com")


class TestCookieManagerIntegration:
    """Integration tests for CookieManager with CAPTCHA solving."""
    
    def test_cookie_manager_loads_provider(self):
        """CookieManager should lazy-load configured provider."""
        with patch.dict(os.environ, {
            "CAPTCHA_PROVIDER": "capsolver",
            "CAPTCHA_API_KEY": "CAP-test",
        }):
            from scraper.cookie_manager import CookieManager
            
            manager = CookieManager()
            
            # Provider should not be loaded yet
            assert manager._captcha_provider_loaded is False
            
            # Get provider (lazy load)
            provider = manager._get_captcha_provider()
            
            assert manager._captcha_provider_loaded is True
            assert provider is not None
    
    def test_cookie_manager_no_provider_configured(self):
        """CookieManager should work without CAPTCHA provider."""
        with patch.dict(os.environ, {"CAPTCHA_PROVIDER": ""}):
            from scraper.cookie_manager import CookieManager
            
            manager = CookieManager()
            provider = manager._get_captcha_provider()
            
            assert provider is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

