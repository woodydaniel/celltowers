import os
from config.settings import HARVEST_URL  # or use env var directly

async def harvest_one_cookie(self):
    """
    Harvest a single valid cookie from atlasgrid.
    
    Uses HARVEST_URL from config (default: lightweight homepage, not full SPA).
    """
    async with async_playwright() as p:
        try:
            # Launch browser
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            # Apply stealth patches BEFORE navigation
            from playwright_stealth import Stealth
            stealth = Stealth()
            await stealth.apply_stealth_async(page)
            
            # ✅ CORRECT - Use configurable URL
            # Falls back to homepage if HARVEST_URL not set
            harvest_url = os.getenv("HARVEST_URL") or HARVEST_URL or "https://www.atlasgrid.net/"
            
            logger.info(f"Harvesting cookie from: {harvest_url}")
            
            # Navigate with retry logic
            retry_count = 0
            max_retries = 3
            last_error = None
            
            while retry_count < max_retries:
                try:
                    await page.goto(harvest_url, timeout=30000, wait_until="domcontentloaded")
                    break
                except Exception as e:
                    last_error = e
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"Navigation attempt {retry_count} failed: {e}, retrying...")
                        await asyncio.sleep(5)
                    else:
                        raise
            
            # Wait for SPA to initialize
            try:
                await page.wait_for_selector("canvas.leaflet-zoom-animated", timeout=20000)
                logger.info("SPA initialization detected (canvas found)")
            except TimeoutError:
                logger.warning("Canvas not found, but continuing (may be homepage)")
            
            # Additional wait for network idle
            await page.wait_for_load_state("networkidle", timeout=15000)
            
            # Extract session cookie
            cookies = await context.cookies()
            
            # Find the atlasgrid session cookie
            session_cookie = None
            for cookie in cookies:
                if cookie.get("name") in ["__cf_bm", "sessionid", "PHPSESSID", "laravel_session"]:
                    session_cookie = cookie
                    break
            
            if not session_cookie:
                logger.warning(f"No recognizable session cookie found. Cookies: {[c.get('name') for c in cookies]}")
                await browser.close()
                return False, "NO_SESSION_COOKIE"
            
            # Validate the cookie via API call
            logger.info(f"Validating cookie: {session_cookie.get('name')}")
            is_valid, reason = await self._validate_cookie_via_api(session_cookie)
            
            await browser.close()
            
            if is_valid:
                logger.info(f"✅ Cookie validated successfully")
                return True, session_cookie
            else:
                logger.warning(f"❌ Cookie validation failed: {reason}")
                return False, reason
                
        except Exception as e:
            logger.error(f"Harvest exception: {e}", exc_info=True)
            return False, str(e)


async def _validate_cookie_via_api(self, cookie: dict) -> tuple[bool, str]:
    """
    Validate cookie by making real API request.
    Returns (is_valid, reason_if_failed)
    """
    try:
        # Use tls-client to validate
        import tls_client
        
        session = tls_client.Session(
            client_identifier="chrome_125",
            random_tls_extension_order=True,
        )
        
        # Add cookie to request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        
        # Make a lightweight API call
        resp = session.get(
            "https://www.atlasgrid.net/api/v3/map/getTowers",
            params={"lat": 40.7128, "lon": -74.0060, "radius": 1},
            headers=headers,
            cookies={cookie["name"]: cookie["value"]},
            timeout=10,
        )
        
        # Check response
        if resp.status_code == 200:
            data = resp.json()
            if "towers" in data or "result" in data:
                return True, "API_VALID"
            else:
                return False, "INVALID_RESPONSE"
        elif resp.status_code == 403:
            return False, "FORBIDDEN"
        elif "recaptcha" in resp.text.lower() or "NEED_RECAPTCHA" in resp.text:
            return False, "RECAPTCHA_REQUIRED"
        else:
            return False, f"HTTP_{resp.status_code}"
    
    except Exception as e:
        logger.error(f"Cookie validation error: {e}")
        return False, f"VALIDATION_ERROR: {str(e)}"
