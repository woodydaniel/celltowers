#!/usr/bin/env python3
"""
Push a manually obtained CellMapper cookie into the Redis cookie pool.

This is intended for validating the Harvester/Worker architecture without
automating CAPTCHA solving or challenge bypass.

Usage:
  source venv/bin/activate
  python scripts/push_cookie_to_pool.py --cookie "a=b; c=d" --ua "Mozilla/5.0 ..."

Environment:
  REDIS_URL (default: redis://localhost:6379/0)
  COOKIE_TTL_SECONDS (default: 1500)
"""

from __future__ import annotations

import argparse
import asyncio
import sys


def _parse_cookie_string(cookie_string: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for item in (cookie_string or "").split(";"):
        item = item.strip()
        if "=" in item:
            k, v = item.split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies


async def _validate_cookie_string(cookie_string: str) -> bool:
    # Lightweight probe to ensure we don't poison the pool with a dead cookie.
    from scraper.api_client import CellMapperClient

    async with CellMapperClient(
        cookies=cookie_string,
        use_proxy=False,
        proxy_url=None,
        rotate_proxies=False,
        fast_mode=False,
        cookie_manager=None,
    ) as cm:
        resp = await cm.get_frequency(channel=1, mcc=310, mnc=260)

    if not resp.success:
        return False
    if isinstance(resp.data, dict) and resp.data.get("statusCode") == "NEED_RECAPTCHA":
        return False
    return True


async def main() -> int:
    parser = argparse.ArgumentParser(description="Push cookie into Redis cookie pool")
    parser.add_argument("--cookie", required=True, help="Cookie header string, e.g. 'a=b; c=d'")
    parser.add_argument(
        "--ua",
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        help="User-Agent string to associate with this cookie",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip API validation (unsafe; may poison workers)",
    )
    args = parser.parse_args()

    cookies = _parse_cookie_string(args.cookie)
    if not cookies:
        print("No valid cookies parsed from --cookie", file=sys.stderr)
        return 2

    if not args.no_validate:
        ok = await _validate_cookie_string(args.cookie)
        if not ok:
            print("cookie rejected by API (NEED_RECAPTCHA); not storing", file=sys.stderr)
            return 3

    from scraper.cookie_pool import CookiePool

    pool = CookiePool()
    await pool.connect()
    key = await pool.put(cookies=cookies, user_agent=args.ua, validated=True)
    count = await pool.count()
    await pool.close()

    print(f"stored_key={key}")
    print(f"pool_count={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))


