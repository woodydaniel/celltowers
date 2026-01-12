#!/usr/bin/env python3
"""
Simple Cookie Saver - No browser automation needed.

Just paste your cookies from browser DevTools and this saves them
to the config file for the scraper to use.

Usage:
    python scripts/save_cookies.py "JSESSIONID=xxx; visited=yes; ..."
    
Or interactively:
    python scripts/save_cookies.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
COOKIES_FILE = CONFIG_DIR / "cookies.txt"


def save_cookies(cookie_string: str) -> None:
    """Save cookie string to config file."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Clean up the cookie string
    cookie_string = cookie_string.strip()
    
    # Save to file
    COOKIES_FILE.write_text(cookie_string)
    print(f"✓ Saved cookies to: {COOKIES_FILE}")
    print(f"  Length: {len(cookie_string)} characters")
    
    # Show what was saved
    cookies = {}
    for item in cookie_string.split(";"):
        if "=" in item:
            key, val = item.strip().split("=", 1)
            cookies[key] = val
    
    print(f"  Cookies found: {len(cookies)}")
    for key in ["JSESSIONID", "visited", "__utma"]:
        if key in cookies:
            print(f"    ✓ {key}")


def main():
    if len(sys.argv) > 1:
        # Cookies passed as argument
        cookie_string = " ".join(sys.argv[1:])
    else:
        # Interactive mode
        print("Paste your cookies from browser DevTools (Cookie header value):")
        print("(Press Enter twice when done)")
        lines = []
        while True:
            line = input()
            if not line:
                break
            lines.append(line)
        cookie_string = " ".join(lines)
    
    if not cookie_string.strip():
        print("ERROR: No cookies provided")
        sys.exit(1)
    
    save_cookies(cookie_string)
    print("\nNow run: python test_run.py")


if __name__ == "__main__":
    main()











