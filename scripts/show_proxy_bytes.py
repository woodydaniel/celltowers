#!/usr/bin/env python3
"""
Display proxy bandwidth usage from Redis metrics.

Usage:
    python scripts/show_proxy_bytes.py [--date YYYY-MM-DD] [--all]

Examples:
    python scripts/show_proxy_bytes.py              # Show today's usage
    python scripts/show_proxy_bytes.py --all        # Show all days
    python scripts/show_proxy_bytes.py --date 2025-12-22
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import redis.asyncio as redis
from config.settings import REDIS_URL


def format_bytes(bytes_value: float) -> str:
    """Format bytes as human-readable string."""
    if bytes_value < 1024:
        return f"{bytes_value:.2f} B"
    elif bytes_value < 1024 ** 2:
        return f"{bytes_value / 1024:.2f} KB"
    elif bytes_value < 1024 ** 3:
        return f"{bytes_value / (1024 ** 2):.2f} MB"
    else:
        return f"{bytes_value / (1024 ** 3):.2f} GB"


async def get_bandwidth_for_date(r: redis.Redis, date_str: str) -> dict:
    """
    Get bandwidth data for a specific date.
    
    Returns:
        dict: {proxy_endpoint: {'sent': bytes, 'recv': bytes, 'total': bytes}}
    """
    redis_key = f"proxy:bytes:{date_str}"
    
    try:
        data = await r.hgetall(redis_key)
        if not data:
            return {}
        
        # Parse the data
        proxies = {}
        for field, value in data.items():
            parts = field.split(":")
            if len(parts) < 3:
                continue
            
            endpoint = ":".join(parts[:-1])  # host:port
            direction = parts[-1]  # 'sent' or 'recv'
            
            if endpoint not in proxies:
                proxies[endpoint] = {"sent": 0.0, "recv": 0.0, "total": 0.0}
            
            bytes_value = float(value)
            proxies[endpoint][direction] = bytes_value
            proxies[endpoint]["total"] += bytes_value
        
        return proxies
    except Exception as e:
        print(f"Error fetching data for {date_str}: {e}")
        return {}


async def show_bandwidth(date_str: str = None, show_all: bool = False):
    """Display bandwidth usage."""
    r = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    
    try:
        await r.ping()
    except Exception as e:
        print(f"Error connecting to Redis at {REDIS_URL}: {e}")
        return
    
    try:
        if show_all:
            # Get all proxy:bytes:* keys
            keys = await r.keys("proxy:bytes:*")
            dates = sorted([k.split(":")[-1] for k in keys])
            
            if not dates:
                print("No bandwidth data found in Redis.")
                return
            
            print("=" * 80)
            print("PROXY BANDWIDTH USAGE - ALL DATES")
            print("=" * 80)
            print()
            
            grand_total_sent = 0.0
            grand_total_recv = 0.0
            
            for date in dates:
                proxies = await get_bandwidth_for_date(r, date)
                if not proxies:
                    continue
                
                total_sent = sum(p["sent"] for p in proxies.values())
                total_recv = sum(p["recv"] for p in proxies.values())
                total = total_sent + total_recv
                
                grand_total_sent += total_sent
                grand_total_recv += total_recv
                
                print(f"Date: {date}")
                print(f"  Sent:     {format_bytes(total_sent)}")
                print(f"  Received: {format_bytes(total_recv)}")
                print(f"  Total:    {format_bytes(total)}")
                print(f"  Proxies:  {len(proxies)}")
                print()
            
            print("=" * 80)
            print(f"GRAND TOTAL:")
            print(f"  Sent:     {format_bytes(grand_total_sent)}")
            print(f"  Received: {format_bytes(grand_total_recv)}")
            print(f"  Total:    {format_bytes(grand_total_sent + grand_total_recv)}")
            print("=" * 80)
        
        else:
            # Show specific date or today
            if not date_str:
                date_str = datetime.utcnow().strftime("%Y-%m-%d")
            
            proxies = await get_bandwidth_for_date(r, date_str)
            
            if not proxies:
                print(f"No bandwidth data found for {date_str}")
                return
            
            # Sort by total bandwidth descending
            sorted_proxies = sorted(
                proxies.items(),
                key=lambda x: x[1]["total"],
                reverse=True
            )
            
            total_sent = sum(p["sent"] for p in proxies.values())
            total_recv = sum(p["recv"] for p in proxies.values())
            total = total_sent + total_recv
            
            print("=" * 80)
            print(f"PROXY BANDWIDTH USAGE - {date_str}")
            print("=" * 80)
            print()
            print(f"Total Sent:     {format_bytes(total_sent)}")
            print(f"Total Received: {format_bytes(total_recv)}")
            print(f"Total:          {format_bytes(total)}")
            print(f"Proxies Used:   {len(proxies)}")
            print()
            print("=" * 80)
            print(f"{'Proxy Endpoint':<40} {'Sent':<15} {'Recv':<15} {'Total':<15}")
            print("=" * 80)
            
            for endpoint, stats in sorted_proxies:
                print(
                    f"{endpoint:<40} "
                    f"{format_bytes(stats['sent']):<15} "
                    f"{format_bytes(stats['recv']):<15} "
                    f"{format_bytes(stats['total']):<15}"
                )
            
            print("=" * 80)
            
            # Show top 5 consumers
            if len(sorted_proxies) > 5:
                print()
                print("TOP 5 BANDWIDTH CONSUMERS:")
                print("-" * 80)
                for i, (endpoint, stats) in enumerate(sorted_proxies[:5], 1):
                    pct = (stats["total"] / total * 100) if total > 0 else 0
                    print(
                        f"{i}. {endpoint}: {format_bytes(stats['total'])} "
                        f"({pct:.1f}% of total)"
                    )
                print("-" * 80)
    
    finally:
        await r.aclose()


def main():
    parser = argparse.ArgumentParser(
        description="Display proxy bandwidth usage from Redis metrics"
    )
    parser.add_argument(
        "--date",
        help="Date to display (YYYY-MM-DD), defaults to today",
        default=None
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Show all available dates"
    )
    
    args = parser.parse_args()
    
    asyncio.run(show_bandwidth(date_str=args.date, show_all=args.all))


if __name__ == "__main__":
    main()








