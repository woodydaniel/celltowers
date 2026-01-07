#!/usr/bin/env python3
"""
Metrics Snapshot Tool - Captures point-in-time metrics for experiment analysis.

This script captures all relevant Redis counters and bandwidth metrics at a specific
moment in time. Used for before/after comparison during systematic testing.

Usage:
    python scripts/snapshot_metrics.py --label "test_a_start"
    python scripts/snapshot_metrics.py --label "test_a_end"
    python scripts/snapshot_metrics.py --show-all  # List all snapshots

Snapshots are stored in Redis and can be compared to calculate deltas.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Optional

import redis.asyncio as redis

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("snapshot_metrics")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# All counter keys we track
COUNTER_KEYS = [
    "cellmapper:counters:api_requests_ok",
    "cellmapper:counters:api_need_recaptcha",
    "cellmapper:counters:cookie_reuse_limit",
    "cellmapper:counters:harvest_success",
    "cellmapper:counters:harvest_rejected",
    "cellmapper:counters:harvest_validated",
    "cellmapper:counters:turnstile_hits",
    "cellmapper:counters:cookies_checked_out",
]

# Snapshot storage key prefix
SNAPSHOT_PREFIX = "experiment:snapshot:"
EXPERIMENT_META_KEY = "experiment:meta"


async def get_counter_value(r: redis.Redis, key: str) -> int:
    """Get a counter value, returning 0 if not set."""
    try:
        val = await r.get(key)
        return int(val) if val else 0
    except Exception:
        return 0


async def get_all_bandwidth_totals(r: redis.Redis) -> dict[str, int]:
    """
    Get bandwidth totals from all proxy:bytes:* and harvester:bytes:* keys.
    
    Returns dict with:
        - proxy_total_bytes: Total worker proxy bandwidth
        - harvester_total_bytes: Total harvester bandwidth (if tracked separately)
        - per_day breakdown
    """
    result = {
        "proxy_total_bytes": 0,
        "harvester_total_bytes": 0,
        "proxy_bytes_by_day": {},
        "harvester_bytes_by_day": {},
    }
    
    # Worker proxy bandwidth
    proxy_keys = await r.keys("proxy:bytes:*")
    for key in proxy_keys:
        try:
            day = key.split(":")[-1]
            hash_data = await r.hgetall(key)
            day_total = sum(int(v) for v in hash_data.values() if v)
            result["proxy_bytes_by_day"][day] = day_total
            result["proxy_total_bytes"] += day_total
        except Exception as e:
            logger.debug(f"Error reading {key}: {e}")
    
    # Harvester bandwidth (if tracked separately)
    harvester_keys = await r.keys("harvester:bytes:*")
    for key in harvester_keys:
        try:
            day = key.split(":")[-1]
            hash_data = await r.hgetall(key)
            day_total = sum(int(v) for v in hash_data.values() if v)
            result["harvester_bytes_by_day"][day] = day_total
            result["harvester_total_bytes"] += day_total
        except Exception as e:
            logger.debug(f"Error reading {key}: {e}")
    
    return result


async def get_cookie_pool_stats(r: redis.Redis) -> dict[str, Any]:
    """Get current cookie pool statistics."""
    cookie_keys = await r.keys("cellmapper:cookie:*")
    count = len(cookie_keys)
    
    # Sample a few cookies for age stats
    ages = []
    for key in cookie_keys[:10]:  # Sample up to 10
        try:
            val = await r.get(key)
            if val:
                data = json.loads(val)
                created_at = data.get("created_at")
                if created_at:
                    age = datetime.now().timestamp() - created_at
                    ages.append(age)
        except Exception:
            pass
    
    return {
        "pool_size": count,
        "avg_cookie_age_seconds": sum(ages) / len(ages) if ages else 0,
        "oldest_cookie_seconds": max(ages) if ages else 0,
    }


async def capture_snapshot(r: redis.Redis, label: str) -> dict[str, Any]:
    """
    Capture a complete metrics snapshot.
    
    Returns the snapshot data.
    """
    timestamp = datetime.now().isoformat()
    
    # Gather all metrics
    counters = {}
    for key in COUNTER_KEYS:
        counters[key] = await get_counter_value(r, key)
    
    bandwidth = await get_all_bandwidth_totals(r)
    pool_stats = await get_cookie_pool_stats(r)
    
    snapshot = {
        "label": label,
        "timestamp": timestamp,
        "unix_time": datetime.now().timestamp(),
        "counters": counters,
        "bandwidth": bandwidth,
        "cookie_pool": pool_stats,
    }
    
    # Store in Redis
    snapshot_key = f"{SNAPSHOT_PREFIX}{label}"
    await r.set(snapshot_key, json.dumps(snapshot, indent=2))
    
    logger.info(f"Snapshot '{label}' captured at {timestamp}")
    
    return snapshot


def format_bytes(b: int) -> str:
    """Format bytes as human-readable string."""
    if b < 1024:
        return f"{b} B"
    elif b < 1024 * 1024:
        return f"{b / 1024:.2f} KB"
    elif b < 1024 * 1024 * 1024:
        return f"{b / (1024 * 1024):.2f} MB"
    else:
        return f"{b / (1024 * 1024 * 1024):.2f} GB"


def print_snapshot(snapshot: dict[str, Any]) -> None:
    """Pretty-print a snapshot."""
    print(f"\n{'=' * 60}")
    print(f"Snapshot: {snapshot['label']}")
    print(f"Time: {snapshot['timestamp']}")
    print(f"{'=' * 60}")
    
    print("\nCounters:")
    for key, val in snapshot["counters"].items():
        short_key = key.replace("cellmapper:counters:", "")
        print(f"  {short_key}: {val:,}")
    
    # Calculate CAPTCHA rate
    ok = snapshot["counters"].get("cellmapper:counters:api_requests_ok", 0)
    captcha = snapshot["counters"].get("cellmapper:counters:api_need_recaptcha", 0)
    total = ok + captcha
    rate = (captcha / total * 100) if total > 0 else 0
    print(f"\n  CAPTCHA rate: {rate:.2f}% ({captcha}/{total})")
    
    print("\nBandwidth:")
    bw = snapshot["bandwidth"]
    print(f"  Proxy (workers): {format_bytes(bw['proxy_total_bytes'])}")
    print(f"  Harvester: {format_bytes(bw['harvester_total_bytes'])}")
    print(f"  Total: {format_bytes(bw['proxy_total_bytes'] + bw['harvester_total_bytes'])}")
    
    print("\nCookie Pool:")
    pool = snapshot["cookie_pool"]
    print(f"  Current size: {pool['pool_size']}")
    print(f"  Avg cookie age: {pool['avg_cookie_age_seconds']:.1f}s")


async def list_snapshots(r: redis.Redis) -> list[str]:
    """List all stored snapshots."""
    keys = await r.keys(f"{SNAPSHOT_PREFIX}*")
    labels = [k.replace(SNAPSHOT_PREFIX, "") for k in keys]
    return sorted(labels)


async def get_snapshot(r: redis.Redis, label: str) -> Optional[dict[str, Any]]:
    """Get a stored snapshot by label."""
    key = f"{SNAPSHOT_PREFIX}{label}"
    data = await r.get(key)
    if data:
        return json.loads(data)
    return None


async def compare_snapshots(r: redis.Redis, label1: str, label2: str) -> None:
    """Compare two snapshots and print deltas."""
    snap1 = await get_snapshot(r, label1)
    snap2 = await get_snapshot(r, label2)
    
    if not snap1 or not snap2:
        print(f"Error: Could not find snapshots '{label1}' and/or '{label2}'")
        return
    
    print(f"\n{'=' * 70}")
    print(f"Comparison: {label1} → {label2}")
    print(f"Duration: {(snap2['unix_time'] - snap1['unix_time']) / 60:.1f} minutes")
    print(f"{'=' * 70}")
    
    # Counter deltas
    print("\n| Metric | Before | After | Delta |")
    print("|--------|--------|-------|-------|")
    
    for key in COUNTER_KEYS:
        short_key = key.replace("cellmapper:counters:", "")
        v1 = snap1["counters"].get(key, 0)
        v2 = snap2["counters"].get(key, 0)
        delta = v2 - v1
        print(f"| {short_key} | {v1:,} | {v2:,} | {delta:+,} |")
    
    # CAPTCHA rate
    ok1 = snap1["counters"].get("cellmapper:counters:api_requests_ok", 0)
    ok2 = snap2["counters"].get("cellmapper:counters:api_requests_ok", 0)
    captcha1 = snap1["counters"].get("cellmapper:counters:api_need_recaptcha", 0)
    captcha2 = snap2["counters"].get("cellmapper:counters:api_need_recaptcha", 0)
    
    delta_ok = ok2 - ok1
    delta_captcha = captcha2 - captcha1
    delta_total = delta_ok + delta_captcha
    
    rate = (delta_captcha / delta_total * 100) if delta_total > 0 else 0
    print(f"\n**CAPTCHA rate during test: {rate:.2f}%** ({delta_captcha} / {delta_total})")
    
    # Bandwidth deltas
    bw1 = snap1["bandwidth"]
    bw2 = snap2["bandwidth"]
    
    proxy_delta = bw2["proxy_total_bytes"] - bw1["proxy_total_bytes"]
    harvester_delta = bw2["harvester_total_bytes"] - bw1["harvester_total_bytes"]
    total_delta = proxy_delta + harvester_delta
    
    print(f"\nBandwidth used during test:")
    print(f"  Proxy (workers): {format_bytes(proxy_delta)}")
    print(f"  Harvester: {format_bytes(harvester_delta)}")
    print(f"  Total: {format_bytes(total_delta)}")
    
    # Calculate cost (Decodo is ~$2/GB)
    cost_per_gb = 2.0
    cost = (total_delta / (1024 * 1024 * 1024)) * cost_per_gb
    print(f"\n  Estimated cost: ${cost:.4f} (at ${cost_per_gb}/GB)")
    
    # Per-cookie metrics
    cookies_minted = delta_ok if delta_ok > 0 else 1  # Avoid division by zero
    harvest_success = snap2["counters"].get("cellmapper:counters:harvest_success", 0) - \
                      snap1["counters"].get("cellmapper:counters:harvest_success", 0)
    
    if harvest_success > 0:
        bw_per_cookie = harvester_delta / harvest_success
        print(f"\n  Harvester bandwidth per cookie: {format_bytes(int(bw_per_cookie))}")


async def set_experiment_meta(r: redis.Redis, test_name: str, config_snapshot: str) -> None:
    """Store experiment metadata."""
    await r.hset(EXPERIMENT_META_KEY, mapping={
        "start_time": datetime.now().isoformat(),
        "test_name": test_name,
        "config_snapshot": config_snapshot,
    })
    logger.info(f"Experiment meta set: {test_name}")


async def main():
    parser = argparse.ArgumentParser(description="Capture and compare metrics snapshots")
    parser.add_argument("--label", type=str, help="Label for this snapshot (e.g., 'test_a_start')")
    parser.add_argument("--show-all", action="store_true", help="List all stored snapshots")
    parser.add_argument("--show", type=str, help="Show a specific snapshot by label")
    parser.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"),
                        help="Compare two snapshots")
    parser.add_argument("--set-experiment", type=str, help="Set current experiment name")
    parser.add_argument("--config", type=str, default="", help="Config snapshot to store with experiment")
    
    args = parser.parse_args()
    
    r = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    
    try:
        await r.ping()
        
        if args.show_all:
            labels = await list_snapshots(r)
            if labels:
                print("Stored snapshots:")
                for label in labels:
                    snap = await get_snapshot(r, label)
                    if snap:
                        print(f"  - {label} ({snap['timestamp']})")
            else:
                print("No snapshots found.")
        
        elif args.show:
            snap = await get_snapshot(r, args.show)
            if snap:
                print_snapshot(snap)
            else:
                print(f"Snapshot '{args.show}' not found.")
        
        elif args.compare:
            await compare_snapshots(r, args.compare[0], args.compare[1])
        
        elif args.set_experiment:
            await set_experiment_meta(r, args.set_experiment, args.config)
        
        elif args.label:
            snap = await capture_snapshot(r, args.label)
            print_snapshot(snap)
        
        else:
            parser.print_help()
    
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())

