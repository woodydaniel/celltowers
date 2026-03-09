#!/usr/bin/env python3
"""
CellMapper Tower Scraper - Main Entry Point

Scrapes cell tower data from CellMapper.net for US carriers:
- Verizon
- AT&T  
- T-Mobile

For personal/research use only. See cellmapper.net/TOS for terms.

Usage:
    python main.py                    # Full US scrape, all carriers
    python main.py --test             # Test mode (small area)
    python main.py --carrier tmobile  # Single carrier only
    python main.py --resume           # Resume from saved progress
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))


# =============================================================================
# Graceful Shutdown Handling
# =============================================================================

class ShutdownHandler:
    """Handles graceful shutdown on SIGTERM/SIGINT."""
    
    def __init__(self):
        self.shutdown_requested = False
        self._original_handlers = {}
    
    def setup(self) -> None:
        """Install signal handlers."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            self._original_handlers[sig] = signal.signal(sig, self._handle_signal)
    
    def _handle_signal(self, signum, frame) -> None:
        """Handle shutdown signal."""
        sig_name = signal.Signals(signum).name
        if not self.shutdown_requested:
            logging.getLogger(__name__).warning(
                f"Received {sig_name} - finishing current tile and saving progress..."
            )
            self.shutdown_requested = True
            # Best-effort: persist progress immediately so a rapid stop/restart
            # doesn't risk losing recent state.
            global _shutdown_save_progress
            if _shutdown_save_progress is not None:
                try:
                    _shutdown_save_progress()
                    logging.getLogger(__name__).info("Progress saved on signal")
                except Exception as e:
                    logging.getLogger(__name__).error(f"Failed to save progress on signal: {e}")
        else:
            logging.getLogger(__name__).warning(
                f"Received {sig_name} again - forcing immediate exit"
            )
            sys.exit(1)
    
    def restore(self) -> None:
        """Restore original signal handlers."""
        for sig, handler in self._original_handlers.items():
            signal.signal(sig, handler)


# Global shutdown handler instance
shutdown_handler = ShutdownHandler()

# Optional callback set after GeoGrid is created, used for best-effort signal-time saves.
_shutdown_save_progress: Optional[Callable[[], None]] = None

from config.settings import (
    CONFIG_DIR,
    LOG_FILE,
    LOG_FORMAT,
    LOG_LEVEL,
    LOGS_DIR,
    REDIS_URL,
    TARGET_CARRIERS,
    REQUESTS_PER_SESSION,
    SESSION_COOLDOWN,
    RANDOMIZE_TILES,
    WARNING_THRESHOLD,
    MAX_RETRIES_PER_TILE,
    DEFER_COOLDOWN_SEC,
    METRICS_INTERVAL_SEC,
    STALL_THRESHOLD_SEC,
)
from scraper.api_client import CellMapperClient, CaptchaRequiredError
from scraper.geo_utils import Bounds, GeoGrid, USBounds, estimate_scrape_time, DeferQueue
from scraper.parser import TowerParser, get_provider_name
from scraper.storage import DataStorage
from scraper.proxy_manager import ProxyManager
from scraper.cookie_manager import get_cookie_manager
from scraper.session_pool import SessionPool
from scraper.notifier import get_notifier
from scraper.metrics import WorkerMetrics

# Optional Redis heartbeat for harvester failsafe (do not hard-fail if redis isn't installed)
try:
    import redis.asyncio as redis_async  # type: ignore
except Exception:  # pragma: no cover
    redis_async = None

# Worker -> Harvester progress keys (Redis)
WORKERS_LAST_SUCCESS_TS_KEY = os.environ.get(
    "WORKERS_LAST_SUCCESS_TS_KEY", "cellmapper:workers:last_success_ts"
)
WORKERS_LAST_SNAPSHOT_TS_KEY = os.environ.get(
    "WORKERS_LAST_SNAPSHOT_TS_KEY", "cellmapper:workers:last_snapshot_ts"
)
WORKERS_LAST_SUCCESS_META_KEY = os.environ.get(
    "WORKERS_LAST_SUCCESS_META_KEY", "cellmapper:workers:last_success_meta"
)

# Setup logging (default, may be reconfigured for carrier-specific logs)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

async def _safe_redis_set(r, key: str, value: str) -> None:
    """Best-effort Redis SET; never raise (heartbeat must not break scraping)."""
    if not r:
        return
    try:
        await r.set(key, value)
    except Exception:
        # Keep noisy failures out of logs; main scrape health is more important.
        logger.debug("Redis heartbeat write failed", exc_info=True)


async def _update_worker_heartbeat(
    r,
    *,
    worker_id: str,
    carrier_name: str,
    run_tag: str,
    tiles_processed: int,
    towers_found: int,
    kind: str,
) -> None:
    """
    Write worker liveness/progress to Redis for harvester bandwidth failsafe.

    kind:
      - 'success': a tile was completed successfully
      - 'snapshot': periodic metrics snapshot
    """
    ts = int(time.time())
    if kind == "success":
        await _safe_redis_set(r, WORKERS_LAST_SUCCESS_TS_KEY, str(ts))
        meta = {
            "ts": ts,
            "worker_id": worker_id,
            "carrier": carrier_name,
            "run_tag": run_tag,
            "tiles_processed": tiles_processed,
            "towers_found": towers_found,
            "hostname": os.environ.get("HOSTNAME", ""),
        }
        await _safe_redis_set(r, WORKERS_LAST_SUCCESS_META_KEY, json.dumps(meta))
        await _safe_redis_set(r, f"cellmapper:workers:{worker_id}:last_success_ts", str(ts))
    elif kind == "snapshot":
        await _safe_redis_set(r, WORKERS_LAST_SNAPSHOT_TS_KEY, str(ts))
        await _safe_redis_set(r, f"cellmapper:workers:{worker_id}:last_snapshot_ts", str(ts))


async def _safe_redis_close(r) -> None:
    """Best-effort close for redis asyncio client."""
    if not r:
        return
    try:
        await r.aclose()
    except Exception:
        logger.debug("Redis heartbeat close failed", exc_info=True)


def setup_carrier_logging(carrier: str) -> None:
    """
    Reconfigure logging to use carrier-specific log file.
    
    This allows parallel carrier scrapes to have separate log files
    for easier debugging (e.g., scraper_tmobile.log).
    """
    carrier_log_file = LOGS_DIR / f"scraper_{carrier}.log"
    
    # Get root logger and reconfigure
    root_logger = logging.getLogger()
    
    # Remove existing file handlers (keep StreamHandler)
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            handler.close()
            root_logger.removeHandler(handler)
    
    # Add carrier-specific file handler
    file_handler = logging.FileHandler(carrier_log_file)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    file_handler.setLevel(getattr(logging, LOG_LEVEL))
    root_logger.addHandler(file_handler)
    
    logger.info(f"Logging to carrier-specific file: {carrier_log_file}")


def setup_worker_logging(carrier: str, run_tag: str = "") -> None:
    """
    Reconfigure logging to use carrier+worker-specific log file.

    For regional scaling we run multiple workers per carrier (e.g. west/east),
    so logs must not collide.
    """
    suffix = f"_{run_tag}" if run_tag else ""
    worker_log_file = LOGS_DIR / f"scraper_{carrier}{suffix}.log"

    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            handler.close()
            root_logger.removeHandler(handler)

    file_handler = logging.FileHandler(worker_log_file)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    file_handler.setLevel(getattr(logging, LOG_LEVEL))
    root_logger.addHandler(file_handler)

    logger.info(f"Logging to worker-specific file: {worker_log_file}")


def load_network_config() -> dict:
    """Load carrier MCC/MNC configuration."""
    config_file = CONFIG_DIR / "networks.json"
    with open(config_file, "r") as f:
        return json.load(f)


def get_carrier_codes(carrier: str, config: dict) -> list[tuple[int, int]]:
    """Get list of (MCC, MNC) tuples for a carrier."""
    carrier_data = config["carriers"].get(carrier, {})
    codes = []
    for entry in carrier_data.get("mcc_mnc", []):
        codes.append((entry["mcc"], entry["mnc"]))
    return codes


MAX_SUBTILE_DEPTH = 3  # 0.25 -> 0.125 -> 0.0625 -> 0.03125


async def _fetch_subtiles(
    client: "CellMapperClient",
    parser: "TowerParser",
    storage: "DataStorage",
    bounds: "Bounds",
    mcc: int,
    mnc: int,
    tech: str,
    tile_id: str,
    session_pool=None,
    session=None,
    metrics=None,
    depth: int = 1,
) -> int:
    """
    Subdivide a tile that returned hasMore=True and fetch each sub-tile.

    Splits the parent bounds into 4 quadrants (halving lat and lon) and fetches
    each one.  If a sub-tile itself returns hasMore, recurse up to
    MAX_SUBTILE_DEPTH levels.

    Returns the number of NEW towers written to storage from all sub-tiles.
    """
    if depth > MAX_SUBTILE_DEPTH:
        logger.warning(
            f"Tile {tile_id} still has hasMore at subdivision depth {depth} "
            f"(bounds {bounds}) — skipping further subdivision"
        )
        return 0

    mid_lat = (bounds.north + bounds.south) / 2
    mid_lon = (bounds.east + bounds.west) / 2

    quadrants = [
        Bounds(north=bounds.north, south=mid_lat, east=mid_lon,     west=bounds.west),   # NW
        Bounds(north=bounds.north, south=mid_lat, east=bounds.east,  west=mid_lon),       # NE
        Bounds(north=mid_lat,      south=bounds.south, east=mid_lon, west=bounds.west),   # SW
        Bounds(north=mid_lat,      south=bounds.south, east=bounds.east, west=mid_lon),   # SE
    ]

    total_written = 0

    for qi, sub_bounds in enumerate(quadrants):
        sub_label = f"{tile_id}/d{depth}q{qi}"
        logger.info(
            f"Subtile {sub_label}: fetching {tech} MCC={mcc} MNC={mnc} "
            f"bounds=({sub_bounds.south:.4f},{sub_bounds.west:.4f})-"
            f"({sub_bounds.north:.4f},{sub_bounds.east:.4f})"
        )

        try:
            response = await client.get_towers(
                mcc=mcc, mnc=mnc,
                bounds=sub_bounds.to_dict(),
                technology=tech,
            )

            if metrics:
                metrics.record_api_result(
                    success=bool(getattr(response, "success", False)),
                    request_time_sec=float(getattr(response, "request_time", 0.0) or 0.0),
                    error_code=str(getattr(response, "error_code", "") or ""),
                )

            if session_pool and session:
                session_pool.mark_result(session, response)

            if not response.success:
                logger.warning(f"Subtile {sub_label} failed: {response.error}")
                continue

            if response.data:
                records, sub_has_more = parser.parse_towers_response(
                    response.data, mcc, mnc, tech
                )
                written = storage.write_many(records)
                total_written += written
                logger.info(f"Subtile {sub_label}: {written} new towers (hasMore={sub_has_more})")

                if sub_has_more:
                    deeper = await _fetch_subtiles(
                        client=client,
                        parser=parser,
                        storage=storage,
                        bounds=sub_bounds,
                        mcc=mcc,
                        mnc=mnc,
                        tech=tech,
                        tile_id=tile_id,
                        session_pool=session_pool,
                        session=session,
                        metrics=metrics,
                        depth=depth + 1,
                    )
                    total_written += deeper

        except CaptchaRequiredError:
            logger.warning(f"Subtile {sub_label}: CAPTCHA during subdivision — skipping quadrant")
            continue
        except Exception as e:
            logger.error(f"Subtile {sub_label}: unexpected error — {e}")
            continue

    logger.info(
        f"Tile {tile_id} subdivision depth={depth}: "
        f"{total_written} new towers recovered from 4 sub-tiles"
    )
    return total_written


async def scrape_carrier(
    client: CellMapperClient,
    parser: TowerParser,
    storage: DataStorage,
    grid: GeoGrid,
    mcc: int,
    mnc: int,
    carrier_name: str,
    technologies: list[str] = ["LTE", "NR"],
    session_pool: Optional[SessionPool] = None,
    randomize_tiles: bool = False,
    notifier=None,
    metrics: Optional[WorkerMetrics] = None,
) -> dict:
    """
    Scrape all towers for a single MCC/MNC combination.
    
    Args:
        client: CellMapper API client
        parser: Tower data parser
        storage: Data storage handler
        grid: Geographic grid to scrape
        mcc: Mobile Country Code
        mnc: Mobile Network Code
        carrier_name: Human-readable carrier name
        technologies: List of RAT types to scrape (LTE, NR)
        notifier: Email notifier for alerts
        
    Returns:
        Statistics dictionary
    """
    stats = {
        "carrier": carrier_name,
        "mcc": mcc,
        "mnc": mnc,
        "tiles_processed": 0,
        "tiles_deferred": 0,
        "tiles_recovered": 0,
        "tiles_permanently_failed": 0,
        "towers_found": 0,
        "errors": 0,
    }
    
    # Hardening v2: Defer queue for stubborn tiles
    defer_queue = DeferQueue(
        cooldown_sec=DEFER_COOLDOWN_SEC,
        max_retries=MAX_RETRIES_PER_TILE,
    )
    
    # Track consecutive failures for warning threshold alerts
    consecutive_failures = 0
    last_error = ""
    warning_sent = False  # Only send one warning per threshold breach
    
    provider = get_provider_name(mcc, mnc)
    logger.info(f"Starting scrape for {provider} (MCC={mcc}, MNC={mnc})")

    # Redis heartbeat client (best-effort). Used by harvester to pause minting if workers stall.
    worker_redis = None
    if redis_async and REDIS_URL:
        try:
            worker_redis = redis_async.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
            await worker_redis.ping()
        except Exception:
            worker_redis = None
            logger.debug("Redis heartbeat disabled (connect/ping failed)", exc_info=True)
    
    # Get pending tiles (optionally randomized to avoid pattern detection)
    pending_tiles = list(grid.get_pending_tiles(randomize=randomize_tiles))
    total_tiles = len(pending_tiles)
    
    if total_tiles == 0:
        logger.info("No pending tiles - all completed or grid is empty")
        await _safe_redis_close(worker_redis)
        return stats
    
    logger.info(f"Processing {total_tiles} tiles for {provider}")
    
    session = None  # Track current session for error handling

    class TileRetryExceededError(RuntimeError):
        """Raised when a tile can't be processed after many session rotations."""
        pass
    
    for i, tile in enumerate(pending_tiles):
        # Check for graceful shutdown request
        if shutdown_handler.shutdown_requested:
            logger.info(f"Shutdown requested - stopping after {i} tiles")
            grid.save_progress()  # Ensure progress is saved
            break
        
        try:
            tile_towers = 0
            
            # Proactive session rotation via session pool (+ self-healing on CAPTCHA/transport)
            # Retry the SAME tile if we rotate away due to immediate CAPTCHA or transient transport issues.
            tile_attempts = 0
            while True:
                tile_attempts += 1
                if tile_attempts > 25:
                    raise TileRetryExceededError(f"Tile {tile.id}: exceeded session retry attempts")

                if session_pool:
                    session = await session_pool.get_session()
                    if not session:
                        raise RuntimeError("SessionPool returned no sessions")

                    # Update client with this session's cookies and proxy
                    if session.cookies:
                        client.cookies = session.cookies.copy()  # replace, don't merge
                        client.current_proxy = session.proxy
                        await client._create_client(proxy_url=session.proxy.url)
                        logger.info(f"Using {len(client.cookies)} cookies from session: {session}")

                    # Per-session preflight: validate cookies immediately after refresh
                    if getattr(session, "needs_preflight", False):
                        logger.info(f"Preflight: validating cookies for session {session}")
                        try:
                            pre = await client.get_towers(
                                mcc=mcc,
                                mnc=mnc,
                                bounds=tile.bounds.to_dict(),
                                technology=technologies[0] if technologies else "LTE",
                            )
                            # Count this preflight against the session and classify failures
                            session_pool.mark_result(session, pre)
                            if pre.success:
                                session.needs_preflight = False
                                logger.info("Preflight: OK")
                            else:
                                logger.warning(f"Preflight: failed ({pre.error_code or pre.error}) -> rotate")
                                session.needs_preflight = False
                                continue  # rotate and retry tile
                        except CaptchaRequiredError as e:
                            logger.error(f"Preflight: NEED_RECAPTCHA -> rotate: {e}")
                            session.needs_preflight = False
                            session_pool.mark_captcha(session)
                            continue  # rotate and retry tile

                # Only mark a tile completed if ALL tech requests succeeded.
                tile_requests_ok = True

                try:
                    for tech in technologies:
                        # Fetch towers for this tile
                        response = await client.get_towers(
                            mcc=mcc,
                            mnc=mnc,
                            bounds=tile.bounds.to_dict(),
                            technology=tech,
                        )
                        if metrics:
                            metrics.record_api_result(
                                success=bool(getattr(response, "success", False)),
                                request_time_sec=float(getattr(response, "request_time", 0.0) or 0.0),
                                error_code=str(getattr(response, "error_code", "") or ""),
                            )
                        
                        # Track request in session pool
                        if session_pool and session:
                            # Rich classification (transport vs non-transport); SessionPool owns rotation decisions.
                            session_pool.mark_result(session, response)
                        
                        if not response.success:
                            tile_requests_ok = False
                            if response.error:
                                logger.warning(
                                    f"Tile {tile.id} request failed ({tech}): "
                                    f"{response.error} ({response.error_code or 'no_code'})"
                                )
                                stats["errors"] += 1
                                consecutive_failures += 1
                                last_error = response.error
                                
                                # Send warning if threshold reached
                                if (consecutive_failures >= WARNING_THRESHOLD 
                                    and not warning_sent and notifier):
                                    notifier.update_progress(
                                        stats["tiles_processed"], 
                                        stats["towers_found"]
                                    )
                                    notifier.send_warning_threshold(
                                        consecutive_failures, 
                                        last_error, 
                                        carrier=carrier_name
                                    )
                                    warning_sent = True
                            # Rotate and retry this tile with a different session.
                            break

                        if response.success and response.data:
                            # Parse the response
                            records, has_more = parser.parse_towers_response(
                                response.data, mcc, mnc, tech
                            )
                            
                            # Store records
                            written = storage.write_many(records)
                            tile_towers += written
                            stats["towers_found"] += written
                            
                            # Reset consecutive failures on success
                            consecutive_failures = 0
                            warning_sent = False
                            
                            if has_more:
                                sub_written = await _fetch_subtiles(
                                    client=client,
                                    parser=parser,
                                    storage=storage,
                                    bounds=tile.bounds,
                                    mcc=mcc,
                                    mnc=mnc,
                                    tech=tech,
                                    tile_id=tile.id,
                                    session_pool=session_pool,
                                    session=session,
                                    metrics=metrics,
                                )
                                tile_towers += sub_written
                                stats["towers_found"] += sub_written
                        else:
                            if response.error:
                                logger.warning(
                                    f"Tile {tile.id} error: {response.error} ({response.error_code or 'no_code'})"
                                )
                                stats["errors"] += 1
                                consecutive_failures += 1
                                last_error = response.error
                                
                                # Send warning if threshold reached
                                if (consecutive_failures >= WARNING_THRESHOLD 
                                    and not warning_sent and notifier):
                                    notifier.update_progress(
                                        stats["tiles_processed"], 
                                        stats["towers_found"]
                                    )
                                    notifier.send_warning_threshold(
                                        consecutive_failures, 
                                        last_error, 
                                        carrier=carrier_name
                                    )
                                    warning_sent = True

                    # If any tech request failed, retry this tile (do not mark completed).
                    if not tile_requests_ok:
                        continue

                    # Tile completed successfully
                    break

                except CaptchaRequiredError as e:
                    # SessionPool mode: rotate and retry the SAME tile.
                    # Do not mark the tile completed/errored; cookies/IP mismatch is per-session.
                    logger.error(f"CAPTCHA required - rotating session and retrying tile {tile.id}: {e}")
                    stats["errors"] += 1
                    if session_pool and session:
                        session_pool.mark_captcha(session)
                    if metrics:
                        metrics.record_error("captcha", str(e))
                    continue
            
            # Mark tile as completed
            grid.mark_completed(tile.id, tower_count=tile_towers)
            stats["tiles_processed"] += 1
            
            # Record metrics for this tile
            if metrics:
                metrics.record_tile_completed(
                    tile_id=tile.id,
                    tower_count=tile_towers,
                    data_bytes=tile_towers * 200,  # Approximate bytes per tower record
                )
                # Worker progress heartbeat for harvester failsafe
                await _update_worker_heartbeat(
                    worker_redis,
                    worker_id=metrics.worker_id,
                    carrier_name=carrier_name,
                    run_tag="",
                    tiles_processed=stats["tiles_processed"],
                    towers_found=stats["towers_found"],
                    kind="success",
                )
                # Update proxy stats from session pool
                if session_pool:
                    pool_stats = session_pool.get_stats()
                    metrics.update_proxy_stats(
                        active=pool_stats.get("healthy_sessions", 0),
                        bad=pool_stats.get("bad_proxies", 0),
                        cooling=pool_stats.get("proxies_cooling", 0),
                    )
                
                # Write periodic snapshot if interval elapsed
                if metrics.should_write_snapshot(METRICS_INTERVAL_SEC):
                    metrics.write_snapshot_to_file()
                    logger.info(f"Metrics snapshot written | {metrics.get_velocity_report()}")
                    await _update_worker_heartbeat(
                        worker_redis,
                        worker_id=metrics.worker_id,
                        carrier_name=carrier_name,
                        run_tag="",
                        tiles_processed=stats["tiles_processed"],
                        towers_found=stats["towers_found"],
                        kind="snapshot",
                    )
                    
                    # Velocity watchdog: Pause if stuck in low-productivity loop
                    velocity = metrics.get_velocity_tiles_per_hour()
                    time_since_success = metrics.get_time_since_last_success()
                    if velocity < 1.0 and time_since_success > 1800:  # <1 tile/hr AND 30 min no success
                        logger.warning(
                            f"🛑 Velocity watchdog triggered! "
                            f"velocity={velocity:.1f} tiles/hr, no success for {time_since_success:.0f}s. "
                            f"Pausing 900s to save proxy resources..."
                        )
                        await asyncio.sleep(900)  # 15-minute pause
            
            # Progress logging with velocity and ETA
            if (i + 1) % 10 == 0 or i == total_tiles - 1:
                pct = (i + 1) / total_tiles * 100
                defer_stats = defer_queue.get_stats()
                pool_report = session_pool.report() if session_pool else ""
                
                # Enhanced logging with velocity/ETA
                velocity_report = ""
                eta_str = ""
                if metrics:
                    velocity = metrics.get_velocity_tiles_per_hour()
                    remaining = total_tiles - (i + 1)
                    eta_hours = remaining / velocity if velocity > 0 else 0
                    eta_str = f" | ETA={eta_hours:.1f}h" if velocity > 0 else ""
                    velocity_report = f" | {metrics.get_velocity_report()}"
                
                logger.info(
                    f"Progress: {i + 1}/{total_tiles} tiles ({pct:.1f}%) - "
                    f"{stats['towers_found']} towers{eta_str} | "
                    f"deferred={defer_stats['pending_deferred']} | "
                    f"{pool_report}{velocity_report}"
                )
        
        except TileRetryExceededError as e:
            # Hardening v2: Defer tile instead of blocking the run
            logger.warning(str(e))
            stats["errors"] += 1
            if defer_queue.defer(tile):
                stats["tiles_deferred"] += 1
            else:
                # Tile exceeded max retries - permanently failed
                stats["tiles_permanently_failed"] += 1
                grid.mark_completed(tile.id, tower_count=0, error_count=tile.error_count + 1)
            grid.save_progress()
            continue

        except CaptchaRequiredError as e:
            # Direct-mode only: bubble up to run_scraper's cookie refresh logic
            logger.error(f"CAPTCHA required - session invalid: {e}")
            stats["errors"] += 1
            if metrics:
                metrics.record_error("captcha", str(e))
            grid.save_progress()
            raise
                
        except Exception as e:
            logger.error(f"Error processing tile {tile.id}: {e}")
            stats["errors"] += 1
            if metrics:
                # Categorize common errors
                error_str = str(e)
                if "407" in error_str:
                    metrics.record_error("407", error_str)
                elif "522" in error_str:
                    metrics.record_error("522", error_str)
                elif "timeout" in error_str.lower():
                    metrics.record_error("timeout", error_str)
                else:
                    metrics.record_error("other", error_str)
            # Do NOT mark completed on unexpected errors; keep it pending for later retry.
            grid.save_progress()
    
    # Hardening v2: Process any remaining deferred tiles
    if not defer_queue.is_empty:
        logger.info(f"Processing {len(defer_queue)} deferred tiles after main queue...")
        deferred_tiles = defer_queue.drain_all()
        
        for tile in deferred_tiles:
            if shutdown_handler.shutdown_requested:
                logger.info("Shutdown requested - stopping deferred tile processing")
                break
            
            # Reset retry counter for final attempt
            tile.retry_count = 0
            
            # Re-process deferred tile with same logic (simplified - single attempt)
            try:
                tile_towers = 0
                if session_pool:
                    session = await session_pool.get_session()
                    if session and session.cookies:
                        client.cookies = session.cookies.copy()
                        client.current_proxy = session.proxy
                        await client._create_client(proxy_url=session.proxy.url)
                
                for tech in technologies:
                    response = await client.get_towers(
                        mcc=mcc, mnc=mnc,
                        bounds=tile.bounds.to_dict(),
                        technology=tech,
                    )
                    if session_pool and session:
                        session_pool.mark_result(session, response)
                    
                    if response.success and response.data:
                        records, has_more = parser.parse_towers_response(
                            response.data, mcc, mnc, tech
                        )
                        written = storage.write_many(records)
                        tile_towers += written
                        stats["towers_found"] += written
                        stats["tiles_recovered"] += 1

                        if has_more:
                            sub_written = await _fetch_subtiles(
                                client=client,
                                parser=parser,
                                storage=storage,
                                bounds=tile.bounds,
                                mcc=mcc,
                                mnc=mnc,
                                tech=tech,
                                tile_id=tile.id,
                                session_pool=session_pool,
                                session=session,
                                metrics=metrics,
                            )
                            tile_towers += sub_written
                            stats["towers_found"] += sub_written
                
                grid.mark_completed(tile.id, tower_count=tile_towers)
                stats["tiles_processed"] += 1
                logger.info(f"Recovered deferred tile {tile.id}: {tile_towers} towers")
                
            except Exception as e:
                logger.error(f"Deferred tile {tile.id} failed again: {e}")
                stats["tiles_permanently_failed"] += 1
                stats["errors"] += 1
        
        grid.save_progress()
    
    # Log defer queue stats
    defer_stats = defer_queue.get_stats()
    logger.info(
        f"Completed {provider}: {stats['tiles_processed']} tiles, "
        f"{stats['towers_found']} towers, {stats['errors']} errors | "
        f"deferred: {defer_stats['total_deferred']}, recovered: {defer_stats['total_recovered']}, "
        f"failed: {defer_stats['permanently_failed']}"
    )
    
    await _safe_redis_close(worker_redis)
    return stats


async def run_scraper(
    carriers: list[str],
    test_mode: bool = False,
    resume: bool = True,
    output_format: str = "jsonl",
    use_proxies: bool = False,
    proxy_file: Optional[Path] = None,
    auto_refresh_cookies: bool = False,
    use_session_pool: bool = False,
    randomize_tiles: bool = False,
    requests_per_session: int = 4,
    cookie_engine: str = "auto",
    bounds: Optional[tuple[float, float, float, float]] = None,
    run_tag: str = "",
    fast_mode: bool = False,
) -> dict:
    """
    Main scraper orchestration.
    
    Args:
        carriers: List of carrier keys to scrape (verizon, att, tmobile)
        test_mode: If True, scrape a small test area only
        resume: If True, resume from saved progress
        output_format: Output format (jsonl, csv, sqlite)
        
    Returns:
        Overall statistics
    """
    start_time = datetime.now()
    logger.info(f"CellMapper Scraper starting at {start_time.isoformat()}")
    logger.info(f"Carriers: {carriers}")
    logger.info(f"Test mode: {test_mode}")
    logger.info(f"Cookie engine: {cookie_engine}")
    
    # Load configuration
    config = load_network_config()
    
    # Create geographic grid
    if test_mode:
        # Small test area (Los Angeles area - high tower density)
        grid = GeoGrid.for_area(
            north=34.2,
            south=33.9,
            east=-118.1,
            west=-118.5,
            lat_step=0.1,
            lon_step=0.1,
        )
        logger.info(f"Test mode: {len(grid.tiles)} tiles in test area")
        if bounds:
            logger.warning("--bounds ignored in --test mode")
    else:
        if bounds:
            north, west, south, east = bounds
            grid = GeoGrid.for_area(
                north=north,
                south=south,
                east=east,
                west=west,
            )
            logger.info(f"Bounds mode: {len(grid.tiles)} tiles in {grid.bounds}")
        else:
            # Full US grid
            grid = GeoGrid.for_us()
            logger.info(f"Full US mode: {len(grid.tiles)} tiles")

    # Allow SIGTERM handler to save progress immediately (best-effort)
    global _shutdown_save_progress
    _shutdown_save_progress = grid.save_progress

    # Apply run tag for regional scaling (affects progress file naming)
    if run_tag:
        grid.set_run_tag(run_tag)
    
    # Don't reset progress if resuming
    if not resume:
        grid.reset_progress()
    
    # Estimate time
    total_mcc_mnc = sum(len(get_carrier_codes(c, config)) for c in carriers)
    estimates = estimate_scrape_time(
        tile_count=len(grid.tiles),
        carriers=total_mcc_mnc,
        request_delay=2.0,  # Conservative estimate
        requests_per_tile=2,  # LTE + NR
    )
    logger.info(
        f"Estimated time: {estimates['total_hours']:.1f} hours "
        f"({estimates['total_days']:.1f} days)"
    )
    
    overall_stats = {
        "start_time": start_time.isoformat(),
        "carriers_processed": [],
        "total_towers": 0,
        "total_errors": 0,
    }
    
    # Determine single carrier mode early for isolation setup
    single_carrier = carriers[0] if len(carriers) == 1 else ""
    
    # Initialize proxy manager if enabled
    # For single-carrier mode, use carrier-specific proxy file for isolation
    proxy_manager = None
    if use_proxies or use_session_pool:
        if single_carrier and not proxy_file:
            # Use carrier-specific proxy file to prevent session collisions
            carrier_proxy_file = CONFIG_DIR / f"proxies_{single_carrier}.txt"
            if carrier_proxy_file.exists():
                proxy_file = carrier_proxy_file
                logger.info(f"Using carrier-specific proxies: {carrier_proxy_file}")
            else:
                proxy_file = CONFIG_DIR / "proxies.txt"
                logger.warning(f"Carrier proxy file not found ({carrier_proxy_file}), using shared proxies.txt")
        else:
            proxy_file = proxy_file or (CONFIG_DIR / "proxies.txt")
        
        proxy_manager = ProxyManager(proxy_file)
        if proxy_manager.total_count > 0:
            logger.info(f"Proxy rotation enabled with {proxy_manager.total_count} proxies")
        else:
            logger.warning("No proxies loaded from file. Running without proxies.")
            if not use_session_pool:
                proxy_manager = None
    
    # Initialize session pool if enabled (recommended for large scrapes)
    session_pool = None
    if use_session_pool and proxy_manager and proxy_manager.total_count > 0:
        session_pool = SessionPool(
            proxy_manager=proxy_manager,
            requests_per_session=requests_per_session,
            cooldown_seconds=SESSION_COOLDOWN,
            carrier=single_carrier,  # Carrier-specific cookie isolation
            cookie_engine=cookie_engine,
        )
        await session_pool.initialize()
        logger.info(
            f"Session pool initialized: {session_pool.get_stats()['total_sessions']} sessions, "
            f"rotating every {requests_per_session} requests"
            f"{f', carrier: {single_carrier}' if single_carrier else ''}"
        )
    
    # Initialize components
    parser = TowerParser()
    # Use carrier-specific cookie manager for isolation (needed for both session pool and direct mode)
    cookie_manager = (
        get_cookie_manager(carrier=single_carrier, cookie_engine=cookie_engine)
        if auto_refresh_cookies
        else None
    )
    max_cookie_refreshes = 3  # Max auto-refresh attempts per run
    cookie_refresh_count = 0
    
    # Initialize email notifier for alerts
    notifier = get_notifier()
    
    # Use randomized tiles if session pool is enabled (or explicitly requested)
    use_random_tiles = randomize_tiles or (use_session_pool and RANDOMIZE_TILES)
    
    # For parallel execution (single carrier), use carrier-specific output files
    # This prevents race conditions when running multiple carriers simultaneously
    if single_carrier:
        setup_worker_logging(single_carrier, run_tag=run_tag)
        logger.info(
            f"Single carrier mode: using carrier-specific files for {single_carrier}"
            f"{f' ({run_tag})' if run_tag else ''}"
        )
    
    # Initialize worker metrics for monitoring
    worker_id = f"{single_carrier}_{run_tag}" if run_tag else (single_carrier or "all")
    metrics = WorkerMetrics(worker_id=worker_id, logs_dir=LOGS_DIR)
    logger.info(f"Worker metrics initialized: {worker_id}")
    
    rotate_proxies = not use_session_pool
    async with CellMapperClient(
        proxy_manager=proxy_manager,
        rotate_proxies=rotate_proxies,
        fast_mode=fast_mode,
        cookie_manager=cookie_manager,
    ) as client:
        with DataStorage(format=output_format, carrier=single_carrier, run_tag=run_tag) as storage:
            
            carrier_index = 0
            while carrier_index < len(carriers):
                carrier = carriers[carrier_index]
                carrier_codes = get_carrier_codes(carrier, config)
                
                if not carrier_codes:
                    logger.warning(f"No MCC/MNC codes found for carrier: {carrier}")
                    carrier_index += 1
                    continue
                
                logger.info(
                    f"Processing {carrier} with {len(carrier_codes)} MCC/MNC combinations"
                )
                
                # Set carrier-specific progress file for parallel execution
                grid.set_carrier(carrier)
                # IMPORTANT: --no-resume must win, even for carrier-specific progress files.
                # GeoGrid.set_carrier() reloads carrier progress; reset again here to ensure a fresh run.
                if not resume:
                    grid.reset_progress()
                
                # Reload progress for this carrier (in case resuming)
                if resume:
                    pending = sum(1 for t in grid.tiles.values() if not t.completed)
                    completed = len(grid.tiles) - pending
                    if completed > 0:
                        logger.info(f"Resuming {carrier}: {completed}/{len(grid.tiles)} tiles already completed")
                
                # For most carriers, we only need to scrape the primary MCC/MNC
                # The API returns all towers for that carrier regardless of specific MNC
                # So we use just the first/primary code
                primary_mcc, primary_mnc = carrier_codes[0]
                
                try:
                    # Set carrier on notifier for email context
                    notifier.set_carrier(carrier)
                    
                    carrier_stats = await scrape_carrier(
                        client=client,
                        parser=parser,
                        storage=storage,
                        grid=grid,
                        mcc=primary_mcc,
                        mnc=primary_mnc,
                        carrier_name=carrier,
                        session_pool=session_pool,
                        randomize_tiles=use_random_tiles,
                        notifier=notifier,
                        metrics=metrics,
                    )
                    
                    overall_stats["carriers_processed"].append(carrier_stats)
                    overall_stats["total_towers"] += carrier_stats["towers_found"]
                    overall_stats["total_errors"] += carrier_stats["errors"]
                    
                    # NOTE: Do NOT reset progress here - each worker has its own carrier-specific
                    # progress file. Resetting would delete the file and lose all progress.
                    # The old code called grid.reset_progress() here which was catastrophic
                    # for single-carrier workers with restart policies.
                    
                    carrier_index += 1
                    
                except CaptchaRequiredError:
                    if auto_refresh_cookies and cookie_refresh_count < max_cookie_refreshes:
                        logger.warning("CAPTCHA detected - attempting auto-refresh...")
                        cookie_refresh_count += 1
                        
                        try:
                            # Get proxy URL if using session pool
                            proxy_url = client.current_proxy.url if client.current_proxy else None
                            new_cookies = await cookie_manager.refresh_cookies(proxy_url=proxy_url)
                            if new_cookies:
                                logger.info("✓ Cookies refreshed successfully")
                                # Replace cookies entirely with fresh ones
                                client.cookies = client._parse_cookies(new_cookies)
                                await client._create_client(proxy_url=proxy_url)
                                logger.info("Retrying carrier after cookie refresh...")
                                continue  # Retry same carrier
                            else:
                                logger.error("Cookie refresh failed - no cookies returned")
                        except Exception as e:
                            logger.error(f"Cookie refresh error: {e}")
                    
                    # Can't auto-refresh or refresh failed
                    raise
    
    # Final statistics
    end_time = datetime.now()
    duration = end_time - start_time
    overall_stats["end_time"] = end_time.isoformat()
    overall_stats["duration_seconds"] = duration.total_seconds()
    
    logger.info("=" * 60)
    logger.info("SCRAPER COMPLETE")
    logger.info(f"Duration: {duration}")
    logger.info(f"Total towers collected: {overall_stats['total_towers']}")
    logger.info(f"Total errors: {overall_stats['total_errors']}")
    logger.info("=" * 60)
    
    # Send completion notification email (with false-completion guard)
    # Guard: Don't send completion email if run looks suspicious (likely progress corruption)
    tiles_processed_this_run = sum(
        cs.get("tiles_processed", 0) for cs in overall_stats.get("carriers_processed", [])
    )
    duration_seconds = duration.total_seconds()
    is_suspicious_completion = (
        duration_seconds < 300  # Less than 5 minutes
        and overall_stats['total_towers'] == 0
        and tiles_processed_this_run <= 1
    )
    
    if is_suspicious_completion:
        logger.warning(
            f"SUSPICIOUS COMPLETION DETECTED - NOT sending email. "
            f"Duration={duration_seconds:.1f}s, towers={overall_stats['total_towers']}, "
            f"tiles_processed={tiles_processed_this_run}. "
            f"This may indicate progress file corruption."
        )
    else:
        notifier.update_progress(
            tiles=len(grid.tiles),
            towers=overall_stats['total_towers']
        )
        notifier.send_completion(
            duration_hours=duration.total_seconds() / 3600,
            carrier=single_carrier or "all",
        )
    
    # Clean up session pool and log Hardening v2 stats
    if session_pool:
        logger.info(f"Final {session_pool.report()}")
        pool_stats = session_pool.get_stats()
        logger.info(
            f"Hardening v2 summary: captchas={pool_stats['captcha_triggers']}, "
            f"bad_proxies={pool_stats['bad_proxies']}, "
            f"total_captcha_hits={pool_stats['total_captcha_hits']}"
        )
        await session_pool.close()
    
    # Write final metrics snapshot
    metrics.write_snapshot_to_file()
    final_snapshot = metrics.get_periodic_snapshot()
    logger.info(
        f"Final metrics: tiles={final_snapshot['tiles_completed_total']}, "
        f"towers={final_snapshot['towers_found_total']}, "
        f"velocity={final_snapshot['velocity_tiles_per_hour']} tiles/hr, "
        f"success_rate={final_snapshot['session_success_rate']}%"
    )
    
    # Save final stats
    stats_file = Path("data/scrape_stats.json")
    stats_file.parent.mkdir(parents=True, exist_ok=True)
    with open(stats_file, "w") as f:
        json.dump(overall_stats, f, indent=2)
    
    return overall_stats


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Scrape cell tower data from CellMapper.net",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --test                    # Quick test run
  python main.py --carrier tmobile         # T-Mobile only  
  python main.py --carrier att --carrier verizon  # Multiple carriers
  python main.py --format csv              # Output as CSV
  python main.py --no-resume               # Start fresh (ignore progress)
        """,
    )
    
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode (small geographic area)",
    )

    # Regional scaling: custom geographic bounds
    # Usage: --bounds 49 -125 24 -90  (north west south east)
    parser.add_argument(
        "--bounds",
        nargs=4,
        type=float,
        metavar=("NORTH", "WEST", "SOUTH", "EAST"),
        help="Restrict scrape to bounding box: north west south east (lat/lon degrees)",
    )

    parser.add_argument(
        "--run-tag",
        type=str,
        default="",
        help="Suffix for progress/output/log files (e.g. 'west' or 'east')",
    )
    
    parser.add_argument(
        "--carrier",
        action="append",
        choices=["verizon", "att", "tmobile"],
        help="Carrier to scrape (can specify multiple). Default: all three",
    )
    
    parser.add_argument(
        "--format",
        choices=["jsonl", "csv", "sqlite"],
        default="jsonl",
        help="Output format (default: jsonl)",
    )
    
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't resume from previous progress",
    )
    
    parser.add_argument(
        "--estimate",
        action="store_true",
        help="Show time estimate and exit without scraping",
    )
    
    parser.add_argument(
        "--proxies",
        action="store_true",
        help="Enable proxy rotation using config/proxies.txt",
    )
    
    parser.add_argument(
        "--proxy-file",
        type=Path,
        help="Path to proxy list file (default: config/proxies.txt)",
    )
    
    parser.add_argument(
        "--auto-refresh",
        action="store_true",
        help="Auto-refresh cookies when CAPTCHA detected (requires Playwright)",
    )

    parser.add_argument(
        "--cookie-engine",
        choices=["auto", "playwright", "flaresolverr"],
        default="auto",
        help="Cookie refresh engine. "
             "'auto' uses environment configuration; "
             "'playwright' forces Playwright; "
             "'flaresolverr' forces FlareSolverr (NOT allowed with --session-pool).",
    )
    
    parser.add_argument(
        "--session-pool",
        action="store_true",
        help="Use session pool with proactive rotation (RECOMMENDED for large scrapes). "
             "Each proxy gets its own session, rotating every few requests.",
    )
    
    parser.add_argument(
        "--randomize",
        action="store_true",
        help="Randomize tile order to avoid geographic patterns (auto-enabled with --session-pool)",
    )
    
    parser.add_argument(
        "--requests-per-session",
        type=int,
        default=REQUESTS_PER_SESSION,  # Hardening v2: default lowered to 2
        help=f"Number of requests per proxy before rotating (default: {REQUESTS_PER_SESSION})",
    )

    parser.add_argument(
        "--fast",
        action="store_true",
        help="Enable opt-in speed mode (lower per-request delays when proxy pool is large). "
             "Monitor metrics for 429/403/CAPTCHA spikes.",
    )
    
    args = parser.parse_args()

    # Resolve cookie engine with safe defaults/fail-fast rules
    cookie_engine = args.cookie_engine
    if args.session_pool and cookie_engine == "auto":
        cookie_engine = "playwright"
    if args.session_pool and cookie_engine == "flaresolverr":
        parser.error(
            "--cookie-engine flaresolverr cannot be used with --session-pool. "
            "Session-pool requires proxy-bound cookies; use --cookie-engine playwright."
        )
    
    # Determine carriers
    carriers = args.carrier if args.carrier else TARGET_CARRIERS
    
    # Show estimate only
    if args.estimate:
        config = load_network_config()
        if args.test:
            grid = GeoGrid.for_area(
                north=34.2, south=33.9, east=-118.1, west=-118.5,
                lat_step=0.1, lon_step=0.1,
            )
        else:
            if args.bounds:
                north, west, south, east = args.bounds
                grid = GeoGrid.for_area(north=north, south=south, east=east, west=west)
            else:
                grid = GeoGrid.for_us()
        
        total_mcc_mnc = len(carriers)  # Using primary MNC only
        estimates = estimate_scrape_time(
            tile_count=len(grid.tiles),
            carriers=total_mcc_mnc,
            request_delay=2.0,
            requests_per_tile=2,
        )
        
        print(f"\n{'='*50}")
        print("SCRAPE TIME ESTIMATE")
        print(f"{'='*50}")
        print(f"Grid tiles: {len(grid.tiles):,}")
        print(f"Carriers: {len(carriers)}")
        print(f"Technologies per tile: 2 (LTE + 5G NR)")
        print(f"Total API requests: {estimates['total_requests']:,}")
        print(f"Estimated time: {estimates['total_hours']:.1f} hours")
        print(f"             = {estimates['total_days']:.1f} days")
        print(f"{'='*50}\n")
        return
    
    # Setup graceful shutdown handling
    shutdown_handler.setup()
    
    # Initialize notifier for critical error alerts
    notifier = get_notifier()
    current_carrier = carriers[0] if carriers else "unknown"
    notifier.set_carrier(current_carrier)
    
    # Run the scraper
    try:
        asyncio.run(
            run_scraper(
                carriers=carriers,
                test_mode=args.test,
                resume=not args.no_resume,
                output_format=args.format,
                use_proxies=args.proxies,
                proxy_file=args.proxy_file,
                auto_refresh_cookies=args.auto_refresh,
                use_session_pool=args.session_pool,
                randomize_tiles=args.randomize,
                requests_per_session=args.requests_per_session,
                cookie_engine=cookie_engine,
                bounds=tuple(args.bounds) if args.bounds else None,
                run_tag=args.run_tag,
                fast_mode=args.fast,
            )
        )
    except CaptchaRequiredError:
        logger.error("Session expired. Please refresh your browser cookies.")
        logger.error("1. Visit cellmapper.net in your browser")
        logger.error("2. Extract JSESSIONID cookie")
        logger.error("3. Set CELLMAPPER_COOKIES environment variable")
        logger.error("4. Run again with --resume to continue")
        
        # Send critical error email
        notifier.send_critical_error(
            "CAPTCHA_REQUIRED",
            details="Session expired, auto-refresh failed. Manual intervention required.",
        )
        sys.exit(2)
    except KeyboardInterrupt:
        logger.info("Scraper interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Scraper failed: {e}")
        
        # Send critical error email
        notifier.send_critical_error(
            "UNHANDLED_EXCEPTION",
            details=str(e),
        )
        sys.exit(1)
    finally:
        shutdown_handler.restore()


if __name__ == "__main__":
    main()

