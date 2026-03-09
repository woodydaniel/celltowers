"""
Geographic Utilities

Provides tools for dividing the US into geographic tiles/grids
and tracking progress across regions.
"""

from __future__ import annotations

import json
import logging
import math
import os
import shutil
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, Optional

from config.settings import (
    GRID_SIZE_LAT,
    GRID_SIZE_LON,
    PROGRESS_FILE,
    US_BOUNDS,
    MAX_RETRIES_PER_TILE,
    DEFER_COOLDOWN_SEC,
)

logger = logging.getLogger(__name__)


@dataclass
class Bounds:
    """Geographic bounding box."""
    
    north: float
    south: float
    east: float
    west: float
    
    def to_dict(self) -> dict[str, float]:
        """Convert to dictionary format for API calls."""
        return {
            "north": self.north,
            "south": self.south,
            "east": self.east,
            "west": self.west,
        }
    
    @property
    def center_lat(self) -> float:
        """Get center latitude."""
        return (self.north + self.south) / 2
    
    @property
    def center_lon(self) -> float:
        """Get center longitude."""
        return (self.east + self.west) / 2
    
    @property
    def area_sq_degrees(self) -> float:
        """Get approximate area in square degrees."""
        return abs(self.north - self.south) * abs(self.east - self.west)
    
    def contains(self, lat: float, lon: float) -> bool:
        """Check if a point is within bounds."""
        return (
            self.south <= lat <= self.north and
            self.west <= lon <= self.east
        )
    
    def __str__(self) -> str:
        return f"Bounds({self.south:.3f},{self.west:.3f} to {self.north:.3f},{self.east:.3f})"


@dataclass
class Tile:
    """A geographic tile with tracking metadata."""
    
    id: str
    bounds: Bounds
    row: int
    col: int
    completed: bool = False
    tower_count: int = 0
    error_count: int = 0
    retry_count: int = 0  # Hardening v2: per-tile retry budget
    deferred: bool = False  # Hardening v2: tile is in defer queue
    
    def to_dict(self) -> dict:
        """Serialize tile to dictionary."""
        return {
            "id": self.id,
            "bounds": self.bounds.to_dict(),
            "row": self.row,
            "col": self.col,
            "completed": self.completed,
            "tower_count": self.tower_count,
            "error_count": self.error_count,
            "retry_count": self.retry_count,
            "deferred": self.deferred,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Tile":
        """Deserialize tile from dictionary."""
        return cls(
            id=data["id"],
            bounds=Bounds(**data["bounds"]),
            row=data["row"],
            col=data["col"],
            completed=data.get("completed", False),
            tower_count=data.get("tower_count", 0),
            error_count=data.get("error_count", 0),
            retry_count=data.get("retry_count", 0),
            deferred=data.get("deferred", False),
        )


class USBounds:
    """Continental US bounds reference."""
    
    NORTH = US_BOUNDS["north"]
    SOUTH = US_BOUNDS["south"]
    EAST = US_BOUNDS["east"]
    WEST = US_BOUNDS["west"]
    
    @classmethod
    def get_bounds(cls) -> Bounds:
        """Get US bounding box."""
        return Bounds(
            north=cls.NORTH,
            south=cls.SOUTH,
            east=cls.EAST,
            west=cls.WEST,
        )
    
    @classmethod
    def is_valid_us_coordinate(cls, lat: float, lon: float) -> bool:
        """Check if coordinates are within continental US."""
        return (
            cls.SOUTH <= lat <= cls.NORTH and
            cls.WEST <= lon <= cls.EAST
        )


@dataclass
class DeferredTile:
    """A tile that has been deferred for later retry."""
    tile: Tile
    deferred_at: float  # Unix timestamp when deferred
    attempt_count: int  # How many times this tile has been deferred


class DeferQueue:
    """
    Queue for tiles that repeatedly fail, preventing run termination.
    
    Hardening v2: Stubborn tiles go here instead of blocking the run.
    They re-enter the main queue after a cooldown period.
    """
    
    def __init__(
        self,
        cooldown_sec: float = DEFER_COOLDOWN_SEC,
        max_retries: int = MAX_RETRIES_PER_TILE,
    ):
        self._queue: deque[DeferredTile] = deque()
        self.cooldown_sec = cooldown_sec
        self.max_retries = max_retries
        
        # Stats
        self.total_deferred = 0
        self.total_recovered = 0
        self.permanently_failed = 0
    
    def defer(self, tile: Tile) -> bool:
        """
        Defer a tile for later retry.
        
        Returns True if tile was deferred, False if it has exceeded max retries
        and should be marked as permanently failed.
        """
        tile.retry_count += 1
        tile.deferred = True
        
        if tile.retry_count > self.max_retries:
            # Exceeded retry budget - mark as permanently failed
            self.permanently_failed += 1
            logger.warning(
                f"Tile {tile.id} exceeded retry budget ({self.max_retries}) - "
                f"marking as permanently failed"
            )
            return False
        
        self._queue.append(DeferredTile(
            tile=tile,
            deferred_at=time.time(),
            attempt_count=tile.retry_count,
        ))
        self.total_deferred += 1
        logger.info(
            f"Deferred tile {tile.id} (attempt {tile.retry_count}/{self.max_retries})"
        )
        return True
    
    def get_ready_tiles(self) -> list[Tile]:
        """
        Get tiles that have cooled down and are ready for retry.
        
        Returns a list of tiles whose cooldown has expired.
        """
        now = time.time()
        ready = []
        remaining = deque()
        
        while self._queue:
            deferred = self._queue.popleft()
            if now - deferred.deferred_at >= self.cooldown_sec:
                deferred.tile.deferred = False
                ready.append(deferred.tile)
                self.total_recovered += 1
                logger.info(
                    f"Tile {deferred.tile.id} ready for retry after cooldown"
                )
            else:
                remaining.append(deferred)
        
        self._queue = remaining
        return ready
    
    def drain_all(self) -> list[Tile]:
        """
        Force-drain all deferred tiles regardless of cooldown.
        
        Used at end of main queue to process remaining deferred tiles.
        """
        tiles = []
        while self._queue:
            deferred = self._queue.popleft()
            deferred.tile.deferred = False
            tiles.append(deferred.tile)
            self.total_recovered += 1
        return tiles
    
    def __len__(self) -> int:
        return len(self._queue)
    
    @property
    def is_empty(self) -> bool:
        return len(self._queue) == 0
    
    def get_stats(self) -> dict:
        """Get defer queue statistics."""
        return {
            "pending_deferred": len(self._queue),
            "total_deferred": self.total_deferred,
            "total_recovered": self.total_recovered,
            "permanently_failed": self.permanently_failed,
        }


@dataclass
class GeoGrid:
    """
    Geographic grid system for dividing regions into tiles.
    
    Creates a grid of rectangular tiles covering the specified bounds,
    with progress tracking for resume capability.
    
    For parallel carrier scraping, use set_carrier() to use carrier-specific
    progress files (e.g., progress_tmobile.json).
    """
    
    bounds: Bounds
    lat_step: float = GRID_SIZE_LAT
    lon_step: float = GRID_SIZE_LON
    progress_file: Path = PROGRESS_FILE
    tiles: dict[str, Tile] = field(default_factory=dict)
    carrier: str = field(default="")
    run_tag: str = field(default="")
    
    def __post_init__(self):
        """Initialize the grid."""
        self._generate_tiles()
        self._load_progress()
    
    def set_carrier(self, carrier: str) -> None:
        """
        Set carrier for carrier-specific progress file.
        
        This allows multiple carriers to run in parallel without
        conflicting on progress tracking.
        """
        self.carrier = carrier
        if carrier:
            # Use carrier-specific progress file
            suffix = f"_{self.run_tag}" if self.run_tag else ""
            self.progress_file = PROGRESS_FILE.parent / f"progress_{carrier}{suffix}.json"
            logger.info(f"Using carrier-specific progress file: {self.progress_file}")
            # Reload progress from carrier-specific file
            self._load_progress()

    def set_run_tag(self, run_tag: str) -> None:
        """
        Set a run tag used to isolate progress files for multiple workers.

        Example: carrier=tmobile, run_tag=west -> progress_tmobile_west.json
        """
        self.run_tag = run_tag.strip()
        # If carrier already set, re-point progress file and reload
        if self.carrier:
            self.set_carrier(self.carrier)
    
    def _generate_tiles(self) -> None:
        """Generate grid tiles covering the bounds."""
        self.tiles = {}
        
        # Calculate number of rows and columns
        lat_range = self.bounds.north - self.bounds.south
        lon_range = self.bounds.east - self.bounds.west
        
        num_rows = math.ceil(lat_range / self.lat_step)
        num_cols = math.ceil(abs(lon_range) / self.lon_step)
        
        logger.info(f"Generating grid: {num_rows} rows x {num_cols} cols = {num_rows * num_cols} tiles")
        
        for row in range(num_rows):
            for col in range(num_cols):
                # Calculate tile bounds
                south = self.bounds.south + (row * self.lat_step)
                north = min(south + self.lat_step, self.bounds.north)
                west = self.bounds.west + (col * self.lon_step)
                east = min(west + self.lon_step, self.bounds.east)
                
                tile_id = f"tile_{row:04d}_{col:04d}"
                tile_bounds = Bounds(
                    north=north,
                    south=south,
                    east=east,
                    west=west,
                )
                
                self.tiles[tile_id] = Tile(
                    id=tile_id,
                    bounds=tile_bounds,
                    row=row,
                    col=col,
                )
        
        logger.info(f"Generated {len(self.tiles)} tiles")
    
    def _load_progress(self) -> None:
        """Load progress from file if exists (with fallback to rotated backups)."""
        for candidate in self._candidate_progress_files():
            if not candidate.exists():
                continue
            try:
                with open(candidate, "r") as f:
                    progress_data = json.load(f)
                self._apply_progress_data(progress_data)
                if candidate != self.progress_file:
                    logger.warning(f"Restored progress from backup: {candidate.name}")
                return
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load {candidate.name}: {e}")

        # If we get here, there was nothing usable.
        if self.progress_file.exists():
            logger.warning("Progress file exists but could not be loaded; starting fresh")
        return

    def _candidate_progress_files(self) -> list[Path]:
        """
        Progress candidates in priority order.

        We keep up to 3 rotated backups:
        - progress_*.json
        - progress_*.json.bak
        - progress_*.json.bak.2
        - progress_*.json.bak.3
        """
        return [
            self.progress_file,
            self.progress_file.with_suffix(".json.bak"),
            self.progress_file.with_suffix(".json.bak.2"),
            self.progress_file.with_suffix(".json.bak.3"),
        ]

    def _apply_progress_data(self, progress_data: dict) -> None:
        """Apply loaded progress data to tiles."""
        for tile_id, tile_data in progress_data.get("tiles", {}).items():
            if tile_id in self.tiles:
                self.tiles[tile_id].completed = tile_data.get("completed", False)
                self.tiles[tile_id].tower_count = tile_data.get("tower_count", 0)
                self.tiles[tile_id].error_count = tile_data.get("error_count", 0)
                self.tiles[tile_id].retry_count = tile_data.get("retry_count", 0)
                self.tiles[tile_id].deferred = tile_data.get("deferred", False)

        completed = sum(1 for t in self.tiles.values() if t.completed)
        deferred = sum(1 for t in self.tiles.values() if t.deferred)
        logger.info(
            f"Loaded progress: {completed}/{len(self.tiles)} tiles completed, "
            f"{deferred} deferred"
        )
    
    def save_progress(self) -> None:
        """Save current progress to file (atomic + rotating backups)."""
        progress_data = {
            "total_tiles": len(self.tiles),
            "completed_tiles": sum(1 for t in self.tiles.values() if t.completed),
            "deferred_tiles": sum(1 for t in self.tiles.values() if t.deferred),
            "tiles": {
                tile_id: {
                    "completed": tile.completed,
                    "tower_count": tile.tower_count,
                    "error_count": tile.error_count,
                    "retry_count": tile.retry_count,
                    "deferred": tile.deferred,
                }
                for tile_id, tile in self.tiles.items()
            }
        }
        
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)

        # Rotate backups: .bak.3 (oldest) is dropped, .bak.2 -> .bak.3, .bak -> .bak.2, current -> .bak
        bak1 = self.progress_file.with_suffix(".json.bak")
        bak2 = self.progress_file.with_suffix(".json.bak.2")
        bak3 = self.progress_file.with_suffix(".json.bak.3")
        try:
            if bak3.exists():
                bak3.unlink()
            if bak2.exists():
                bak2.replace(bak3)
            if bak1.exists():
                bak1.replace(bak2)
            if self.progress_file.exists():
                shutil.copy2(self.progress_file, bak1)
        except Exception as e:
            logger.warning(f"Failed to rotate progress backups: {e}")

        # Atomic write: write to tmp then replace.
        #
        # Use a per-process tmp name to avoid leaving a stale `.tmp` behind if a
        # SIGTERM arrives mid-write. Also clean up any older stale tmp files.
        for stale in self.progress_file.parent.glob(f"{self.progress_file.name}.tmp*"):
            try:
                stale.unlink()
            except Exception:
                pass

        tmp = self.progress_file.with_suffix(f".json.tmp.{os.getpid()}")
        try:
            with open(tmp, "w") as f:
                json.dump(progress_data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())

            # Validate temp file is valid JSON before replacing the real file.
            with open(tmp, "r") as f:
                json.load(f)

            tmp.replace(self.progress_file)
            logger.debug("Progress saved (atomic)")
        except Exception as e:
            logger.error(f"Progress save failed; keeping previous file. Error: {e}")
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
    
    def get_pending_tiles(self, randomize: bool = False) -> Generator[Tile, None, None]:
        """
        Yield tiles that haven't been completed yet.
        
        Args:
            randomize: If True, shuffle tile order to avoid geographic patterns
                      that might trigger bot detection.
        """
        pending = [tile for tile in self.tiles.values() if not tile.completed]
        
        if randomize:
            import random
            random.shuffle(pending)
            logger.info(f"Randomized order of {len(pending)} pending tiles")
        else:
            pending.sort(key=lambda t: (t.row, t.col))
        
        for tile in pending:
            yield tile
    
    def get_tile(self, tile_id: str) -> Optional[Tile]:
        """Get a specific tile by ID."""
        return self.tiles.get(tile_id)
    
    def mark_completed(
        self,
        tile_id: str,
        tower_count: int = 0,
        error_count: int = 0,
        save: bool = True,
    ) -> None:
        """Mark a tile as completed."""
        if tile_id in self.tiles:
            self.tiles[tile_id].completed = True
            self.tiles[tile_id].tower_count = tower_count
            self.tiles[tile_id].error_count = error_count
            
            if save:
                self.save_progress()
    
    def reset_progress(self) -> None:
        """Reset all progress (in-memory only, does NOT delete progress file).
        
        WARNING: This only resets the in-memory state. The progress file is
        intentionally preserved to prevent accidental data loss. If you truly
        need to start fresh, manually delete the progress file.
        """
        for tile in self.tiles.values():
            tile.completed = False
            tile.tower_count = 0
            tile.error_count = 0
        
        # NOTE: Previously this deleted the progress file, which caused catastrophic
        # data loss when workers restarted. Now we only reset in-memory state.
        # The file is preserved intentionally.
        
        logger.info("Progress reset (in-memory only, file preserved)")
    
    def get_stats(self) -> dict:
        """Get grid statistics."""
        total = len(self.tiles)
        completed = sum(1 for t in self.tiles.values() if t.completed)
        deferred = sum(1 for t in self.tiles.values() if t.deferred)
        total_towers = sum(t.tower_count for t in self.tiles.values())
        total_errors = sum(t.error_count for t in self.tiles.values())
        total_retries = sum(t.retry_count for t in self.tiles.values())
        
        return {
            "total_tiles": total,
            "completed_tiles": completed,
            "pending_tiles": total - completed,
            "deferred_tiles": deferred,
            "total_retries": total_retries,
            "completion_percent": completed / total * 100 if total > 0 else 0,
            "total_towers_found": total_towers,
            "total_errors": total_errors,
        }
    
    @classmethod
    def for_us(
        cls,
        lat_step: float = GRID_SIZE_LAT,
        lon_step: float = GRID_SIZE_LON,
    ) -> "GeoGrid":
        """Create a grid covering continental US."""
        return cls(
            bounds=USBounds.get_bounds(),
            lat_step=lat_step,
            lon_step=lon_step,
        )
    
    @classmethod
    def for_area(
        cls,
        north: float,
        south: float,
        east: float,
        west: float,
        lat_step: float = GRID_SIZE_LAT,
        lon_step: float = GRID_SIZE_LON,
    ) -> "GeoGrid":
        """Create a grid for a custom area."""
        return cls(
            bounds=Bounds(north=north, south=south, east=east, west=west),
            lat_step=lat_step,
            lon_step=lon_step,
        )


def calculate_tile_count(
    bounds: Bounds,
    lat_step: float = GRID_SIZE_LAT,
    lon_step: float = GRID_SIZE_LON,
) -> int:
    """Calculate number of tiles for given bounds without creating grid."""
    lat_range = bounds.north - bounds.south
    lon_range = abs(bounds.east - bounds.west)
    
    num_rows = math.ceil(lat_range / lat_step)
    num_cols = math.ceil(lon_range / lon_step)
    
    return num_rows * num_cols


def estimate_scrape_time(
    tile_count: int,
    carriers: int,
    request_delay: float,
    requests_per_tile: int = 1,
) -> dict:
    """
    Estimate total scraping time.
    
    Args:
        tile_count: Number of geographic tiles
        carriers: Number of carrier MCC/MNC combinations
        request_delay: Average delay between requests (seconds)
        requests_per_tile: Number of requests per tile per carrier
        
    Returns:
        Dict with time estimates
    """
    total_requests = tile_count * carriers * requests_per_tile
    total_seconds = total_requests * request_delay
    
    return {
        "total_requests": total_requests,
        "total_seconds": total_seconds,
        "total_minutes": total_seconds / 60,
        "total_hours": total_seconds / 3600,
        "total_days": total_seconds / 86400,
    }

