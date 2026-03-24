#!/usr/bin/env python3
"""
Rebuild GeoGrid progress files from existing tower JSONL data.

Important limitation:
- This can only prove a tile was visited if at least one tower exists in that tile.
  Tiles that legitimately have zero towers cannot be inferred from tower data alone.
  This tool is meant as an emergency recovery option when progress files are lost.

Typical usage (inside the repo):
  python scripts/rebuild_progress_from_towers.py --all
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from config.settings import GRID_SIZE_LAT, GRID_SIZE_LON


@dataclass(frozen=True)
class Bounds:
    north: float
    west: float
    south: float
    east: float


BOUNDS_EAST = Bounds(
    north=49.384358,
    west=-96.0,
    south=24.396308,
    east=-66.934570,
)

BOUNDS_WEST = Bounds(
    north=49.384358,
    west=-124.848974,
    south=24.396308,
    east=-96.0,
)


def _grid_shape(bounds: Bounds) -> Tuple[int, int]:
    lat_range = bounds.north - bounds.south
    lon_range = bounds.east - bounds.west
    num_rows = math.ceil(lat_range / GRID_SIZE_LAT)
    num_cols = math.ceil(abs(lon_range) / GRID_SIZE_LON)
    return num_rows, num_cols


def _tile_id_for_point(lat: float, lon: float, bounds: Bounds) -> str | None:
    if lat < bounds.south or lat > bounds.north:
        return None
    if lon < bounds.west or lon > bounds.east:
        return None

    # GeoGrid generates tiles starting from south/west with row increasing northward.
    row = int((lat - bounds.south) / GRID_SIZE_LAT)
    col = int((lon - bounds.west) / GRID_SIZE_LON)
    return f"tile_{row:04d}_{col:04d}"


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r") as f:
        return json.load(f)


def _save_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(data, f, indent=2)


def _ensure_progress_file(path: Path, bounds: Bounds) -> Dict[str, Any]:
    if path.exists():
        return _load_json(path)

    rows, cols = _grid_shape(bounds)
    tiles: Dict[str, Any] = {}
    for r in range(rows):
        for c in range(cols):
            tid = f"tile_{r:04d}_{c:04d}"
            tiles[tid] = {
                "completed": False,
                "tower_count": 0,
                "error_count": 0,
                "retry_count": 0,
                "deferred": False,
            }

    progress = {
        "total_tiles": rows * cols,
        "completed_tiles": 0,
        "deferred_tiles": 0,
        "tiles": tiles,
    }
    _save_json(path, progress)
    return progress


def rebuild_one(
    towers_file: Path,
    progress_file: Path,
    bounds: Bounds,
) -> Dict[str, Any]:
    progress = _ensure_progress_file(progress_file, bounds)

    tiles: Dict[str, Any] = progress.get("tiles", {}) or {}
    tower_count_per_tile: Dict[str, int] = {}

    visited_tiles = 0
    with towers_file.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                tower = json.loads(line)
            except Exception:
                continue

            lat = tower.get("latitude")
            lon = tower.get("longitude")
            if lat is None or lon is None:
                continue

            tid = _tile_id_for_point(float(lat), float(lon), bounds)
            if tid is None:
                continue
            tower_count_per_tile[tid] = tower_count_per_tile.get(tid, 0) + 1

    restored = 0
    already_done = 0

    for tid, cnt in tower_count_per_tile.items():
        if tid not in tiles:
            # Bounds mismatch or grid shape mismatch; ignore.
            continue
        visited_tiles += 1
        if not tiles[tid].get("completed"):
            tiles[tid]["completed"] = True
            restored += 1
        else:
            already_done += 1
        tiles[tid]["tower_count"] = cnt

    progress["tiles"] = tiles
    progress["completed_tiles"] = sum(1 for t in tiles.values() if t.get("completed"))
    progress["deferred_tiles"] = sum(1 for t in tiles.values() if t.get("deferred"))

    _save_json(progress_file, progress)

    return {
        "progress_file": str(progress_file),
        "tiles_with_towers": visited_tiles,
        "restored": restored,
        "already_done": already_done,
        "completed_tiles": progress["completed_tiles"],
        "total_tiles": progress["total_tiles"],
    }


def _all_jobs(data_dir: Path) -> Iterable[tuple[Path, Path, Bounds]]:
    towers_dir = data_dir / "towers"

    yield towers_dir / "towers_att_east.jsonl", data_dir / "progress_att_east.json", BOUNDS_EAST
    yield towers_dir / "towers_att_west.jsonl", data_dir / "progress_att_west.json", BOUNDS_WEST
    yield towers_dir / "towers_tmobile_east.jsonl", data_dir / "progress_tmobile_east.json", BOUNDS_EAST
    yield towers_dir / "towers_tmobile_west.jsonl", data_dir / "progress_tmobile_west.json", BOUNDS_WEST
    yield towers_dir / "towers_verizon_east.jsonl", data_dir / "progress_verizon_east.json", BOUNDS_EAST
    yield towers_dir / "towers_verizon_west.jsonl", data_dir / "progress_verizon_west.json", BOUNDS_WEST


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data", help="Path to data directory")
    parser.add_argument("--all", action="store_true", help="Rebuild all known progress files")
    parser.add_argument("--towers-file", help="Single towers JSONL file")
    parser.add_argument("--progress-file", help="Single progress JSON file")
    parser.add_argument("--region", choices=["east", "west"], help="Bounds preset for single-file mode")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)

    results = []
    if args.all:
        for towers_file, progress_file, bounds in _all_jobs(data_dir):
            if not towers_file.exists():
                print(f"SKIP (missing towers): {towers_file}")
                continue
            results.append(rebuild_one(towers_file, progress_file, bounds))
    else:
        if not (args.towers_file and args.progress_file and args.region):
            parser.error("Either use --all or provide --towers-file, --progress-file, and --region")
        bounds = BOUNDS_EAST if args.region == "east" else BOUNDS_WEST
        results.append(
            rebuild_one(Path(args.towers_file), Path(args.progress_file), bounds)
        )

    print("=== REBUILD RESULTS ===")
    for r in results:
        print(
            f"{r['progress_file']}: tiles_with_towers={r['tiles_with_towers']}, "
            f"restored={r['restored']}, completed={r['completed_tiles']}/{r['total_tiles']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

