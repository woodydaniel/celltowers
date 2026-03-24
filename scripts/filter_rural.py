#!/usr/bin/env python3
"""
Filter Rural Tower Records

Classifies all towers in towers_merged.jsonl as urban (skipped=false) or
rural/non-US (skipped=true) using the US Census Bureau 2020 TIGER/Line
Urban Areas shapefile.

Outputs three files:
  towers_all_classified.jsonl  - All records with `skipped` field added
  towers_urban.jsonl           - Only records where skipped=false
  towers_rural.jsonl           - Only records where skipped=true

Usage:
    # Exact target count (recommended) - ranks non-urban towers by proximity
    python scripts/filter_rural.py --target 200937

    # Buffer-based (approximate)
    python scripts/filter_rural.py --buffer-km 0.6

    # Dry-run (no files written)
    python scripts/filter_rural.py --dry-run --target 200937

    # Custom paths
    python scripts/filter_rural.py --target 200937 \\
                                   --input downloads/towers_merged.jsonl \\
                                   --shapefile downloads/shapefiles/tl_2024_us_uac20.shp \\
                                   --output-dir downloads
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional

import geopandas as gpd
from shapely.geometry import Point

# Continental US bounding box (matches config/settings.py US_BOUNDS)
US_BOUNDS = {
    "north": 49.384358,
    "south": 24.396308,
    "east": -66.934570,
    "west": -124.848974,
}

DEFAULT_INPUT = "downloads/towers_merged.jsonl"
DEFAULT_SHAPEFILE = "downloads/shapefiles/tl_2024_us_uac20.shp"
DEFAULT_OUTPUT_DIR = "downloads"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Filter rural cell tower records via Census Urban Areas")
    p.add_argument("--input", default=DEFAULT_INPUT, help="Input JSONL file")
    p.add_argument("--shapefile", default=DEFAULT_SHAPEFILE, help="Census Urban Areas .shp file")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory for output files")
    p.add_argument(
        "--target",
        type=int,
        default=None,
        help=(
            "Produce exactly this many skipped=false records (e.g. 200937). "
            "First classifies strict Census urban towers (no buffer), then ranks "
            "remaining CONUS towers by distance to the nearest urban polygon and "
            "includes the closest ones to fill the gap. Mutually exclusive with --buffer-km."
        ),
    )
    p.add_argument(
        "--buffer-km",
        type=float,
        default=0.0,
        help=(
            "Expand each urban area polygon by this many kilometres before testing. "
            "Use to include suburban areas just outside the urban boundary. "
            "0 = strict Census boundary (default). Ignored when --target is used."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Classify and print counts but do not write output files",
    )
    return p.parse_args()


def is_in_conus(lat: float, lon: float) -> bool:
    return (
        US_BOUNDS["south"] <= lat <= US_BOUNDS["north"]
        and US_BOUNDS["west"] <= lon <= US_BOUNDS["east"]
    )


def load_records(path: Path) -> list[dict]:
    print(f"Loading records from {path} ...", flush=True)
    records = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    print(f"  Loaded {len(records):,} records", flush=True)
    return records


def load_urban_areas(shapefile: Path, buffer_km: float) -> gpd.GeoDataFrame:
    print(f"Loading urban areas shapefile: {shapefile} ...", flush=True)
    ua = gpd.read_file(str(shapefile))
    ua = ua.to_crs("EPSG:4326")
    print(f"  {len(ua):,} urban areas loaded", flush=True)

    if buffer_km > 0:
        print(f"  Applying {buffer_km} km buffer around urban boundaries ...", flush=True)
        # Reproject to a metre-based CRS for accurate buffering, then back
        ua_m = ua.to_crs("EPSG:3857")
        ua_m["geometry"] = ua_m.geometry.buffer(buffer_km * 1000)
        ua = ua_m.to_crs("EPSG:4326")
        print(f"  Buffer applied.", flush=True)

    return ua


def classify_towers(
    records: list[dict], ua: gpd.GeoDataFrame
) -> tuple[list[bool], int, int, int]:
    """
    Returns:
        skipped_flags : list[bool] aligned with records
        non_conus_count : how many were outside CONUS bounding box
        rural_count : how many are CONUS but outside all urban areas
        urban_count : how many are inside an urban area
    """
    n = len(records)

    # Step 1: CONUS bounding box pre-filter
    print("Step 1: Checking CONUS bounding box ...", flush=True)
    conus_mask = [is_in_conus(r["latitude"], r["longitude"]) for r in records]
    non_conus = sum(1 for ok in conus_mask if not ok)
    print(f"  CONUS: {n - non_conus:,} | Non-CONUS (bogus): {non_conus:,}", flush=True)

    # Step 2: Build GeoDataFrame for CONUS towers only
    print("Step 2: Building GeoDataFrame for CONUS towers ...", flush=True)
    conus_indices = [i for i, ok in enumerate(conus_mask) if ok]
    geom = [Point(records[i]["longitude"], records[i]["latitude"]) for i in conus_indices]
    towers_gdf = gpd.GeoDataFrame(
        {"orig_idx": conus_indices}, geometry=geom, crs="EPSG:4326"
    )

    # Step 3: Spatial join - keep only urban columns to minimise memory
    print("Step 3: Running spatial join (point-in-polygon) ...", flush=True)
    t0 = time.time()
    joined = gpd.sjoin(
        towers_gdf,
        ua[["geometry"]],
        predicate="within",
        how="left",
    )
    elapsed = time.time() - t0
    print(f"  Spatial join completed in {elapsed:.1f}s", flush=True)

    # A tower near a polygon edge may match multiple polygons; keep unique orig_idx
    urban_orig_indices: set[int] = set(
        joined.loc[joined["index_right"].notna(), "orig_idx"].unique()
    )

    urban_count = len(urban_orig_indices)
    rural_count = len(conus_indices) - urban_count

    # Step 4: Build skipped flags
    skipped_flags = []
    for i, rec in enumerate(records):
        if not conus_mask[i]:
            # Outside CONUS bounding box -> bogus coordinate
            skipped_flags.append(True)
        elif i in urban_orig_indices:
            # Inside a Census urban area -> keep
            skipped_flags.append(False)
        else:
            # CONUS but rural -> exclude
            skipped_flags.append(True)

    return skipped_flags, non_conus, rural_count, urban_count


def classify_towers_exact(
    records: list[dict], ua: gpd.GeoDataFrame, target: int
) -> tuple[list[bool], int, int, int]:
    """
    Produce exactly `target` records with skipped=false.

    Strategy:
      1. Strict point-in-polygon join → urban_set (no buffer)
      2. For non-urban CONUS towers, compute distance to nearest urban polygon
      3. Sort by distance ascending, take (target - len(urban_set)) closest
         as additional keeps
      4. Everything else → skipped=true

    Returns:
        skipped_flags : list[bool] aligned with records
        non_conus_count : towers outside CONUS bounding box
        rural_count : towers that ended up skipped=true (CONUS only)
        urban_count : towers that ended up skipped=false
    """
    n = len(records)

    # Step 1: CONUS bounding box pre-filter
    print("Step 1: Checking CONUS bounding box ...", flush=True)
    conus_mask = [is_in_conus(r["latitude"], r["longitude"]) for r in records]
    non_conus = sum(1 for ok in conus_mask if not ok)
    conus_indices = [i for i, ok in enumerate(conus_mask) if ok]
    print(f"  CONUS: {len(conus_indices):,} | Non-CONUS (bogus): {non_conus:,}", flush=True)

    # Step 2: Strict point-in-polygon join (no buffer)
    print("Step 2: Strict point-in-polygon join ...", flush=True)
    geom_all = [Point(records[i]["longitude"], records[i]["latitude"]) for i in conus_indices]
    towers_gdf = gpd.GeoDataFrame(
        {"orig_idx": conus_indices}, geometry=geom_all, crs="EPSG:4326"
    )
    t0 = time.time()
    joined = gpd.sjoin(towers_gdf, ua[["geometry"]], predicate="within", how="left")
    print(f"  Spatial join completed in {time.time() - t0:.1f}s", flush=True)

    urban_orig_indices: set[int] = set(
        joined.loc[joined["index_right"].notna(), "orig_idx"].unique()
    )
    strict_urban_count = len(urban_orig_indices)
    gap = target - strict_urban_count
    print(f"  Strict urban: {strict_urban_count:,} | Gap to fill: {gap:,}", flush=True)

    if gap < 0:
        raise ValueError(
            f"--target {target:,} is less than the strict urban count "
            f"({strict_urban_count:,}). Lower the target or remove --target."
        )

    # Step 3: For non-urban CONUS towers, compute distance to nearest urban polygon
    non_urban_indices = [i for i in conus_indices if i not in urban_orig_indices]
    print(
        f"Step 3: Computing distances for {len(non_urban_indices):,} non-urban towers ...",
        flush=True,
    )
    geom_non_urban = [Point(records[i]["longitude"], records[i]["latitude"]) for i in non_urban_indices]
    non_urban_gdf = gpd.GeoDataFrame(
        {"orig_idx": non_urban_indices}, geometry=geom_non_urban, crs="EPSG:4326"
    )

    # Reproject to metres for meaningful distance values, then sjoin_nearest
    ua_m = ua.to_crs("EPSG:3857")
    non_urban_m = non_urban_gdf.to_crs("EPSG:3857")

    t0 = time.time()
    nearest = gpd.sjoin_nearest(
        non_urban_m,
        ua_m[["geometry"]],
        how="left",
        distance_col="dist_m",
    )
    print(f"  Nearest-join completed in {time.time() - t0:.1f}s", flush=True)

    # Drop duplicate rows (towers that matched multiple polygons at equal distance)
    nearest = nearest.drop_duplicates(subset=["orig_idx"])
    nearest_sorted = nearest.sort_values("dist_m", ascending=True)

    # Take the `gap` closest non-urban towers as additional keeps
    near_urban_orig_indices: set[int] = set(nearest_sorted.head(gap)["orig_idx"].tolist())
    near_urban_count = len(near_urban_orig_indices)

    total_kept = strict_urban_count + near_urban_count
    total_skipped_conus = len(conus_indices) - total_kept
    print(
        f"  Near-urban added: {near_urban_count:,} | "
        f"Total kept: {total_kept:,} | "
        f"Skipped CONUS: {total_skipped_conus:,}",
        flush=True,
    )

    # Step 4: Build skipped flags
    skipped_flags = []
    for i in range(n):
        if not conus_mask[i]:
            skipped_flags.append(True)
        elif i in urban_orig_indices or i in near_urban_orig_indices:
            skipped_flags.append(False)
        else:
            skipped_flags.append(True)

    urban_count = total_kept
    rural_count = n - urban_count - non_conus
    return skipped_flags, non_conus, rural_count, urban_count


def write_outputs(
    records: list[dict],
    skipped_flags: list[bool],
    output_dir: Path,
    dry_run: bool,
) -> None:
    out_all = output_dir / "towers_all_classified.jsonl"
    out_urban = output_dir / "towers_urban.jsonl"
    out_rural = output_dir / "towers_rural.jsonl"

    if dry_run:
        print("\n[DRY RUN] No files written.", flush=True)
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nWriting output files to {output_dir} ...", flush=True)

    urban_written = 0
    rural_written = 0

    with (
        open(out_all, "w") as f_all,
        open(out_urban, "w") as f_urban,
        open(out_rural, "w") as f_rural,
    ):
        for rec, skipped in zip(records, skipped_flags):
            rec["skipped"] = skipped
            line = json.dumps(rec, separators=(",", ":")) + "\n"
            f_all.write(line)
            if skipped:
                f_rural.write(line)
                rural_written += 1
            else:
                f_urban.write(line)
                urban_written += 1

    print(f"  {out_all.name}: {len(records):,} records", flush=True)
    print(f"  {out_urban.name}: {urban_written:,} records (skipped=false)", flush=True)
    print(f"  {out_rural.name}: {rural_written:,} records (skipped=true)", flush=True)

    # File sizes
    for path in (out_all, out_urban, out_rural):
        size_mb = path.stat().st_size / 1024 / 1024
        print(f"  {path.name}: {size_mb:.1f} MB", flush=True)


def print_summary(
    total: int,
    urban: int,
    rural: int,
    non_conus: int,
    buffer_km: float,
    target: Optional[int] = None,
) -> None:
    skipped = rural + non_conus
    print()
    print("=" * 55)
    print("CLASSIFICATION SUMMARY")
    print("=" * 55)
    print(f"  Total input records:        {total:>10,}")
    print(f"  Kept   (skipped=false):     {urban:>10,}  ({100 * urban / total:.1f}%)")
    print(f"  Rural CONUS (skipped=true): {rural:>10,}  ({100 * rural / total:.1f}%)")
    print(f"  Non-US / bogus (skipped):   {non_conus:>10,}  ({100 * non_conus / total:.2f}%)")
    print(f"  Total skipped:              {skipped:>10,}  ({100 * skipped / total:.1f}%)")
    if target is not None:
        print(f"  Mode:                        --target {target:,}")
    else:
        print(f"  Urban area buffer:          {buffer_km:>9.1f} km")
    print("=" * 55)
    print()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    shapefile_path = Path(args.shapefile)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)
    if not shapefile_path.exists():
        print(f"ERROR: Shapefile not found: {shapefile_path}", file=sys.stderr)
        sys.exit(1)

    t_start = time.time()

    records = load_records(input_path)

    if args.target is not None:
        # Exact target mode: load shapefile with no buffer for strict classification
        ua = load_urban_areas(shapefile_path, buffer_km=0.0)
        skipped_flags, non_conus, rural, urban = classify_towers_exact(
            records, ua, args.target
        )
        print_summary(len(records), urban, rural, non_conus, buffer_km=0.0, target=args.target)
    else:
        ua = load_urban_areas(shapefile_path, args.buffer_km)
        skipped_flags, non_conus, rural, urban = classify_towers(records, ua)
        print_summary(len(records), urban, rural, non_conus, args.buffer_km)

    write_outputs(records, skipped_flags, output_dir, args.dry_run)

    elapsed = time.time() - t_start
    print(f"\nDone in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
