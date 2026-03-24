#!/usr/bin/env python3
"""
Select Top 250,000 Most-Urban Un-Geocoded Towers

Source: downloads/towers_complete_classified.jsonl
        (original scrape + all rescrubs, deduplicated)

Filter:
  - geocode_status != "success"  (exclude already-geocoded towers)
  - valid CONUS coordinates (lat 24.4–49.4, lon -125 to -66.9)

Ranking (most urban → least urban):
  1. Towers INSIDE a Census 2020 Urban Area → ranked by UA land area (ALAND20)
     descending. Largest metro (NYC ~8,412 km²) first.
  2. Towers OUTSIDE any UA → ranked by distance to nearest UA boundary
     ascending (closest fringe towers first, deepest rural last).

Output: downloads/urban_250k_ungeocode.jsonl  (250,000 records)

Usage:
    python scripts/select_urban_250k_ungeocode.py
    python scripts/select_urban_250k_ungeocode.py --target 250000
    python scripts/select_urban_250k_ungeocode.py --dry-run
"""

import argparse
import json
import sys
import time
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

PROJECT_ROOT = Path(__file__).parent.parent
INPUT_FILE   = PROJECT_ROOT / "downloads" / "towers_complete_classified.jsonl"
SHAPEFILE    = PROJECT_ROOT / "downloads" / "shapefiles" / "tl_2024_us_uac20.shp"
OUTPUT_FILE  = PROJECT_ROOT / "downloads" / "urban_250k_ungeocode.jsonl"

US_BOUNDS = dict(south=24.396308, north=49.384358, west=-124.848974, east=-66.934570)

TARGET_DEFAULT = 250_000


def in_conus(lat: float, lon: float) -> bool:
    return (US_BOUNDS["south"] <= lat <= US_BOUNDS["north"]
            and US_BOUNDS["west"] <= lon <= US_BOUNDS["east"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default=str(INPUT_FILE))
    parser.add_argument("--output", default=str(OUTPUT_FILE))
    parser.add_argument("--target", type=int, default=TARGET_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    t0 = time.time()

    # ── Step 1: Load un-geocoded CONUS towers ──────────────────────────────
    print("Step 1: Loading towers_complete_classified.jsonl …", flush=True)
    records = []
    skipped_geocoded = 0
    skipped_conus = 0
    with open(args.input) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            if r.get("geocode_status") == "success":
                skipped_geocoded += 1
                continue
            lat = r.get("latitude", 0)
            lon = r.get("longitude", 0)
            if not in_conus(lat, lon):
                skipped_conus += 1
                continue
            records.append(r)

    print(f"  Un-geocoded CONUS candidates : {len(records):>9,}")
    print(f"  Skipped (already geocoded)   : {skipped_geocoded:>9,}")
    print(f"  Skipped (non-CONUS/bogus)    : {skipped_conus:>9,}")
    print(f"  Target output                : {args.target:>9,}")

    if len(records) < args.target:
        print(f"\nWARNING: Only {len(records):,} candidates available — less than target {args.target:,}", flush=True)

    # ── Step 2: Load Census Urban Areas shapefile ──────────────────────────
    print("\nStep 2: Loading Census Urban Areas shapefile …", flush=True)
    ua = gpd.read_file(str(SHAPEFILE)).to_crs("EPSG:4326")
    # Project to equal-area CRS for accurate distance calculations (EPSG:5070 = CONUS Albers)
    ua_proj = ua.to_crs("EPSG:5070")
    print(f"  {len(ua):,} Urban Area polygons loaded", flush=True)

    # ── Step 3: Build GeoDataFrame of candidate towers ────────────────────
    print("\nStep 3: Building tower GeoDataFrame …", flush=True)
    lats = [r["latitude"]  for r in records]
    lons = [r["longitude"] for r in records]
    geom = [Point(lon, lat) for lat, lon in zip(lats, lons)]
    towers_gdf = gpd.GeoDataFrame({"idx": range(len(records))}, geometry=geom, crs="EPSG:4326")
    towers_proj = towers_gdf.to_crs("EPSG:5070")

    # ── Step 4: Spatial join — towers inside Urban Areas ──────────────────
    print("\nStep 4: Spatial join — towers within Census Urban Areas …", flush=True)
    joined = gpd.sjoin(
        towers_proj,
        ua_proj[["geometry", "ALAND20", "NAME20"]],
        predicate="within",
        how="left",
    )
    # If a tower falls in multiple UA polygons (rare edge case), keep the largest
    joined = joined.sort_values("ALAND20", ascending=False).drop_duplicates(subset="idx")
    joined = joined.set_index("idx")

    urban_mask = joined["ALAND20"].notna()
    n_urban = urban_mask.sum()
    n_rural = len(records) - n_urban
    print(f"  Inside an Urban Area : {n_urban:>9,}")
    print(f"  Outside (rural)      : {n_rural:>9,}", flush=True)

    # ── Step 5: For rural towers, compute distance to nearest UA ──────────
    print("\nStep 5: Computing distance to nearest UA for rural towers …", flush=True)
    rural_indices = [i for i in range(len(records)) if not urban_mask.get(i, False)]
    rural_gdf = towers_proj.loc[towers_proj["idx"].isin(rural_indices)].copy()

    if len(rural_gdf) > 0:
        # Use sjoin_nearest for efficient distance calculation
        nearest = gpd.sjoin_nearest(
            rural_gdf,
            ua_proj[["geometry", "ALAND20", "NAME20"]],
            how="left",
            distance_col="dist_m",
        )
        # Drop duplicates if multiple nearest
        nearest = nearest.drop_duplicates(subset="idx").set_index("idx")
    else:
        nearest = pd.DataFrame()

    # ── Step 6: Build sort key ─────────────────────────────────────────────
    # Urban towers: sort_key = ALAND20 (descending → negate for ascending sort)
    # Rural towers:  sort_key = -(max_ALAND20 + 1 + dist_m) — always below urban
    print("\nStep 6: Sorting by urbanness …", flush=True)
    max_aland = float(ua["ALAND20"].max())

    sort_keys = []
    for i, r in enumerate(records):
        if urban_mask.get(i, False):
            aland = float(joined.loc[i, "ALAND20"])
            sort_keys.append(-aland)                # most urban = most negative
        else:
            if len(nearest) > 0 and i in nearest.index:
                dist = float(nearest.loc[i, "dist_m"])
            else:
                dist = float("inf")
            # All rural towers rank below urban; closer to UA = better rank
            sort_keys.append(max_aland + 1.0 + dist)

    # ── Step 7: Sort and select top N ─────────────────────────────────────
    print("\nStep 7: Selecting top {:,} towers …".format(args.target), flush=True)
    order = sorted(range(len(records)), key=lambda i: sort_keys[i])
    selected_indices = order[:args.target]
    selected = [records[i] for i in selected_indices]

    # Stats for the selected set
    n_sel_urban = sum(1 for i in selected_indices if urban_mask.get(i, False))
    n_sel_rural = len(selected) - n_sel_urban
    # Source breakdown
    src_counts: dict[str, int] = {}
    for r in selected:
        s = r.get("source", "?")
        src_counts[s] = src_counts.get(s, 0) + 1

    print(f"\n  ── Selection summary ──────────────────────────────")
    print(f"  Total selected        : {len(selected):>9,}")
    print(f"  Inside Urban Area     : {n_sel_urban:>9,}")
    print(f"  Near/outside UA (fringe) : {n_sel_rural:>6,}")
    print(f"  Source breakdown:")
    for src, cnt in sorted(src_counts.items()):
        print(f"    {src:<12}: {cnt:,}")

    if args.dry_run:
        print("\n[DRY RUN] No file written.", flush=True)
        return

    # ── Step 8: Write output ───────────────────────────────────────────────
    print(f"\nStep 8: Writing {len(selected):,} records to {args.output} …", flush=True)
    out_path = Path(args.output)
    tmp_path = out_path.with_suffix(".jsonl.tmp")
    with open(tmp_path, "w") as f:
        for r in selected:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    tmp_path.replace(out_path)

    size_mb = out_path.stat().st_size / 1024 / 1024
    elapsed = time.time() - t0
    print(f"  Done: {size_mb:.1f} MB  ({elapsed:.0f}s)")
    print(f"\nOutput: {out_path}")


if __name__ == "__main__":
    main()
