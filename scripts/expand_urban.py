#!/usr/bin/env python3
"""
Expand the rural=false set in towers_all_classified.jsonl.

Takes the N closest rural=true towers to the nearest Census Urban Area polygon
and flips them to rural=false, so they are included in the geocoding run.

Only towers_all_classified.jsonl is modified. All existing field values
(address, geocode_status, etc.) are preserved — only the `rural` column changes.

Usage:
    python scripts/expand_urban.py --add 50000
    python scripts/expand_urban.py --add 50000 --dry-run
"""

import argparse
import json
import sys
import time
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point

CLASSIFIED_FILE = "downloads/towers_all_classified.jsonl"
SHAPEFILE = "downloads/shapefiles/tl_2024_us_uac20.shp"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Expand rural=false set by N closest rural towers")
    p.add_argument("--add", type=int, required=True, help="Number of rural=true records to flip to rural=false")
    p.add_argument("--dry-run", action="store_true", help="Print counts only, do not write file")
    p.add_argument("--input", default=CLASSIFIED_FILE)
    p.add_argument("--shapefile", default=SHAPEFILE)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).parent.parent
    input_path = project_root / args.input
    shapefile_path = project_root / args.shapefile

    if not input_path.exists():
        print(f"ERROR: {input_path} not found", file=sys.stderr)
        sys.exit(1)
    if not shapefile_path.exists():
        print(f"ERROR: {shapefile_path} not found", file=sys.stderr)
        sys.exit(1)

    t_start = time.time()

    # ── Step 1: Load all records ──────────────────────────────────────────────
    print(f"Loading {input_path.name} ...", flush=True)
    records = []
    with open(input_path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    total = len(records)
    rural_indices = [i for i, r in enumerate(records) if r.get("rural") is True]
    urban_indices = [i for i, r in enumerate(records) if r.get("rural") is False]

    print(f"  Total records:  {total:,}")
    print(f"  rural=false:    {len(urban_indices):,}")
    print(f"  rural=true:     {len(rural_indices):,}", flush=True)

    if args.add > len(rural_indices):
        print(f"ERROR: --add {args.add:,} exceeds rural=true count ({len(rural_indices):,})", file=sys.stderr)
        sys.exit(1)

    # ── Step 2: Load shapefile (no buffer — strict boundaries) ───────────────
    print(f"\nLoading shapefile ...", flush=True)
    ua = gpd.read_file(str(shapefile_path))
    ua_m = ua.to_crs("EPSG:3857")
    print(f"  {len(ua_m):,} urban areas loaded", flush=True)

    # ── Step 3: Build GeoDataFrame for rural=true towers ─────────────────────
    print(f"\nComputing distances for {len(rural_indices):,} rural=true towers ...", flush=True)
    geom = [Point(records[i]["longitude"], records[i]["latitude"]) for i in rural_indices]
    rural_gdf = gpd.GeoDataFrame(
        {"orig_idx": rural_indices}, geometry=geom, crs="EPSG:4326"
    ).to_crs("EPSG:3857")

    # ── Step 4: sjoin_nearest to get distance to nearest urban polygon ────────
    t0 = time.time()
    nearest = gpd.sjoin_nearest(
        rural_gdf,
        ua_m[["geometry"]],
        how="left",
        distance_col="dist_m",
    )
    print(f"  Nearest-join done in {time.time() - t0:.1f}s", flush=True)

    # Drop duplicates (towers equidistant from multiple polygons)
    nearest = nearest.drop_duplicates(subset=["orig_idx"])
    nearest_sorted = nearest.sort_values("dist_m", ascending=True)

    # Take the N closest
    to_flip = set(nearest_sorted.head(args.add)["orig_idx"].tolist())
    assert len(to_flip) == args.add, f"Expected {args.add} to flip, got {len(to_flip)}"

    dist_stats = nearest_sorted["dist_m"].head(args.add)
    print(f"\n  Closest {args.add:,} rural towers — distance to urban boundary:")
    print(f"    Min:    {dist_stats.min()/1000:.2f} km")
    print(f"    Median: {dist_stats.median()/1000:.2f} km")
    print(f"    P90:    {dist_stats.quantile(0.9)/1000:.2f} km")
    print(f"    Max:    {dist_stats.max()/1000:.2f} km")

    # ── Step 5: Summary ───────────────────────────────────────────────────────
    new_false = len(urban_indices) + args.add
    new_true = len(rural_indices) - args.add

    print()
    print("=" * 50)
    print("RESULT SUMMARY")
    print("=" * 50)
    print(f"  rural=false (before): {len(urban_indices):>10,}")
    print(f"  rural=false (after):  {new_false:>10,}  (+{args.add:,})")
    print(f"  rural=true  (before): {len(rural_indices):>10,}")
    print(f"  rural=true  (after):  {new_true:>10,}")
    print(f"  Total records:        {total:>10,}  (unchanged)")
    print("=" * 50)

    if args.dry_run:
        print("\n[DRY RUN] No file written.")
        return

    # ── Step 6: Write back — only `rural` column changes ─────────────────────
    print(f"\nWriting updated file ...", flush=True)
    tmp_path = input_path.with_suffix(".jsonl.tmp")

    with open(input_path) as fin, open(tmp_path, "w") as fout:
        for i, line in enumerate(fin):
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if i in to_flip:
                rec["rural"] = False
            fout.write(json.dumps(rec, separators=(",", ":")) + "\n")

    tmp_path.replace(input_path)
    size_mb = input_path.stat().st_size / 1024 / 1024
    print(f"  Done. {input_path.name}: {size_mb:.1f} MB  ({time.time() - t_start:.1f}s total)", flush=True)


if __name__ == "__main__":
    main()
