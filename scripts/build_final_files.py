#!/usr/bin/env python3
"""
Build Final Tower Files

Creates three output files:
  1. towers_all_classified.jsonl          — original scrape (untouched)
  2. towers_rescrubs_classified.jsonl     — net-new rescrub towers, merged & classified
  3. towers_complete_classified.jsonl     — all towers merged across original + rescrub

Merge logic (same as merge_towers.py):
  - Group by (provider, round(lat,6), round(lon,6))
  - Combine tower_id, site_id into lists
  - Combine bands (unique, sorted)
  - first_seen = earliest, last_seen = latest

Each record gets:
  - Field transformations (generation, band_labels, site_type, etc.)
  - rural/urban classification via Census Urban Areas shapefile
  - `source` flag: "original", "rescrub", or "both"

Usage:
    python scripts/build_final_files.py
    python scripts/build_final_files.py --dry-run
"""

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point

PROJECT_ROOT = Path(__file__).parent.parent
ORIGINAL_CLASSIFIED = PROJECT_ROOT / "downloads" / "towers_all_classified.jsonl"
RAW_MERGED = PROJECT_ROOT / "downloads" / "towers_merged_all.jsonl"
SHAPEFILE = PROJECT_ROOT / "downloads" / "shapefiles" / "tl_2024_us_uac20.shp"
OUTPUT_DIR = PROJECT_ROOT / "downloads"

US_BOUNDS = {
    "north": 49.384358, "south": 24.396308,
    "east": -66.934570, "west": -124.848974,
}

GENERATION_MAP = {
    "LTE": "4G", "LTE-A": "4G Advanced", "SA": "5G Standalone",
    "NSA": "5G Non-Standalone", "NR": "5G", "": "Unknown",
}
GENERATION_RANK = {"Unknown": 0, "4G": 1, "4G Advanced": 2, "5G Non-Standalone": 3, "5G": 4, "5G Standalone": 5}

SITE_TYPE_MAP = {
    "MACRO": "Tower", "MICRO": "Small Cell", "PICO": "Pico Cell",
    "DAS": "Distributed Antenna", "COW": "Cell on Wheels",
    "DECOMMISSIONED": "Decommissioned", "": "Unknown",
}

BAND_LABEL_MAP = {
    1: "B1 (2100 MHz)", 2: "B2 (PCS 1900 MHz)", 4: "B4 (AWS-1 1700/2100 MHz)",
    5: "B5 (850 MHz)", 10: "B10 (1700 MHz)", 12: "B12 (700 MHz A)",
    13: "B13 (700 MHz C)", 14: "B14 (700 MHz PS)", 17: "B17 (700 MHz B)",
    25: "B25 (PCS+ 1900 MHz)", 26: "B26 (850 MHz Extended)", 30: "B30 (WCS 2300 MHz)",
    38: "B38 (2600 MHz TDD)", 41: "B41 (2500 MHz TDD)", 46: "B46 (LAA 5 GHz)",
    48: "B48 (CBRS 3.5 GHz)", 66: "B66 (AWS-3 1700/2100 MHz)",
    70: "B70 (AWS-4 1700/2100 MHz)", 71: "B71 (600 MHz)", 77: "n77 (C-Band 3.7 GHz)",
}


def is_in_conus(lat: float, lon: float) -> bool:
    return (US_BOUNDS["south"] <= lat <= US_BOUNDS["north"]
            and US_BOUNDS["west"] <= lon <= US_BOUNDS["east"])


def date_only(val):
    if val and "T" in str(val):
        return str(val).split("T")[0]
    return val


def merge_raw_records(records: list[dict]) -> dict:
    """Merge multiple raw tower records at the same location into one."""
    if len(records) == 1:
        rec = records[0].copy()
        rec["tower_id"] = [rec["tower_id"]]
        rec["site_id"] = [rec["site_id"]]
        return rec

    base = records[0].copy()

    tower_ids = list(dict.fromkeys(r["tower_id"] for r in records))
    site_ids = list(dict.fromkeys(r["site_id"] for r in records))
    base["tower_id"] = tower_ids
    base["site_id"] = site_ids

    all_bands = []
    for r in records:
        all_bands.extend(b for b in r.get("bands", []) if b is not None)
    base["bands"] = sorted(set(all_bands))

    first_seens = [r.get("first_seen") for r in records if r.get("first_seen")]
    if first_seens:
        base["first_seen"] = min(first_seens)

    last_seens = [r.get("last_seen") for r in records if r.get("last_seen")]
    if last_seens:
        base["last_seen"] = max(last_seens)

    for field in ["tower_name", "tower_parent"]:
        for r in records:
            if r.get(field):
                base[field] = r[field]
                break

    # Pick the highest-generation technology
    rats = [r.get("rat_subtype") or r.get("technology") or "" for r in records]
    gens = [GENERATION_MAP.get(rat, rat if rat else "Unknown") for rat in rats]
    base["_best_generation"] = max(gens, key=lambda g: GENERATION_RANK.get(g, 0))

    # Pick best site_type (non-Unknown)
    types = [r.get("tower_type") or r.get("site_type") or "" for r in records]
    for t in types:
        mapped = SITE_TYPE_MAP.get(t, t if t else "Unknown")
        if mapped != "Unknown":
            base["_best_site_type"] = mapped
            break
    else:
        base["_best_site_type"] = "Unknown"

    # active: true if any is true
    base["visible"] = any(r.get("visible", r.get("active", False)) for r in records)

    return base


def transform_merged_record(rec: dict, source: str, rural: bool) -> dict:
    rat = rec.get("rat_subtype") or rec.get("technology") or ""
    generation = rec.get("_best_generation") or GENERATION_MAP.get(rat, rat if rat else "Unknown")
    site_type = rec.get("_best_site_type")
    if not site_type:
        tower_type = rec.get("tower_type") or rec.get("site_type") or ""
        site_type = SITE_TYPE_MAP.get(tower_type, tower_type if tower_type else "Unknown")

    bands = rec.get("bands") or []
    band_labels = [BAND_LABEL_MAP.get(b, f"B{b}") for b in bands]
    active = rec.get("visible", rec.get("active", False))

    return {
        "tower_id": rec.get("tower_id"),
        "site_id": rec.get("site_id"),
        "latitude": rec.get("latitude"),
        "longitude": rec.get("longitude"),
        "provider": rec.get("provider"),
        "generation": generation,
        "site_type": site_type,
        "active": active,
        "bands": bands,
        "band_labels": band_labels,
        "tower_name": rec.get("tower_name", ""),
        "tower_parent": rec.get("tower_parent", ""),
        "first_seen": date_only(rec.get("first_seen")),
        "last_seen": date_only(rec.get("last_seen")),
        "rural": rural,
        "source": source,
        "address": "",
        "city": "",
        "state": "",
        "zipcode": "",
        "geocode_status": "pending",
        "geocode_distance": None,
        "geocode_accuracy": "",
    }


def classify_rural(records: list[dict], ua: gpd.GeoDataFrame) -> list[bool]:
    """Returns list of rural flags (True = rural) aligned with records."""
    conus_indices = [i for i, r in enumerate(records)
                     if is_in_conus(r.get("latitude", 0), r.get("longitude", 0))]
    conus_set = set(conus_indices)

    if not conus_indices:
        return [True] * len(records)

    geom = [Point(records[i]["longitude"], records[i]["latitude"]) for i in conus_indices]
    towers_gdf = gpd.GeoDataFrame({"orig_idx": conus_indices}, geometry=geom, crs="EPSG:4326")

    joined = gpd.sjoin(towers_gdf, ua[["geometry"]], predicate="within", how="left")
    urban_indices: set[int] = set(joined.loc[joined["index_right"].notna(), "orig_idx"].unique())

    rural_count = len(conus_indices) - len(urban_indices)
    non_conus = len(records) - len(conus_indices)
    print(f"  Urban: {len(urban_indices):,} | Rural CONUS: {rural_count:,} | Non-CONUS: {non_conus:,}", flush=True)

    flags = []
    for i in range(len(records)):
        if i not in conus_set:
            flags.append(True)
        elif i in urban_indices:
            flags.append(False)
        else:
            flags.append(True)
    return flags


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    for p in [ORIGINAL_CLASSIFIED, RAW_MERGED, SHAPEFILE]:
        if not p.exists():
            print(f"ERROR: {p} not found", file=sys.stderr)
            sys.exit(1)

    t_start = time.time()

    # ── Step 1: Load original tower IDs ──
    print("Step 1: Loading original classified tower IDs ...", flush=True)
    original_ids = set()
    with open(ORIGINAL_CLASSIFIED) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            tid = rec.get("tower_id")
            if isinstance(tid, list):
                for t in tid:
                    original_ids.add(str(t))
            elif tid:
                original_ids.add(str(tid))
    print(f"  Original tower IDs: {len(original_ids):,}", flush=True)

    # ── Step 2: Extract net-new rescrub records ──
    print("\nStep 2: Extracting net-new rescrub records from raw merged ...", flush=True)
    rescrub_raw = []
    with open(RAW_MERGED) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            tid = str(rec.get("tower_id", ""))
            if tid and tid not in original_ids:
                rescrub_raw.append(rec)
    print(f"  Net-new rescrub records: {len(rescrub_raw):,}", flush=True)

    # ── Step 3: Merge rescrub records by (provider, lat, lon) ──
    print("\nStep 3: Merging rescrub records by (provider, lat, lon) ...", flush=True)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for rec in rescrub_raw:
        key = (rec["provider"], round(rec["latitude"], 6), round(rec["longitude"], 6))
        groups[key].append(rec)

    multi = sum(1 for recs in groups.values() if len(recs) > 1)
    merged_away = sum(len(recs) - 1 for recs in groups.values() if len(recs) > 1)
    print(f"  Unique locations: {len(groups):,}", flush=True)
    print(f"  Locations with multiple records: {multi:,}", flush=True)
    print(f"  Records merged: {merged_away:,}", flush=True)

    rescrub_merged = []
    for key, recs in groups.items():
        rescrub_merged.append(merge_raw_records(recs))
    rescrub_merged.sort(key=lambda r: (r["provider"], r["latitude"], r["longitude"]))

    # ── Step 4: Load shapefile and classify ──
    print("\nStep 4: Loading Census Urban Areas shapefile ...", flush=True)
    ua = gpd.read_file(str(SHAPEFILE))
    ua = ua.to_crs("EPSG:4326")
    print(f"  {len(ua):,} urban areas loaded", flush=True)

    print("\nStep 5: Classifying rescrub towers (rural/urban) ...", flush=True)
    rural_flags = classify_rural(rescrub_merged, ua)

    if args.dry_run:
        urban = sum(1 for r in rural_flags if not r)
        rural = sum(1 for r in rural_flags if r)
        print(f"\n[DRY RUN] {len(rescrub_merged):,} merged rescrub records (urban: {urban:,}, rural: {rural:,})")
        return

    # ── Step 6: Write rescrub-only file ──
    print("\nStep 6: Writing towers_rescrubs_classified.jsonl ...", flush=True)
    rescrub_path = OUTPUT_DIR / "towers_rescrubs_classified.jsonl"
    with open(rescrub_path, "w") as f:
        for rec, rural in zip(rescrub_merged, rural_flags):
            transformed = transform_merged_record(rec, source="rescrub", rural=rural)
            f.write(json.dumps(transformed, separators=(",", ":")) + "\n")
    size_mb = rescrub_path.stat().st_size / 1024 / 1024
    print(f"  Done: {len(rescrub_merged):,} records ({size_mb:.1f} MB)", flush=True)

    # ── Step 7: Build complete merged file ──
    # Load original classified records, add source flag, then append rescrub
    # Also merge across original+rescrub if same (provider, lat, lon)
    print("\nStep 7: Building towers_complete_classified.jsonl ...", flush=True)

    # Load all original records
    original_records = []
    with open(ORIGINAL_CLASSIFIED) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rec["source"] = "original"
            original_records.append(rec)
    print(f"  Original records: {len(original_records):,}", flush=True)

    # Load rescrub classified records
    rescrub_records = []
    with open(rescrub_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rescrub_records.append(json.loads(line))
    print(f"  Rescrub records: {len(rescrub_records):,}", flush=True)

    # Group ALL records by (provider, lat6, lon6) to find cross-source overlaps
    all_groups: dict[tuple, dict] = {}
    for rec in original_records:
        key = (rec["provider"], round(rec["latitude"], 6), round(rec["longitude"], 6))
        all_groups[key] = {"original": rec, "rescrub": None}

    cross_merges = 0
    for rec in rescrub_records:
        key = (rec["provider"], round(rec["latitude"], 6), round(rec["longitude"], 6))
        if key in all_groups:
            all_groups[key]["rescrub"] = rec
            cross_merges += 1
        else:
            all_groups[key] = {"original": None, "rescrub": rec}

    print(f"  Cross-source merges (same location): {cross_merges:,}", flush=True)
    print(f"  Unique locations in complete file: {len(all_groups):,}", flush=True)

    # Write complete file
    complete_path = OUTPUT_DIR / "towers_complete_classified.jsonl"
    total_written = 0
    with open(complete_path, "w") as fout:
        for key in sorted(all_groups.keys()):
            pair = all_groups[key]
            orig = pair["original"]
            rescrub = pair["rescrub"]

            if orig and rescrub:
                # Merge: combine bands, tower_ids, site_ids, dates
                merged = orig.copy()

                # Combine tower_id lists
                orig_tids = orig.get("tower_id", [])
                if not isinstance(orig_tids, list):
                    orig_tids = [orig_tids]
                rescrub_tids = rescrub.get("tower_id", [])
                if not isinstance(rescrub_tids, list):
                    rescrub_tids = [rescrub_tids]
                merged["tower_id"] = list(dict.fromkeys(orig_tids + rescrub_tids))

                orig_sids = orig.get("site_id", [])
                if not isinstance(orig_sids, list):
                    orig_sids = [orig_sids]
                rescrub_sids = rescrub.get("site_id", [])
                if not isinstance(rescrub_sids, list):
                    rescrub_sids = [rescrub_sids]
                merged["site_id"] = list(dict.fromkeys(orig_sids + rescrub_sids))

                # Combine bands
                orig_bands = set(orig.get("bands", []))
                rescrub_bands = set(rescrub.get("bands", []))
                combined_bands = sorted(orig_bands | rescrub_bands)
                merged["bands"] = combined_bands
                merged["band_labels"] = [BAND_LABEL_MAP.get(b, f"B{b}") for b in combined_bands]

                # Best generation
                orig_gen = orig.get("generation", "Unknown")
                rescrub_gen = rescrub.get("generation", "Unknown")
                merged["generation"] = max([orig_gen, rescrub_gen],
                                           key=lambda g: GENERATION_RANK.get(g, 0))

                # Dates
                dates_first = [d for d in [orig.get("first_seen"), rescrub.get("first_seen")] if d]
                if dates_first:
                    merged["first_seen"] = min(dates_first)
                dates_last = [d for d in [orig.get("last_seen"), rescrub.get("last_seen")] if d]
                if dates_last:
                    merged["last_seen"] = max(dates_last)

                # active: true if either is true
                merged["active"] = orig.get("active", False) or rescrub.get("active", False)

                merged["source"] = "both"
                fout.write(json.dumps(merged, separators=(",", ":")) + "\n")
            elif orig:
                fout.write(json.dumps(orig, separators=(",", ":")) + "\n")
            else:
                fout.write(json.dumps(rescrub, separators=(",", ":")) + "\n")
            total_written += 1

    size_mb = complete_path.stat().st_size / 1024 / 1024
    print(f"  Done: {total_written:,} records ({size_mb:.1f} MB)", flush=True)

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print("FINAL FILE INVENTORY")
    print(f"{'='*60}")
    orig_count = len(original_records)
    rescrub_count = len(rescrub_merged)
    print(f"  1. towers_all_classified.jsonl        {orig_count:>10,}  (original, untouched)")
    print(f"  2. towers_rescrubs_classified.jsonl    {rescrub_count:>10,}  (net-new rescrubs, merged)")
    print(f"  3. towers_complete_classified.jsonl    {total_written:>10,}  (all merged, source flag)")
    print(f"     - source='original':  {sum(1 for v in all_groups.values() if v['original'] and not v['rescrub']):,}")
    print(f"     - source='rescrub':   {sum(1 for v in all_groups.values() if v['rescrub'] and not v['original']):,}")
    print(f"     - source='both':      {cross_merges:,}")
    print(f"{'='*60}")
    print(f"  Completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
