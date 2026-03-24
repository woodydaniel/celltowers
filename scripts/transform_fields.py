#!/usr/bin/env python3
"""
Transform Tower Record Fields for Human Readability

Applies the following changes to the three classified output files:
  towers_all_classified.jsonl
  towers_urban.jsonl
  towers_rural.jsonl

The original source file (towers_merged.jsonl) is NEVER touched.

Changes applied:
  - Add `generation`   : human-readable network generation (4G, 5G, etc.)
  - Add `band_labels`  : readable band names (e.g. "B71 600MHz", "B66 AWS-3")
  - Rename `visible`   -> `active`
  - Rename `tower_type`-> `site_type` with plain-English values
  - Clean `first_seen` / `last_seen`: strip time component (keep date only)
  - Drop internal/technical fields (see FIELDS_TO_DROP below)

Usage:
    python scripts/transform_fields.py
    python scripts/transform_fields.py --dry-run   # print sample only, no writes
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

# ── Files to transform (relative to project root) ────────────────────────────

TARGET_FILES = [
    "downloads/towers_all_classified.jsonl",
    "downloads/towers_urban.jsonl",
    "downloads/towers_rural.jsonl",
]

# ── Fields to drop ────────────────────────────────────────────────────────────

FIELDS_TO_DROP = {
    "channels",           # raw EARFCN numbers, meaningless without lookup
    "bandwidths",         # MHz per channel, highly technical
    "bands_str",          # redundant with `bands`
    "mcc",                # always US, redundant with `provider`
    "mnc",                # carrier code, redundant with `provider`
    "rat_subtype",        # replaced by `generation`
    "technology",         # replaced by `generation` / `band_labels`
    "region_id",          # internal CellMapper region ID
    "estimated_band_data",# nested technical object, replaced by `band_labels`
    "cells",              # always empty {}
    "scraped_at",         # internal pipeline metadata
}

# ── generation: mapped from rat_subtype ──────────────────────────────────────

GENERATION_MAP = {
    "LTE":   "4G",
    "LTE-A": "4G Advanced",
    "SA":    "5G Standalone",
    "NSA":   "5G Non-Standalone",
    "NR":    "5G",
    "":      "Unknown",
}

# ── site_type: mapped from tower_type ────────────────────────────────────────

SITE_TYPE_MAP = {
    "MACRO":         "Tower",
    "MICRO":         "Small Cell",
    "PICO":          "Pico Cell",
    "DAS":           "Distributed Antenna",
    "COW":           "Cell on Wheels",
    "DECOMMISSIONED":"Decommissioned",
    "":              "Unknown",
}

# ── band_labels: US band number -> readable label ────────────────────────────
# Covers all band numbers seen in the dataset

BAND_LABEL_MAP = {
    1:  "B1 (2100 MHz)",
    2:  "B2 (PCS 1900 MHz)",
    4:  "B4 (AWS-1 1700/2100 MHz)",
    5:  "B5 (850 MHz)",
    10: "B10 (1700 MHz)",
    12: "B12 (700 MHz A)",
    13: "B13 (700 MHz C)",
    14: "B14 (700 MHz PS)",
    17: "B17 (700 MHz B)",
    25: "B25 (PCS+ 1900 MHz)",
    26: "B26 (850 MHz Extended)",
    30: "B30 (WCS 2300 MHz)",
    38: "B38 (2600 MHz TDD)",
    41: "B41 (2500 MHz TDD)",
    46: "B46 (LAA 5 GHz)",
    48: "B48 (CBRS 3.5 GHz)",
    66: "B66 (AWS-3 1700/2100 MHz)",
    70: "B70 (AWS-4 1700/2100 MHz)",
    71: "B71 (600 MHz)",
    77: "n77 (C-Band 3.7 GHz)",
}


def transform_record(rec: dict) -> dict:
    """Apply all field transformations to a single record."""
    out = {}

    # ── Derive new fields before building output ──────────────────────────────

    # generation (from rat_subtype)
    rat = rec.get("rat_subtype", "") or ""
    generation = GENERATION_MAP.get(rat, rat if rat else "Unknown")

    # band_labels (from bands list)
    bands = rec.get("bands") or []
    band_labels = [BAND_LABEL_MAP.get(b, "B%d" % b) for b in bands]

    # site_type (from tower_type)
    tower_type = rec.get("tower_type", "") or ""
    site_type = SITE_TYPE_MAP.get(tower_type, tower_type if tower_type else "Unknown")

    # active (from visible)
    active = rec.get("visible", False)

    # date-only first/last seen
    def date_only(val):
        if val and "T" in val:
            return val.split("T")[0]
        return val

    # ── Build output record in clean field order ──────────────────────────────

    out["tower_id"]        = rec.get("tower_id")
    out["site_id"]         = rec.get("site_id")
    out["latitude"]        = rec.get("latitude")
    out["longitude"]       = rec.get("longitude")
    out["provider"]        = rec.get("provider")
    out["generation"]      = generation
    out["site_type"]       = site_type
    out["active"]          = active
    out["bands"]           = bands
    out["band_labels"]     = band_labels
    out["tower_name"]      = rec.get("tower_name", "")
    out["tower_parent"]    = rec.get("tower_parent", "")
    out["first_seen"]      = date_only(rec.get("first_seen"))
    out["last_seen"]       = date_only(rec.get("last_seen"))
    out["skipped"]         = rec.get("skipped")

    return out


def transform_file(input_path: Path, dry_run: bool) -> None:
    print(f"\nTransforming: {input_path}", flush=True)
    if not input_path.exists():
        print(f"  ERROR: file not found, skipping.", flush=True)
        return

    tmp_path = input_path.with_suffix(".jsonl.tmp")
    count = 0
    t0 = time.time()

    try:
        with open(input_path, "r") as fin, open(tmp_path, "w") as fout:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                transformed = transform_record(rec)
                if not dry_run:
                    fout.write(json.dumps(transformed, separators=(",", ":")) + "\n")
                count += 1

        if dry_run:
            tmp_path.unlink(missing_ok=True)
            print(f"  [DRY RUN] Would transform {count:,} records", flush=True)
        else:
            # Atomic replace
            tmp_path.replace(input_path)
            elapsed = time.time() - t0
            size_mb = input_path.stat().st_size / 1024 / 1024
            print(f"  Done: {count:,} records in {elapsed:.1f}s  ({size_mb:.1f} MB)", flush=True)

    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        print(f"  ERROR during transform: {e}", file=sys.stderr)
        raise


def print_sample(path: Path) -> None:
    """Print one transformed record for visual inspection."""
    print(f"\n--- Sample record from {path.name} ---")
    with open(path) as f:
        line = f.readline()
    rec = json.loads(line)
    for k, v in rec.items():
        print(f"  {k:<16}: {v}")


def main() -> None:
    p = argparse.ArgumentParser(description="Transform tower fields for human readability")
    p.add_argument("--dry-run", action="store_true", help="Show counts only, do not write files")
    args = p.parse_args()

    project_root = Path(__file__).parent.parent

    t_total = time.time()
    for rel_path in TARGET_FILES:
        transform_file(project_root / rel_path, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"\nAll files transformed in {time.time() - t_total:.1f}s total.")
        # Show a sample from each file
        for rel_path in TARGET_FILES:
            print_sample(project_root / rel_path)
    else:
        print("\n[DRY RUN] No files were modified.")


if __name__ == "__main__":
    main()
