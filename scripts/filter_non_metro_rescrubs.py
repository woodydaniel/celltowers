#!/usr/bin/env python3
"""
Filter Rescrub Towers — 250k Non-Metro Selection

Builds a 250,000-tower output from towers_rescrubs_classified.jsonl by
prioritising the least-urban areas first:

  Tier 1 — Heartland/rural states (OH, AR, IN, MO, TN, KY, AL, MS, OK, KS,
            WV, IA, NE, ND, SD, MT, WY, ID, NM, LA, MN south of Minneapolis)
  Tier 2 — Mixed states (TX, FL, GA, NC, SC, VA, PA, MI, CO, AZ, UT, OR,
            WA, WI, MN, NV, NH, ME, VT, RI, DE, AK, HI)
  Tier 3 — Major-metro states (CA, NY, NJ, IL, MA, MD, DC, CT)

Within every tier towers are sorted by their distance to the NEAREST of the
top-30 US metro centres, DESCENDING — so the most-rural towers are added
first and the outlying fringe of a big metro is consumed only once we've
exhausted smaller/rural areas.

Usage:
    python scripts/filter_non_metro_rescrubs.py
    python scripts/filter_non_metro_rescrubs.py --target 200000
    python scripts/filter_non_metro_rescrubs.py --input downloads/towers_rescrubs_classified.jsonl
    python scripts/filter_non_metro_rescrubs.py --dry-run
"""

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_TARGET = 250_000
DEFAULT_INPUT = "downloads/towers_rescrubs_classified.jsonl"
DEFAULT_OUTPUT = "downloads/rescrubs_non_metro_250k.jsonl"

# Approximate state bounding boxes: (lat_min, lat_max, lon_min, lon_max)
# Checked from smallest/most-specific first to resolve overlaps at borders.
STATE_BOUNDS: dict[str, tuple[float, float, float, float]] = {
    # Very small states first so they aren't swallowed by neighbours
    "DC": (38.79, 38.99, -77.12, -76.91),
    "DE": (38.45, 39.84, -75.79, -74.98),
    "RI": (41.10, 42.02, -71.91, -71.08),
    "CT": (40.95, 42.05, -73.73, -71.78),
    "NJ": (38.87, 41.36, -75.56, -73.88),
    "MA": (41.19, 42.89, -73.53, -69.92),
    "NH": (42.70, 45.31, -72.56, -70.61),
    "VT": (42.73, 45.02, -73.44, -71.46),
    "MD": (37.91, 39.72, -79.49, -75.05),
    # Larger eastern states
    "ME": (43.06, 47.46, -71.08, -66.95),
    "NY": (40.50, 45.02, -79.76, -71.85),
    "PA": (39.72, 42.27, -80.52, -74.69),
    "WV": (37.20, 40.64, -82.64, -77.72),
    "VA": (36.54, 39.47, -83.68, -75.16),
    "NC": (33.75, 36.59, -84.32, -75.46),
    "SC": (32.05, 35.21, -83.35, -78.54),
    "GA": (30.36, 35.00, -85.61, -80.83),
    "FL": (24.40, 31.00, -87.63, -79.97),
    "AL": (30.14, 35.01, -88.47, -84.89),
    "MS": (30.17, 35.01, -91.65, -88.10),
    "TN": (34.98, 36.68, -90.31, -81.65),
    "KY": (36.50, 39.15, -89.57, -81.96),
    "OH": (38.40, 42.32, -84.82, -80.52),
    "IN": (37.77, 41.76, -88.10, -84.79),
    "MI": (41.70, 48.31, -90.42, -82.41),
    "WI": (42.49, 47.08, -92.89, -86.25),
    "MN": (43.50, 49.38, -97.24, -89.49),
    "IA": (40.38, 43.50, -96.64, -90.14),
    "MO": (36.00, 40.62, -95.77, -89.10),
    "AR": (33.00, 36.50, -94.62, -89.64),
    "LA": (28.92, 33.02, -94.04, -88.82),
    "OK": (33.62, 37.00, -103.00, -94.43),
    "TX": (25.84, 36.50, -106.65, -93.51),
    "KS": (36.99, 40.00, -102.05, -94.59),
    "NE": (39.99, 43.00, -104.05, -95.31),
    "SD": (42.48, 45.94, -104.06, -96.44),
    "ND": (45.94, 49.00, -104.05, -96.55),
    "MT": (44.36, 49.00, -116.05, -104.04),
    "WY": (40.99, 45.01, -111.05, -104.05),
    "CO": (37.00, 41.00, -109.06, -102.04),
    "NM": (31.33, 37.00, -109.05, -103.00),
    "AZ": (31.33, 37.00, -114.82, -109.04),
    "UT": (36.99, 42.00, -114.05, -109.04),
    "NV": (35.00, 42.00, -120.00, -114.04),
    "ID": (41.99, 49.00, -117.24, -111.04),
    "OR": (41.99, 46.26, -124.57, -116.46),
    "WA": (45.54, 49.00, -124.73, -116.92),
    "CA": (32.53, 42.01, -124.41, -114.13),
    # Non-contiguous
    "AK": (54.00, 71.50, -168.00, -130.00),
    "HI": (18.90, 22.24, -160.25, -154.81),
}

# Tier definitions  -------------------------------------------------------
# Tier 1: heartland / rural states — filled first
TIER_1 = {
    "OH", "AR", "IN", "MO", "TN", "KY", "AL", "MS", "OK", "KS",
    "WV", "IA", "NE", "ND", "SD", "MT", "WY", "ID", "NM", "LA",
}

# Tier 2: mixed states with major cities AND significant rural areas
TIER_2 = {
    "TX", "FL", "GA", "NC", "SC", "VA", "PA", "MI", "WI", "MN",
    "CO", "AZ", "UT", "OR", "WA", "NV", "NH", "ME", "VT", "RI",
    "DE", "AK", "HI",
}

# Tier 3: heavy-metro states — used last, outlying fringe first
TIER_3 = {"CA", "NY", "NJ", "IL", "MA", "MD", "DC", "CT"}

# Top-30 US metro centres (lat, lon, name) — used for distance scoring
METRO_CENTRES: list[tuple[float, float, str]] = [
    (34.0522, -118.2437, "Los Angeles"),
    (40.7128, -74.0060,  "New York"),
    (41.8781, -87.6298,  "Chicago"),
    (33.7490, -84.3880,  "Atlanta"),
    (25.7617, -80.1918,  "Miami"),
    (32.7767, -96.7970,  "Dallas"),
    (29.7604, -95.3698,  "Houston"),
    (39.9526, -75.1652,  "Philadelphia"),
    (38.9072, -77.0369,  "Washington DC"),
    (42.3601, -71.0589,  "Boston"),
    (37.7749, -122.4194, "San Francisco"),
    (32.7157, -117.1611, "San Diego"),
    (28.5383, -81.3792,  "Orlando"),
    (33.4484, -112.0740, "Phoenix"),
    (36.1699, -115.1398, "Las Vegas"),
    (39.7392, -104.9903, "Denver"),
    (39.2904, -76.6122,  "Baltimore"),
    (47.6062, -122.3321, "Seattle"),
    (27.9506, -82.4572,  "Tampa"),
    (44.9778, -93.2650,  "Minneapolis"),
    (37.3382, -121.8863, "San Jose"),
    (33.9806, -117.3755, "Riverside"),
    (42.3314, -83.0458,  "Detroit"),
    (35.2271, -80.8431,  "Charlotte"),
    (39.7684, -86.1581,  "Indianapolis"),
    (38.5816, -121.4944, "Sacramento"),
    (38.6270, -90.1994,  "St Louis"),
    (40.7608, -111.8910, "Salt Lake City"),
    (29.4241, -98.4936,  "San Antonio"),
    (43.0389, -76.1497,  "Syracuse"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate great-circle distance in km (±0.5% accuracy)."""
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return r * 2 * math.asin(math.sqrt(a))


def min_dist_to_metro(lat: float, lon: float) -> float:
    """Return the km distance to the nearest metro centre."""
    return min(haversine_km(lat, lon, mc[0], mc[1]) for mc in METRO_CENTRES)


def assign_state(lat: float, lon: float) -> str:
    """Return the best-guess US state code for a lat/lon point."""
    candidates = []
    for state, (lat_min, lat_max, lon_min, lon_max) in STATE_BOUNDS.items():
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            candidates.append(state)

    if not candidates:
        return "XX"  # outside known bounds (non-CONUS / bogus)

    if len(candidates) == 1:
        return candidates[0]

    # Multiple matches (border overlap) — pick the one whose bbox centre
    # is closest to the point.
    def bbox_centre_dist(s: str) -> float:
        b = STATE_BOUNDS[s]
        clat = (b[0] + b[1]) / 2
        clon = (b[2] + b[3]) / 2
        return math.hypot(lat - clat, lon - clon)

    return min(candidates, key=bbox_centre_dist)


def assign_tier(state: str) -> int:
    if state in TIER_1:
        return 1
    if state in TIER_2:
        return 2
    if state in TIER_3:
        return 3
    return 2  # unknown / non-CONUS → treat as tier 2


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build 250k non-metro rescrub tower set")
    p.add_argument("--input",  default=DEFAULT_INPUT,  help="Source JSONL (rescrubs)")
    p.add_argument("--output", default=DEFAULT_OUTPUT, help="Destination JSONL")
    p.add_argument("--target", type=int, default=DEFAULT_TARGET,
                   help=f"Number of towers to select (default: {DEFAULT_TARGET:,})")
    p.add_argument("--dry-run", action="store_true",
                   help="Report counts only; do not write output file")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).parent.parent
    input_path  = project_root / args.input
    output_path = project_root / args.output

    if not input_path.exists():
        print(f"ERROR: {input_path} not found", file=sys.stderr)
        sys.exit(1)

    target = args.target
    print(f"Loading {input_path.name} …", flush=True)

    # ── Step 1: Read + annotate ──────────────────────────────────────────────
    records: list[dict] = []
    skipped_non_conus = 0
    state_counts: dict[str, int] = defaultdict(int)

    with open(input_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            lat = rec.get("latitude", 0.0)
            lon = rec.get("longitude", 0.0)

            state = assign_state(lat, lon)
            if state == "XX":
                skipped_non_conus += 1
                continue

            tier      = assign_tier(state)
            dist_km   = min_dist_to_metro(lat, lon)

            rec["_state"]   = state
            rec["_tier"]    = tier
            rec["_dist_km"] = dist_km
            records.append(rec)
            state_counts[state] += 1

    total_conus = len(records)
    print(f"  CONUS towers: {total_conus:,}  |  non-CONUS/bogus skipped: {skipped_non_conus:,}")

    by_tier: dict[int, int] = defaultdict(int)
    for r in records:
        by_tier[r["_tier"]] += 1
    print(f"  Tier 1 (heartland):    {by_tier[1]:>8,}")
    print(f"  Tier 2 (mixed):        {by_tier[2]:>8,}")
    print(f"  Tier 3 (major metro):  {by_tier[3]:>8,}")

    # ── Step 2: Sort — (tier asc, dist_km desc) ─────────────────────────────
    print("Sorting by tier then distance from nearest metro …", flush=True)
    records.sort(key=lambda r: (r["_tier"], -r["_dist_km"]))

    # ── Step 3: Take first `target` ──────────────────────────────────────────
    selected = records[:target]
    actual   = len(selected)
    shortfall = target - actual

    print(f"\n{'='*55}")
    print("SELECTION SUMMARY")
    print(f"{'='*55}")
    print(f"  Requested:          {target:>10,}")
    print(f"  Available (CONUS):  {total_conus:>10,}")
    print(f"  Selected:           {actual:>10,}")
    if shortfall > 0:
        print(f"  Shortfall:          {shortfall:>10,}  (all CONUS towers used)")

    # Distribution of selected by tier
    sel_tier: dict[int, int] = defaultdict(int)
    sel_state: dict[str, int] = defaultdict(int)
    for r in selected:
        sel_tier[r["_tier"]] += 1
        sel_state[r["_state"]] += 1

    print(f"\n  Tier breakdown:")
    print(f"    Tier 1 (heartland):   {sel_tier[1]:>8,}")
    print(f"    Tier 2 (mixed):       {sel_tier[2]:>8,}")
    print(f"    Tier 3 (major metro): {sel_tier[3]:>8,}")

    # Top 20 states in selection
    top_states = sorted(sel_state.items(), key=lambda x: -x[1])[:20]
    print(f"\n  Top states in selection:")
    for st, cnt in top_states:
        tier_label = "T1" if assign_tier(st) == 1 else ("T2" if assign_tier(st) == 2 else "T3")
        pct = 100 * cnt / actual
        print(f"    {st} ({tier_label}): {cnt:>7,}  ({pct:.1f}%)")

    # What was the cut-off distance?
    if actual > 0:
        last = selected[-1]
        print(f"\n  Last tower selected: tier={last['_tier']}, "
              f"state={last['_state']}, dist_to_metro={last['_dist_km']:.1f} km")

    print(f"{'='*55}")

    # ── Step 4: Write output ─────────────────────────────────────────────────
    if args.dry_run:
        print("\n[DRY RUN] No file written.")
        return

    # Strip internal scoring fields before writing
    INTERNAL_FIELDS = {"_state", "_tier", "_dist_km"}

    print(f"\nWriting {actual:,} towers to {output_path.name} …", flush=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        for rec in selected:
            clean = {k: v for k, v in rec.items() if k not in INTERNAL_FIELDS}
            f.write(json.dumps(clean, separators=(",", ":")) + "\n")

    size_mb = output_path.stat().st_size / 1024 / 1024
    print(f"Done. {output_path.name}: {actual:,} records  ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
