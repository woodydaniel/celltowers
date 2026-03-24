#!/usr/bin/env python3
"""
Merge tower records by carrier + location.

Groups records by (provider, latitude, longitude) and merges them:
- technology: list of unique values
- bands, channels, bandwidths: combined unique lists
- site_id, tower_id: list of unique values
- first_seen: earliest date
- last_seen: latest date
- Removes 'source' field

Usage:
    python scripts/merge_towers.py --input downloads/extracted/data/towers --output downloads/towers_merged.jsonl
"""

import argparse
import json
import glob
from collections import defaultdict
from pathlib import Path
from typing import Any


def merge_records(records: list[dict]) -> dict:
    """Merge multiple tower records into one."""
    if len(records) == 1:
        merged = records[0].copy()
        # Convert single values to lists for consistency
        merged['technology'] = [merged['technology']]
        merged['site_id'] = [merged['site_id']]
        merged['tower_id'] = [merged['tower_id']]
        # Remove source field
        merged.pop('source', None)
        return merged
    
    # Start with first record as base
    merged = records[0].copy()
    
    # Collect unique technologies
    technologies = list(dict.fromkeys(r['technology'] for r in records))
    merged['technology'] = technologies
    
    # Collect unique site_ids and tower_ids
    site_ids = list(dict.fromkeys(r['site_id'] for r in records))
    tower_ids = list(dict.fromkeys(r['tower_id'] for r in records))
    merged['site_id'] = site_ids
    merged['tower_id'] = tower_ids
    
    # Combine bands (unique, sorted, filter None)
    all_bands = []
    for r in records:
        all_bands.extend(b for b in r.get('bands', []) if b is not None)
    merged['bands'] = sorted(set(all_bands))
    merged['bands_str'] = ','.join(str(b) for b in merged['bands'])
    
    # Combine channels (unique, sorted, filter None)
    all_channels = []
    for r in records:
        all_channels.extend(c for c in r.get('channels', []) if c is not None)
    merged['channels'] = sorted(set(all_channels))
    
    # Combine bandwidths (unique, sorted, filter None)
    all_bandwidths = []
    for r in records:
        all_bandwidths.extend(bw for bw in r.get('bandwidths', []) if bw is not None)
    merged['bandwidths'] = sorted(set(all_bandwidths))
    
    # first_seen: earliest date
    first_seens = [r.get('first_seen') for r in records if r.get('first_seen')]
    if first_seens:
        merged['first_seen'] = min(first_seens)
    
    # last_seen: latest date
    last_seens = [r.get('last_seen') for r in records if r.get('last_seen')]
    if last_seens:
        merged['last_seen'] = max(last_seens)
    
    # tower_name, tower_parent: keep first non-empty
    for field in ['tower_name', 'tower_parent']:
        for r in records:
            if r.get(field):
                merged[field] = r[field]
                break
    
    # cells: merge dicts
    all_cells = {}
    for r in records:
        if r.get('cells'):
            all_cells.update(r['cells'])
    merged['cells'] = all_cells
    
    # estimated_band_data: combine lists, dedupe by bandNumber
    all_band_data = []
    seen_bands = set()
    for r in records:
        for bd in r.get('estimated_band_data', []):
            band_num = bd.get('bandNumber')
            if band_num not in seen_bands:
                seen_bands.add(band_num)
                all_band_data.append(bd)
    merged['estimated_band_data'] = all_band_data
    
    # Remove source field
    merged.pop('source', None)
    
    # Keep scraped_at from most recent record
    scraped_ats = [r.get('scraped_at') for r in records if r.get('scraped_at')]
    if scraped_ats:
        merged['scraped_at'] = max(scraped_ats)
    
    return merged


def main():
    parser = argparse.ArgumentParser(description='Merge tower records by carrier + location')
    parser.add_argument('--input', '-i', required=True, help='Input directory with tower JSONL files')
    parser.add_argument('--output', '-o', required=True, help='Output JSONL file')
    args = parser.parse_args()
    
    input_dir = Path(args.input)
    output_file = Path(args.output)
    
    # Read all records
    print(f"Reading tower files from {input_dir}...")
    all_records = []
    input_files = sorted(glob.glob(str(input_dir / '*.jsonl')))
    
    for filepath in input_files:
        filename = Path(filepath).name
        count = 0
        with open(filepath, 'r') as f:
            for line in f:
                if line.strip():
                    all_records.append(json.loads(line))
                    count += 1
        print(f"  {filename}: {count:,} records")
    
    print(f"\nTotal input records: {len(all_records):,}")
    
    # Group by (provider, lat, lon)
    print("\nGrouping by (provider, latitude, longitude)...")
    groups: dict[tuple, list[dict]] = defaultdict(list)
    
    for rec in all_records:
        key = (
            rec['provider'],
            round(rec['latitude'], 6),
            round(rec['longitude'], 6)
        )
        groups[key].append(rec)
    
    print(f"Unique locations: {len(groups):,}")
    
    # Count groups with multiple records
    multi_record_groups = sum(1 for recs in groups.values() if len(recs) > 1)
    total_merged = sum(len(recs) - 1 for recs in groups.values() if len(recs) > 1)
    print(f"Locations with multiple records: {multi_record_groups:,}")
    print(f"Records that will be merged: {total_merged:,}")
    
    # Merge each group
    print("\nMerging records...")
    merged_records = []
    for key, recs in groups.items():
        merged = merge_records(recs)
        merged_records.append(merged)
    
    # Sort by provider, then lat, then lon
    merged_records.sort(key=lambda r: (r['provider'], r['latitude'], r['longitude']))
    
    # Write output
    print(f"\nWriting {len(merged_records):,} merged records to {output_file}...")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        for rec in merged_records:
            f.write(json.dumps(rec) + '\n')
    
    # Summary statistics
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Input records:  {len(all_records):,}")
    print(f"Output records: {len(merged_records):,}")
    print(f"Records merged: {total_merged:,}")
    print(f"Output file:    {output_file}")
    print(f"Output size:    {output_file.stat().st_size / 1024 / 1024:.1f} MB")
    
    # Technology breakdown
    tech_counts = defaultdict(int)
    for rec in merged_records:
        techs = tuple(sorted(rec['technology']))
        tech_counts[techs] += 1
    
    print("\nTechnology combinations:")
    for techs, count in sorted(tech_counts.items(), key=lambda x: -x[1]):
        print(f"  {list(techs)}: {count:,}")


if __name__ == '__main__':
    main()
