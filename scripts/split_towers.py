#!/usr/bin/env python3
"""
Split towers_all_classified.jsonl into urban and rural files.

Run this whenever you're ready to regenerate the split files.

  python scripts/split_towers.py

Output:
  downloads/towers_urban.jsonl  -- rural=false records
  downloads/towers_rural.jsonl  -- rural=true  records
"""

import json
from pathlib import Path

BASE_DIR    = Path(__file__).resolve().parent.parent
MASTER_FILE = BASE_DIR / "downloads" / "towers_all_classified.jsonl"
URBAN_FILE  = BASE_DIR / "downloads" / "towers_urban.jsonl"
RURAL_FILE  = BASE_DIR / "downloads" / "towers_rural.jsonl"


def main():
    if not MASTER_FILE.exists():
        print(f"ERROR: {MASTER_FILE} not found")
        return

    print(f"Reading {MASTER_FILE.name} ...")
    urban_count = rural_count = 0

    with open(MASTER_FILE) as src, \
         open(URBAN_FILE, "w") as urban_fh, \
         open(RURAL_FILE, "w") as rural_fh:

        for raw in src:
            raw = raw.strip()
            if not raw:
                continue
            rec = json.loads(raw)
            if rec.get("rural", True):
                rural_fh.write(raw + "\n")
                rural_count += 1
            else:
                urban_fh.write(raw + "\n")
                urban_count += 1

    print(f"  {URBAN_FILE.name}  {urban_count:,} records")
    print(f"  {RURAL_FILE.name}  {rural_count:,} records")
    print("Done.")


if __name__ == "__main__":
    main()
