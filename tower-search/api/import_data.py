"""
One-time script: imports towers_geocoded_500k.jsonl into SQLite.
Usage: python import_data.py [input.jsonl] [output.db]
"""
from __future__ import annotations

import json
import math
import sqlite3
import sys
import time
from pathlib import Path

DEFAULT_INPUT = Path(__file__).resolve().parent.parent.parent / "downloads" / "towers_geocoded_500k.jsonl"
DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "towers.db"

BATCH_SIZE = 5_000

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS towers (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    tower_id_primary  TEXT,
    site_id_primary   TEXT,
    tower_id_all      TEXT,
    site_id_all       TEXT,
    latitude          REAL NOT NULL,
    longitude         REAL NOT NULL,
    provider          TEXT,
    generation        TEXT,
    site_type         TEXT,
    active            INTEGER,
    bands             TEXT,
    band_labels       TEXT,
    tower_name        TEXT,
    tower_parent      TEXT,
    first_seen        TEXT,
    last_seen         TEXT,
    rural             INTEGER,
    source            TEXT,
    address           TEXT,
    city              TEXT,
    state             TEXT,
    zipcode           TEXT,
    geocode_status    TEXT,
    geocode_distance  REAL,
    geocode_accuracy  TEXT,
    low_precision     INTEGER
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_state        ON towers(state)",
    "CREATE INDEX IF NOT EXISTS idx_city         ON towers(city)",
    "CREATE INDEX IF NOT EXISTS idx_city_state   ON towers(city, state)",
    "CREATE INDEX IF NOT EXISTS idx_generation   ON towers(generation)",
    "CREATE INDEX IF NOT EXISTS idx_site_type    ON towers(site_type)",
    "CREATE INDEX IF NOT EXISTS idx_zipcode      ON towers(zipcode)",
    "CREATE INDEX IF NOT EXISTS idx_coords       ON towers(latitude, longitude)",
    "CREATE INDEX IF NOT EXISTS idx_tower_id     ON towers(tower_id_primary)",
    "CREATE INDEX IF NOT EXISTS idx_site_id      ON towers(site_id_primary)",
    "CREATE INDEX IF NOT EXISTS idx_active       ON towers(active)",
    "CREATE INDEX IF NOT EXISTS idx_rural        ON towers(rural)",
]

CREATE_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS towers_fts USING fts5(
    address, city, state, zipcode, tower_id_primary, site_id_primary,
    content=towers,
    content_rowid=id
)
"""

POPULATE_FTS = """
INSERT INTO towers_fts(rowid, address, city, state, zipcode, tower_id_primary, site_id_primary)
SELECT id, address, city, state, zipcode, tower_id_primary, site_id_primary FROM towers
"""


def _to_list(val) -> list:
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        return [val]
    return []


def _primary(val) -> str:
    lst = _to_list(val)
    return lst[0] if lst else ""


def _parse_row(rec: dict) -> tuple:
    tower_ids = _to_list(rec.get("tower_id", []))
    site_ids = _to_list(rec.get("site_id", []))
    bands = rec.get("bands", [])
    band_labels = rec.get("band_labels", [])
    return (
        _primary(tower_ids),
        _primary(site_ids),
        json.dumps(tower_ids),
        json.dumps(site_ids),
        rec.get("latitude"),
        rec.get("longitude"),
        rec.get("provider", ""),
        rec.get("generation", ""),
        rec.get("site_type", ""),
        1 if rec.get("active") else 0,
        json.dumps(bands if isinstance(bands, list) else []),
        json.dumps(band_labels if isinstance(band_labels, list) else []),
        rec.get("tower_name", "") or "",
        rec.get("tower_parent", "") or "",
        rec.get("first_seen", "") or "",
        rec.get("last_seen", "") or "",
        1 if rec.get("rural") else 0,
        rec.get("source", "") or "",
        rec.get("address", "") or "",
        rec.get("city", "") or "",
        rec.get("state", "") or "",
        rec.get("zipcode", "") or "",
        rec.get("geocode_status", "") or "",
        rec.get("geocode_distance"),
        rec.get("geocode_accuracy", "") or "",
        1 if rec.get("low_precision") else 0,
    )


INSERT_SQL = """
INSERT INTO towers (
    tower_id_primary, site_id_primary, tower_id_all, site_id_all,
    latitude, longitude, provider, generation, site_type, active,
    bands, band_labels, tower_name, tower_parent,
    first_seen, last_seen, rural, source,
    address, city, state, zipcode,
    geocode_status, geocode_distance, geocode_accuracy, low_precision
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def main(input_path: Path, db_path: Path) -> None:
    if not input_path.exists():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    if db_path.exists():
        print(f"Removing existing database: {db_path}")
        db_path.unlink()

    print(f"Importing {input_path} → {db_path}")
    t0 = time.time()

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA cache_size=-65536")  # 64 MB

    con.execute(CREATE_TABLE)
    con.commit()

    total = 0
    batch: list[tuple] = []

    with open(input_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            batch.append(_parse_row(rec))
            if len(batch) >= BATCH_SIZE:
                con.executemany(INSERT_SQL, batch)
                con.commit()
                total += len(batch)
                batch = []
                elapsed = time.time() - t0
                print(f"  {total:,} rows inserted ({elapsed:.1f}s)…", end="\r")

    if batch:
        con.executemany(INSERT_SQL, batch)
        con.commit()
        total += len(batch)

    print(f"\n  {total:,} rows inserted total — building indexes…")

    for ddl in CREATE_INDEXES:
        con.execute(ddl)
    con.commit()
    print("  Indexes built — building FTS…")

    con.execute(CREATE_FTS)
    con.execute(POPULATE_FTS)
    con.commit()

    # FTS integrity check
    row = con.execute("SELECT count(*) FROM towers_fts").fetchone()
    print(f"  FTS index has {row[0]:,} rows")

    con.execute("PRAGMA optimize")
    con.execute("VACUUM")
    con.close()

    elapsed = time.time() - t0
    size_mb = db_path.stat().st_size / 1_048_576
    print(f"\nDone in {elapsed:.1f}s — {db_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    inp = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_INPUT
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_DB
    main(inp, out)
