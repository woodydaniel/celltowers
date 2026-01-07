"""
Data Storage Module

Handles persisting tower data to various formats:
- JSON Lines (.jsonl) for efficient append operations
- CSV for spreadsheet compatibility
- SQLite for structured queries
"""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
from pathlib import Path
from typing import Optional

from config.settings import OUTPUT_FILES, TOWERS_DIR, get_carrier_output_files
from .parser import TowerRecord

logger = logging.getLogger(__name__)


class DataStorage:
    """
    Multi-format data storage for tower records.
    
    Supports JSONL, CSV, and SQLite outputs with deduplication.
    
    For parallel carrier scraping, pass carrier= to use carrier-specific
    output files (e.g., towers_tmobile.jsonl).
    """
    
    def __init__(
        self,
        output_dir: Path = TOWERS_DIR,
        format: str = "jsonl",
        enable_dedup: bool = True,
        carrier: str = "",
        run_tag: str = "",
    ):
        self.output_dir = output_dir
        self.format = format.lower()
        self.enable_dedup = enable_dedup
        self.carrier = carrier
        self.run_tag = run_tag
        
        # Use carrier-specific output files if carrier is set
        if carrier:
            self._output_files = get_carrier_output_files(carrier, run_tag=run_tag)
            suffix = f"_{run_tag}" if run_tag else ""
            logger.info(f"Using carrier-specific output: towers_{carrier}{suffix}.{format}")
        else:
            self._output_files = OUTPUT_FILES
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # File handles
        self._jsonl_file = None
        self._csv_file = None
        self._csv_writer = None
        self._sqlite_conn = None
        
        # Deduplication tracking
        self._seen_ids: set[str] = set()
        
        # Statistics
        self.records_written = 0
        self.duplicates_skipped = 0
    
    def __enter__(self) -> "DataStorage":
        """Context manager entry."""
        self._open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
    
    def _open(self) -> None:
        """Open storage connections."""
        if self.format == "jsonl":
            self._open_jsonl()
        elif self.format == "csv":
            self._open_csv()
        elif self.format == "sqlite":
            self._open_sqlite()
        else:
            raise ValueError(f"Unsupported format: {self.format}")
        
        if self.enable_dedup:
            self._load_existing_ids()
    
    def _open_jsonl(self) -> None:
        """Open JSONL file for appending."""
        filepath = self._output_files.get("jsonl", self.output_dir / "towers.jsonl")
        self._jsonl_file = open(filepath, "a", encoding="utf-8")
        logger.info(f"Opened JSONL file: {filepath}")
    
    def _open_csv(self) -> None:
        """Open CSV file for writing."""
        filepath = self._output_files.get("csv", self.output_dir / "towers.csv")
        file_exists = filepath.exists() and filepath.stat().st_size > 0
        
        self._csv_file = open(filepath, "a", newline="", encoding="utf-8")
        self._csv_writer = csv.writer(self._csv_file)
        
        # Write header if new file
        if not file_exists:
            self._csv_writer.writerow([
                "tower_id", "site_id", "latitude", "longitude",
                "provider", "mcc", "mnc", "band", "frequency",
                "technology", "cell_id", "lac", "tac", "pci",
                "earfcn", "scraped_at", "source"
            ])
        
        logger.info(f"Opened CSV file: {filepath}")
    
    def _open_sqlite(self) -> None:
        """Open SQLite database."""
        filepath = self._output_files.get("sqlite", self.output_dir / "towers.db")
        self._sqlite_conn = sqlite3.connect(filepath)
        
        # Create table if not exists
        self._sqlite_conn.execute("""
            CREATE TABLE IF NOT EXISTS towers (
                tower_id TEXT PRIMARY KEY,
                site_id TEXT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                provider TEXT NOT NULL,
                mcc INTEGER NOT NULL,
                mnc INTEGER NOT NULL,
                band INTEGER,
                frequency TEXT,
                technology TEXT,
                cell_id INTEGER,
                lac INTEGER,
                tac INTEGER,
                pci INTEGER,
                earfcn INTEGER,
                scraped_at TEXT,
                source TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for common queries
        self._sqlite_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_provider ON towers(provider)"
        )
        self._sqlite_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_location ON towers(latitude, longitude)"
        )
        self._sqlite_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mcc_mnc ON towers(mcc, mnc)"
        )
        
        self._sqlite_conn.commit()
        logger.info(f"Opened SQLite database: {filepath}")
    
    def _load_existing_ids(self) -> None:
        """Load existing tower IDs for deduplication."""
        count = 0
        
        if self.format == "jsonl":
            filepath = self._output_files.get("jsonl", self.output_dir / "towers.jsonl")
            if filepath.exists():
                with open(filepath, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            data = json.loads(line.strip())
                            if "tower_id" in data:
                                self._seen_ids.add(data["tower_id"])
                                count += 1
                        except json.JSONDecodeError:
                            continue
        
        elif self.format == "csv":
            filepath = self._output_files.get("csv", self.output_dir / "towers.csv")
            if filepath.exists():
                with open(filepath, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if "tower_id" in row:
                            self._seen_ids.add(row["tower_id"])
                            count += 1
        
        elif self.format == "sqlite":
            cursor = self._sqlite_conn.execute("SELECT tower_id FROM towers")
            for row in cursor:
                self._seen_ids.add(row[0])
                count += 1
        
        if count > 0:
            logger.info(f"Loaded {count} existing tower IDs for deduplication")
    
    def write(self, record: TowerRecord) -> bool:
        """
        Write a single tower record.
        
        Args:
            record: TowerRecord to write
            
        Returns:
            True if written, False if skipped (duplicate)
        """
        # Check for duplicate
        if self.enable_dedup and record.tower_id in self._seen_ids:
            self.duplicates_skipped += 1
            return False
        
        # Mark as seen
        self._seen_ids.add(record.tower_id)
        
        if self.format == "jsonl":
            self._write_jsonl(record)
        elif self.format == "csv":
            self._write_csv(record)
        elif self.format == "sqlite":
            self._write_sqlite(record)
        
        self.records_written += 1
        return True
    
    def write_many(self, records: list[TowerRecord]) -> int:
        """
        Write multiple tower records.
        
        Args:
            records: List of TowerRecord objects
            
        Returns:
            Number of records actually written
        """
        written = 0
        for record in records:
            if self.write(record):
                written += 1
        return written
    
    def _write_jsonl(self, record: TowerRecord) -> None:
        """Write record as JSON line."""
        self._jsonl_file.write(record.to_json() + "\n")
        self._jsonl_file.flush()
    
    def _write_csv(self, record: TowerRecord) -> None:
        """Write record as CSV row."""
        data = record.to_dict()
        self._csv_writer.writerow([
            data["tower_id"], data["site_id"], data["latitude"], data["longitude"],
            data["provider"], data["mcc"], data["mnc"], data["band"], data["frequency"],
            data["technology"], data["cell_id"], data["lac"], data["tac"], data["pci"],
            data["earfcn"], data["scraped_at"], data["source"]
        ])
        self._csv_file.flush()
    
    def _write_sqlite(self, record: TowerRecord) -> None:
        """Write record to SQLite."""
        data = record.to_dict()
        self._sqlite_conn.execute("""
            INSERT OR REPLACE INTO towers (
                tower_id, site_id, latitude, longitude, provider,
                mcc, mnc, band, frequency, technology, cell_id,
                lac, tac, pci, earfcn, scraped_at, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["tower_id"], data["site_id"], data["latitude"], data["longitude"],
            data["provider"], data["mcc"], data["mnc"], data["band"], data["frequency"],
            data["technology"], data["cell_id"], data["lac"], data["tac"], data["pci"],
            data["earfcn"], data["scraped_at"], data["source"]
        ))
        self._sqlite_conn.commit()
    
    def close(self) -> None:
        """Close all storage connections."""
        if self._jsonl_file:
            self._jsonl_file.close()
            self._jsonl_file = None
        
        if self._csv_file:
            self._csv_file.close()
            self._csv_file = None
            self._csv_writer = None
        
        if self._sqlite_conn:
            self._sqlite_conn.close()
            self._sqlite_conn = None
        
        logger.info(f"Storage closed. Written: {self.records_written}, Skipped: {self.duplicates_skipped}")
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        return {
            "format": self.format,
            "records_written": self.records_written,
            "duplicates_skipped": self.duplicates_skipped,
            "unique_ids_tracked": len(self._seen_ids),
        }


class MultiFormatStorage:
    """
    Write to multiple formats simultaneously.
    """
    
    def __init__(
        self,
        output_dir: Path = TOWERS_DIR,
        formats: Optional[list[str]] = None,
    ):
        self.output_dir = output_dir
        self.formats = formats or ["jsonl"]
        self.storages: list[DataStorage] = []
    
    def __enter__(self) -> "MultiFormatStorage":
        """Open all storage formats."""
        for fmt in self.formats:
            storage = DataStorage(
                output_dir=self.output_dir,
                format=fmt,
                enable_dedup=True,
            )
            storage._open()
            self.storages.append(storage)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close all storages."""
        for storage in self.storages:
            storage.close()
    
    def write(self, record: TowerRecord) -> None:
        """Write to all formats."""
        for storage in self.storages:
            storage.write(record)
    
    def write_many(self, records: list[TowerRecord]) -> None:
        """Write multiple records to all formats."""
        for record in records:
            self.write(record)


def export_to_geojson(
    input_file: Path,
    output_file: Optional[Path] = None,
) -> Path:
    """
    Convert JSONL or CSV to GeoJSON format.
    
    Args:
        input_file: Path to input file (jsonl or csv)
        output_file: Path for output GeoJSON (defaults to same name with .geojson)
        
    Returns:
        Path to output file
    """
    if output_file is None:
        output_file = input_file.with_suffix(".geojson")
    
    features = []
    
    suffix = input_file.suffix.lower()
    
    if suffix == ".jsonl":
        with open(input_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    features.append(_record_to_geojson_feature(data))
                except json.JSONDecodeError:
                    continue
    
    elif suffix == ".csv":
        with open(input_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert types
                row["latitude"] = float(row["latitude"])
                row["longitude"] = float(row["longitude"])
                row["mcc"] = int(row["mcc"])
                row["mnc"] = int(row["mnc"])
                features.append(_record_to_geojson_feature(row))
    
    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2)
    
    logger.info(f"Exported {len(features)} features to {output_file}")
    return output_file


def _record_to_geojson_feature(data: dict) -> dict:
    """Convert a tower record to GeoJSON feature."""
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [data["longitude"], data["latitude"]],
        },
        "properties": {
            "tower_id": data.get("tower_id"),
            "provider": data.get("provider"),
            "band": data.get("band"),
            "frequency": data.get("frequency"),
            "technology": data.get("technology"),
            "mcc": data.get("mcc"),
            "mnc": data.get("mnc"),
        },
    }

