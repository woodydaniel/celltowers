"""
Tower Data Parser

Parses CellMapper API responses and extracts tower information
into a standardized format.

Response structure from api.cellmapper.net/v6/getTowers:
{
    "license": "...",
    "statusCode": "OKAY",
    "responseData": [
        {
            "siteID": "94879",
            "latitude": 46.748...,
            "longitude": -112.330...,
            "RAT": "LTE",
            "bandNumbers": [66, 2, 12, 71],
            "channels": [66811, 925, 5035, ...],
            "bandwidths": [15, 15, 5, ...],
            ...
        }
    ],
    "hasMore": true
}
"""

from __future__ import annotations

import json
import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


class BoundedSet:
    """
    A set with a maximum size that evicts oldest entries when full.
    
    Used for deduplication without unbounded memory growth.
    Uses OrderedDict internally for O(1) operations.
    """
    
    def __init__(self, maxsize: int = 500000):
        self._data: OrderedDict[str, None] = OrderedDict()
        self.maxsize = maxsize
        self.evictions = 0
    
    def add(self, item: str) -> bool:
        """
        Add an item to the set.
        
        Returns:
            True if item was new, False if already existed
        """
        if item in self._data:
            # Move to end (most recently seen)
            self._data.move_to_end(item)
            return False
        
        # Evict oldest if at capacity
        if len(self._data) >= self.maxsize:
            self._data.popitem(last=False)
            self.evictions += 1
        
        self._data[item] = None
        return True
    
    def __contains__(self, item: str) -> bool:
        return item in self._data
    
    def __len__(self) -> int:
        return len(self._data)
    
    def clear(self) -> None:
        self._data.clear()
        self.evictions = 0


@dataclass
class TowerRecord:
    """Standardized tower data record."""
    
    # Core identification
    tower_id: str
    site_id: str
    
    # Location
    latitude: float
    longitude: float
    
    # Network info
    provider: str
    mcc: int
    mnc: int
    
    # Technical details - now stores multiple bands per tower
    bands: list[int] = field(default_factory=list)  # e.g., [66, 2, 12, 71]
    channels: list[int] = field(default_factory=list)  # EARFCN values
    bandwidths: list[Optional[int]] = field(default_factory=list)  # MHz values
    technology: str = "LTE"  # LTE, NR (5G), etc.
    rat_subtype: str = ""  # LTE, LTE-A, etc.
    
    # Tower metadata
    tower_type: str = ""  # MACRO, SMALL_CELL, etc.
    region_id: str = ""
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    visible: bool = True
    
    # Additional tower data
    tower_name: str = ""
    tower_parent: str = ""
    cells: dict = field(default_factory=dict)
    estimated_band_data: list = field(default_factory=list)
    
    # Metadata
    scraped_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    source: str = "cellmapper"
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "tower_id": self.tower_id,
            "site_id": self.site_id,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "provider": self.provider,
            "mcc": self.mcc,
            "mnc": self.mnc,
            "bands": self.bands,
            "bands_str": ",".join(str(b) for b in self.bands),  # For CSV compatibility
            "channels": self.channels,
            "bandwidths": self.bandwidths,
            "technology": self.technology,
            "rat_subtype": self.rat_subtype,
            "tower_type": self.tower_type,
            "region_id": self.region_id,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "visible": self.visible,
            "tower_name": self.tower_name,
            "tower_parent": self.tower_parent,
            "cells": self.cells,
            "estimated_band_data": self.estimated_band_data,
            "scraped_at": self.scraped_at,
            "source": self.source,
        }
    
    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: dict) -> "TowerRecord":
        """Create from dictionary."""
        return cls(
            tower_id=data.get("tower_id", ""),
            site_id=data.get("site_id", ""),
            latitude=data.get("latitude", 0.0),
            longitude=data.get("longitude", 0.0),
            provider=data.get("provider", ""),
            mcc=data.get("mcc", 0),
            mnc=data.get("mnc", 0),
            bands=data.get("bands", []),
            channels=data.get("channels", []),
            bandwidths=data.get("bandwidths", []),
            technology=data.get("technology", "LTE"),
            rat_subtype=data.get("rat_subtype", ""),
            tower_type=data.get("tower_type", ""),
            region_id=data.get("region_id", ""),
            first_seen=data.get("first_seen"),
            last_seen=data.get("last_seen"),
            visible=data.get("visible", True),
            tower_name=data.get("tower_name", ""),
            tower_parent=data.get("tower_parent", ""),
            cells=data.get("cells", {}),
            estimated_band_data=data.get("estimated_band_data", []),
            scraped_at=data.get("scraped_at", datetime.utcnow().isoformat()),
            source=data.get("source", "cellmapper"),
        )
    
    def is_valid(self) -> bool:
        """Check if record has minimum required data."""
        return (
            self.site_id and
            -90 <= self.latitude <= 90 and
            -180 <= self.longitude <= 180 and
            self.latitude != 0 and
            self.longitude != 0
        )


# Provider name mappings
PROVIDER_NAMES = {
    # Verizon
    (311, 480): "Verizon", (311, 481): "Verizon", (311, 482): "Verizon",
    (311, 483): "Verizon", (311, 484): "Verizon", (311, 485): "Verizon",
    (311, 486): "Verizon", (311, 487): "Verizon", (311, 488): "Verizon",
    (311, 489): "Verizon", (310, 4): "Verizon", (310, 10): "Verizon",
    (310, 12): "Verizon", (310, 13): "Verizon",
    (311, 270): "Verizon", (311, 271): "Verizon", (311, 272): "Verizon",
    (311, 273): "Verizon", (311, 274): "Verizon", (311, 275): "Verizon",
    (311, 276): "Verizon", (311, 277): "Verizon", (311, 278): "Verizon",
    (311, 279): "Verizon", (311, 280): "Verizon", (311, 281): "Verizon",
    (311, 282): "Verizon", (311, 283): "Verizon", (311, 284): "Verizon",
    (311, 285): "Verizon", (311, 286): "Verizon", (311, 287): "Verizon",
    (311, 288): "Verizon", (311, 289): "Verizon",
    
    # AT&T
    (310, 410): "AT&T", (310, 150): "AT&T", (310, 170): "AT&T",
    (310, 380): "AT&T", (310, 560): "AT&T", (310, 680): "AT&T",
    (310, 980): "AT&T", (311, 180): "AT&T", (313, 100): "AT&T FirstNet",
    
    # T-Mobile
    (310, 160): "T-Mobile", (310, 200): "T-Mobile", (310, 210): "T-Mobile",
    (310, 220): "T-Mobile", (310, 230): "T-Mobile", (310, 240): "T-Mobile",
    (310, 250): "T-Mobile", (310, 260): "T-Mobile", (310, 270): "T-Mobile",
    (310, 310): "T-Mobile", (310, 490): "T-Mobile", (310, 580): "T-Mobile",
    (310, 660): "T-Mobile", (311, 490): "T-Mobile", (311, 882): "T-Mobile",
    (312, 250): "T-Mobile", (316, 10): "T-Mobile",
}


def get_provider_name(mcc: int, mnc: int) -> str:
    """Get friendly provider name from MCC/MNC."""
    return PROVIDER_NAMES.get((mcc, mnc), f"Unknown ({mcc}-{mnc})")


# Band to frequency mapping for reference
BAND_FREQUENCIES = {
    2: "1900 MHz (PCS)",
    4: "1700/2100 MHz (AWS-1)",
    5: "850 MHz (Cellular)",
    12: "700 MHz (Lower A/B/C)",
    13: "700 MHz (Upper C - Verizon)",
    14: "700 MHz (FirstNet)",
    17: "700 MHz (Lower B/C)",
    25: "1900 MHz (Extended PCS)",
    26: "850 MHz (Extended)",
    30: "2300 MHz (WCS)",
    41: "2500 MHz (BRS/EBS - Sprint/T-Mobile)",
    46: "5 GHz (LAA)",
    48: "3.5 GHz (CBRS)",
    66: "1700/2100 MHz (AWS-3)",
    71: "600 MHz (T-Mobile)",
    # 5G NR bands
    "n2": "1900 MHz",
    "n5": "850 MHz",
    "n25": "1900 MHz",
    "n41": "2500 MHz",
    "n66": "1700/2100 MHz",
    "n71": "600 MHz",
    "n77": "3.7 GHz (C-Band)",
    "n78": "3.5 GHz (CBRS)",
    "n260": "39 GHz (mmWave)",
    "n261": "28 GHz (mmWave)",
}


def get_band_frequency(band: int) -> str:
    """Get frequency description for a band number."""
    return BAND_FREQUENCIES.get(band, f"Band {band}")


class TowerParser:
    """
    Parser for CellMapper API responses.
    
    Handles the exact response format from api.cellmapper.net/v6/getTowers
    
    Uses bounded memory for deduplication to prevent OOM on large scrapes.
    Default limit: 500,000 unique tower IDs (~50MB memory)
    """
    
    def __init__(self, max_seen_ids: int = 500000):
        self.parsed_count = 0
        self.error_count = 0
        self.duplicate_count = 0
        self._seen_ids = BoundedSet(maxsize=max_seen_ids)
    
    def parse_towers_response(
        self,
        response_data: Any,
        mcc: int,
        mnc: int,
        technology: str = "LTE",
    ) -> tuple[list[TowerRecord], bool]:
        """
        Parse tower data from CellMapper API response.
        
        Args:
            response_data: Raw API response dict
            mcc: Mobile Country Code
            mnc: Mobile Network Code
            technology: Network technology (LTE, NR)
            
        Returns:
            Tuple of (list of TowerRecord objects, has_more flag)
        """
        records = []
        has_more = False
        
        if response_data is None:
            logger.warning("Received null response data")
            return records, has_more
        
        if not isinstance(response_data, dict):
            logger.warning(f"Unexpected response type: {type(response_data)}")
            return records, has_more
        
        # Check status
        status = response_data.get("statusCode", "")
        if status != "OKAY":
            logger.warning(f"API returned status: {status}")
            return records, has_more
        
        # Check for pagination
        has_more = response_data.get("hasMore", False)
        
        # Get tower list
        towers_list = response_data.get("responseData", [])
        
        if not towers_list:
            logger.debug("No towers in responseData")
            return records, has_more
        
        provider = get_provider_name(mcc, mnc)
        
        for tower_data in towers_list:
            try:
                record = self._parse_single_tower(
                    tower_data, mcc, mnc, provider, technology
                )
                
                if record and record.is_valid():
                    # Create unique key for deduplication
                    unique_key = f"{mcc}_{mnc}_{record.site_id}"
                    
                    # BoundedSet.add() returns True if new, False if duplicate
                    if self._seen_ids.add(unique_key):
                        records.append(record)
                        self.parsed_count += 1
                    else:
                        self.duplicate_count += 1
                        
            except Exception as e:
                self.error_count += 1
                logger.debug(f"Failed to parse tower: {e}")
        
        logger.info(f"Parsed {len(records)} towers from response (hasMore={has_more})")
        return records, has_more
    
    def _parse_single_tower(
        self,
        data: dict,
        mcc: int,
        mnc: int,
        provider: str,
        technology: str,
    ) -> Optional[TowerRecord]:
        """
        Parse a single tower from CellMapper response data.
        
        Expected fields:
        - siteID: "94879"
        - latitude: 46.748...
        - longitude: -112.330...
        - RAT: "LTE"
        - RATSubType: "LTE" or "LTE-A"
        - bandNumbers: [66, 2, 12, 71]
        - channels: [66811, 925, 5035, ...]
        - bandwidths: [15, 15, 5, ...]
        - regionID: "17400"
        - visible: true
        - firstseendate: 1471132800000 (Unix ms)
        - lastseendate: 1753660800000
        - towerAttributes: { "TOWER_TYPE": "MACRO", ... }
        """
        if not isinstance(data, dict):
            return None
        
        # Required fields
        site_id = str(data.get("siteID", ""))
        lat = data.get("latitude")
        lon = data.get("longitude")
        
        if not site_id or lat is None or lon is None:
            return None
        
        # Band information
        band_numbers = data.get("bandNumbers", [])
        channels = data.get("channels", [])
        bandwidths = data.get("bandwidths", [])
        
        # Clean up bandwidths (may contain null values)
        bandwidths = [bw if bw is not None else 0 for bw in bandwidths]
        
        # Technology info
        rat = data.get("RAT", technology)
        rat_subtype = data.get("RATSubType", "")
        
        # Metadata
        region_id = str(data.get("regionID", ""))
        visible = data.get("visible", True)
        
        # Timestamps (convert from Unix ms to ISO format)
        first_seen = self._timestamp_to_iso(data.get("firstseendate"))
        last_seen = self._timestamp_to_iso(data.get("lastseendate"))
        
        # Tower attributes
        tower_attrs = data.get("towerAttributes", {})
        tower_type = tower_attrs.get("TOWER_TYPE", "")
        tower_name = tower_attrs.get("TOWER_NAME", "")
        tower_parent = tower_attrs.get("TOWER_PARENT", "")
        
        # Cell-level data and estimated band data
        cells = data.get("cells", {})
        estimated_band_data = data.get("estimatedBandData", [])
        
        # Create unique tower ID
        tower_id = f"{mcc}_{mnc}_{site_id}"
        
        return TowerRecord(
            tower_id=tower_id,
            site_id=site_id,
            latitude=float(lat),
            longitude=float(lon),
            provider=provider,
            mcc=mcc,
            mnc=mnc,
            bands=band_numbers,
            channels=channels,
            bandwidths=bandwidths,
            technology=rat,
            rat_subtype=rat_subtype,
            tower_type=tower_type,
            region_id=region_id,
            first_seen=first_seen,
            last_seen=last_seen,
            visible=visible,
            tower_name=tower_name,
            tower_parent=tower_parent,
            cells=cells,
            estimated_band_data=estimated_band_data,
        )
    
    def _timestamp_to_iso(self, timestamp_ms: Optional[int]) -> Optional[str]:
        """Convert Unix timestamp in milliseconds to ISO format string."""
        if timestamp_ms is None:
            return None
        try:
            dt = datetime.utcfromtimestamp(timestamp_ms / 1000)
            return dt.isoformat()
        except (ValueError, OSError):
            return None
    
    def reset_deduplication(self) -> None:
        """Reset the seen IDs set for a new scraping session."""
        self._seen_ids.clear()
        self.parsed_count = 0
        self.error_count = 0
        self.duplicate_count = 0
    
    def get_stats(self) -> dict:
        """Get parsing statistics."""
        return {
            "parsed_count": self.parsed_count,
            "error_count": self.error_count,
            "duplicate_count": self.duplicate_count,
            "unique_towers": len(self._seen_ids),
            "dedup_evictions": self._seen_ids.evictions,
        }


def format_bands_for_display(bands: list[int]) -> str:
    """Format band numbers with their frequencies for display."""
    parts = []
    for band in bands:
        freq = BAND_FREQUENCIES.get(band, "")
        if freq:
            parts.append(f"B{band} ({freq})")
        else:
            parts.append(f"B{band}")
    return ", ".join(parts)
