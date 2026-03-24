from __future__ import annotations
from typing import Optional, Any
from pydantic import BaseModel


class TowerResult(BaseModel):
    id: int
    tower_id: str
    site_id: str
    latitude: float
    longitude: float
    provider: str
    generation: str
    site_type: str
    active: bool
    band_labels: list[str]
    tower_name: str
    tower_parent: str
    first_seen: str
    last_seen: str
    rural: bool
    source: str
    address: str
    city: str
    state: str
    zipcode: str
    geocode_status: str
    geocode_accuracy: str
    low_precision: bool


class ParsedFilters(BaseModel):
    state: Optional[str] = None
    city: Optional[str] = None
    generation: Optional[str] = None
    generation_prefix: bool = False   # True → use generation LIKE '{generation}%'
    site_type: Optional[str] = None
    provider: Optional[str] = None
    active: Optional[bool] = None
    rural: Optional[bool] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    radius_miles: float = 5.0
    tower_id: Optional[str] = None
    zipcode: Optional[str] = None
    fts_query: Optional[str] = None  # fallback free-text FTS


class DisambiguationOption(BaseModel):
    city: str
    state: str
    count: int


class AmbiguousTerm(BaseModel):
    term: str
    field: str
    options: list[DisambiguationOption]


class SearchRequest(BaseModel):
    query: str
    page: int = 1
    per_page: int = 50
    sort_by: str = "state"
    sort_order: str = "asc"
    # Caller may pre-resolve ambiguity: e.g. {"city": "Portland", "state": "OR"}
    resolved: dict[str, Any] = {}


class StructuredSearchParams(BaseModel):
    state: Optional[str] = None
    city: Optional[str] = None
    generation: Optional[str] = None
    site_type: Optional[str] = None
    provider: Optional[str] = None
    active: Optional[bool] = None
    rural: Optional[bool] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    radius_miles: float = 5.0
    tower_id: Optional[str] = None
    zipcode: Optional[str] = None
    q: Optional[str] = None
    page: int = 1
    per_page: int = 50
    sort_by: str = "state"
    sort_order: str = "asc"


class SearchResponse(BaseModel):
    parsed: ParsedFilters
    ambiguous: list[AmbiguousTerm]
    results: list[TowerResult]
    total: int
    page: int
    pages: int
    query: str
