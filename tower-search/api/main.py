"""
FastAPI backend for Cell Tower Search.
Serves /api/* endpoints and the React frontend as static files.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import database as db
from models import (
    ParsedFilters,
    SearchRequest,
    SearchResponse,
    StructuredSearchParams,
)
from query_parser import QueryParser

STATIC_DIR = Path(__file__).parent / "static"
parser = QueryParser()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load city/state lookup data from DB for the query parser
    city_counts = await db.get_city_counts()
    parser.init_from_db(city_counts)
    print(f"Query parser initialized with {len(city_counts)} city/state pairs")
    yield


app = FastAPI(title="Cell Tower Search API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.post("/api/search", response_model=SearchResponse)
async def smart_search(req: SearchRequest):
    """Natural language search endpoint."""
    filters, ambiguous = parser.parse(req.query, resolved=req.resolved)

    # If still ambiguous (and no pre-resolution), return disambiguation options
    # without fetching results yet
    if ambiguous and not req.resolved:
        return SearchResponse(
            parsed=filters,
            ambiguous=ambiguous,
            results=[],
            total=0,
            page=req.page,
            pages=0,
            query=req.query,
        )

    results, total = await db.search_towers(
        filters,
        page=req.page,
        per_page=req.per_page,
        sort_by=req.sort_by,
        sort_order=req.sort_order,
    )
    pages = max(1, (total + req.per_page - 1) // req.per_page)
    return SearchResponse(
        parsed=filters,
        ambiguous=[],
        results=results,
        total=total,
        page=req.page,
        pages=pages,
        query=req.query,
    )


@app.get("/api/towers")
async def structured_search(
    state: Optional[str] = None,
    city: Optional[str] = None,
    generation: Optional[str] = None,
    site_type: Optional[str] = None,
    provider: Optional[str] = None,
    active: Optional[bool] = None,
    rural: Optional[bool] = None,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    radius_miles: float = 5.0,
    tower_id: Optional[str] = None,
    zipcode: Optional[str] = None,
    q: Optional[str] = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    sort_by: str = "state",
    sort_order: str = "asc",
):
    """Structured search — used by the refine panel and after disambiguation."""
    gen_prefix = False
    if generation:
        gen_prefix = generation in ("5G", "4G")

    filters = ParsedFilters(
        state=state,
        city=city,
        generation=generation,
        generation_prefix=gen_prefix,
        site_type=site_type,
        provider=provider,
        active=active,
        rural=rural,
        lat=lat,
        lng=lng,
        radius_miles=radius_miles,
        tower_id=tower_id,
        zipcode=zipcode,
        fts_query=q,
    )
    results, total = await db.search_towers(filters, page, per_page, sort_by, sort_order)
    pages = max(1, (total + per_page - 1) // per_page)
    return {
        "results": [r.model_dump() for r in results],
        "total": total,
        "page": page,
        "pages": pages,
    }


@app.get("/api/suggest")
async def suggest(q: str = Query("", min_length=1)):
    """Autocomplete suggestions for the search bar."""
    q_lower = q.lower().strip()
    suggestions: list[str] = []

    # State matches
    from query_parser import US_STATES, STATE_ABBREV_TO_NAME
    for name, abbrev in US_STATES.items():
        if name.startswith(q_lower) or abbrev.lower() == q_lower:
            suggestions.append(name.title())
        if len(suggestions) >= 3:
            break

    # City matches from parser
    if len(q) >= 2:
        from rapidfuzz import process, fuzz
        city_matches = process.extract(
            q_lower,
            parser._all_cities,
            scorer=fuzz.WRatio,
            limit=5,
            score_cutoff=70,
        )
        for city_lower, score, _ in city_matches:
            entries = parser._city_index.get(city_lower, [])
            if entries:
                city, state, _ = entries[0]
                label = f"{city}, {state}"
                if label not in suggestions:
                    suggestions.append(label)

    return {"suggestions": suggestions[:8]}


@app.get("/api/filters")
async def get_filters():
    """Returns distinct values for UI filter dropdowns."""
    return await db.get_distinct_values()


@app.get("/api/stats")
async def get_stats():
    """Returns dataset statistics."""
    return await db.get_stats()


@app.get("/api/towers/export")
async def export_csv(
    state: Optional[str] = None,
    city: Optional[str] = None,
    generation: Optional[str] = None,
    site_type: Optional[str] = None,
    active: Optional[bool] = None,
    rural: Optional[bool] = None,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    radius_miles: float = 5.0,
    tower_id: Optional[str] = None,
    zipcode: Optional[str] = None,
):
    gen_prefix = generation in ("5G", "4G") if generation else False
    filters = ParsedFilters(
        state=state, city=city, generation=generation, generation_prefix=gen_prefix,
        site_type=site_type, active=active, rural=rural,
        lat=lat, lng=lng, radius_miles=radius_miles,
        tower_id=tower_id, zipcode=zipcode,
    )
    csv_data = await db.export_towers_csv(filters)
    filename = "towers_export.csv"
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Serve React frontend (must be last)
# ---------------------------------------------------------------------------

if STATIC_DIR.exists():
    from fastapi.responses import FileResponse

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = STATIC_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")
