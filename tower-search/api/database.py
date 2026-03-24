"""
Async SQLite query layer for tower search.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Optional

import aiosqlite
from contextlib import asynccontextmanager

from models import ParsedFilters, TowerResult

DB_PATH = Path(os.getenv("TOWERS_DB_PATH", str(
    Path(__file__).resolve().parent.parent / "data" / "towers.db"
)))

VALID_SORT_COLS = {"state", "city", "generation", "site_type", "first_seen",
                   "last_seen", "geocode_distance", "active", "provider"}
VALID_SORT_ORDERS = {"asc", "desc"}


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in miles between two lat/lon points."""
    R = 3_958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@asynccontextmanager
async def _get_conn():
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA cache_size=-32768")
        await conn.create_function("haversine", 4, _haversine)
        yield conn


def _row_to_tower(row: aiosqlite.Row) -> TowerResult:
    def _parse_list(val: str | None) -> list:
        if not val:
            return []
        try:
            return json.loads(val)
        except Exception:
            return []

    return TowerResult(
        id=row["id"],
        tower_id=row["tower_id_primary"] or "",
        site_id=row["site_id_primary"] or "",
        latitude=row["latitude"],
        longitude=row["longitude"],
        provider=row["provider"] or "",
        generation=row["generation"] or "",
        site_type=row["site_type"] or "",
        active=bool(row["active"]),
        band_labels=_parse_list(row["band_labels"]),
        tower_name=row["tower_name"] or "",
        tower_parent=row["tower_parent"] or "",
        first_seen=row["first_seen"] or "",
        last_seen=row["last_seen"] or "",
        rural=bool(row["rural"]),
        source=row["source"] or "",
        address=row["address"] or "",
        city=row["city"] or "",
        state=row["state"] or "",
        zipcode=row["zipcode"] or "",
        geocode_status=row["geocode_status"] or "",
        geocode_accuracy=row["geocode_accuracy"] or "",
        low_precision=bool(row["low_precision"]),
    )


def _build_where(filters: ParsedFilters) -> tuple[str, list[Any]]:
    """Build WHERE clause and params from filters. Returns (sql_fragment, params)."""
    clauses: list[str] = []
    params: list[Any] = []

    if filters.tower_id:
        # Could be full tower_id (e.g. 310_410_811184) or just site_id (e.g. 811184)
        if "_" in filters.tower_id:
            clauses.append("tower_id_primary = ?")
            params.append(filters.tower_id)
        else:
            # Partial / site ID match
            clauses.append("(site_id_primary = ? OR tower_id_primary LIKE ?)")
            params.append(filters.tower_id)
            params.append(f"%_{filters.tower_id}")

    if filters.zipcode:
        clauses.append("zipcode = ?")
        params.append(filters.zipcode)

    if filters.state:
        clauses.append("state = ?")
        params.append(filters.state)

    if filters.city:
        clauses.append("LOWER(city) = LOWER(?)")
        params.append(filters.city)

    if filters.generation:
        if filters.generation_prefix:
            clauses.append("generation LIKE ?")
            params.append(f"{filters.generation}%")
        else:
            clauses.append("generation = ?")
            params.append(filters.generation)

    if filters.site_type:
        clauses.append("site_type = ?")
        params.append(filters.site_type)

    if filters.provider:
        clauses.append("provider = ?")
        params.append(filters.provider)

    if filters.active is not None:
        clauses.append("active = ?")
        params.append(1 if filters.active else 0)

    if filters.rural is not None:
        clauses.append("rural = ?")
        params.append(1 if filters.rural else 0)

    if filters.lat is not None and filters.lng is not None:
        # Bounding box pre-filter using the index, then exact Haversine
        r = filters.radius_miles
        dlat = r / 69.0
        dlon = r / (69.0 * max(math.cos(math.radians(filters.lat)), 0.01))
        clauses.append("latitude BETWEEN ? AND ?")
        params.extend([filters.lat - dlat, filters.lat + dlat])
        clauses.append("longitude BETWEEN ? AND ?")
        params.extend([filters.lng - dlon, filters.lng + dlon])
        clauses.append("haversine(latitude, longitude, ?, ?) <= ?")
        params.extend([filters.lat, filters.lng, r])

    return (" AND ".join(clauses), params) if clauses else ("1=1", [])


async def search_towers(
    filters: ParsedFilters,
    page: int = 1,
    per_page: int = 50,
    sort_by: str = "state",
    sort_order: str = "asc",
) -> tuple[list[TowerResult], int]:
    """Returns (results, total_count)."""
    sort_by = sort_by if sort_by in VALID_SORT_COLS else "state"
    sort_order = sort_order if sort_order in VALID_SORT_ORDERS else "asc"
    offset = (page - 1) * per_page

    async with _get_conn() as conn:
        if filters.fts_query:
            # FTS path — optionally combined with structured WHERE clauses.
            # _build_where ignores fts_query, so it yields only the structured predicates.
            fts_q = filters.fts_query.strip() + "*"
            where_extra, params_extra = _build_where(filters)

            if where_extra == "1=1":
                # Pure address FTS — no other structured filters
                full_where = "id IN (SELECT rowid FROM towers_fts WHERE towers_fts MATCH ?)"
                all_params: list[Any] = [fts_q]
            else:
                # Address FTS combined with city/state/generation/etc. filters
                full_where = (
                    "id IN (SELECT rowid FROM towers_fts WHERE towers_fts MATCH ?)"
                    f" AND {where_extra}"
                )
                all_params = [fts_q] + params_extra

            count_sql = f"SELECT count(*) FROM towers WHERE {full_where}"
            cur = await conn.execute(count_sql, all_params)
            total = (await cur.fetchone())[0]

            data_sql = f"""
                SELECT * FROM towers WHERE {full_where}
                ORDER BY {sort_by} {sort_order}
                LIMIT ? OFFSET ?
            """
            cur = await conn.execute(data_sql, all_params + [per_page, offset])
        else:
            # Structured-only path
            where, params = _build_where(filters)
            count_sql = f"SELECT count(*) FROM towers WHERE {where}"
            cur = await conn.execute(count_sql, params)
            total = (await cur.fetchone())[0]

            data_sql = f"""
                SELECT * FROM towers WHERE {where}
                ORDER BY {sort_by} {sort_order}
                LIMIT ? OFFSET ?
            """
            cur = await conn.execute(data_sql, params + [per_page, offset])

        rows = await cur.fetchall()
        results = [_row_to_tower(r) for r in rows]

    return results, total


async def get_city_counts() -> list[tuple[str, str, int]]:
    """Returns [(city, state, count), ...] for query parser initialization."""
    async with _get_conn() as conn:
        cur = await conn.execute(
            "SELECT city, state, count(*) as cnt FROM towers "
            "WHERE city != '' AND state != '' "
            "GROUP BY LOWER(city), state "
            "ORDER BY cnt DESC"
        )
        rows = await cur.fetchall()
    return [(r["city"], r["state"], r["cnt"]) for r in rows]


async def get_stats() -> dict:
    """Returns summary stats for the dataset."""
    async with _get_conn() as conn:
        total_cur = await conn.execute("SELECT count(*) FROM towers")
        total = (await total_cur.fetchone())[0]

        gen_cur = await conn.execute(
            "SELECT generation, count(*) as cnt FROM towers GROUP BY generation ORDER BY cnt DESC"
        )
        generations = {r["generation"]: r["cnt"] for r in await gen_cur.fetchall()}

        provider_cur = await conn.execute(
            "SELECT provider, count(*) as cnt FROM towers WHERE provider != '' "
            "GROUP BY provider ORDER BY cnt DESC"
        )
        providers = {r["provider"]: r["cnt"] for r in await provider_cur.fetchall()}

        state_cur = await conn.execute(
            "SELECT state, count(*) as cnt FROM towers WHERE state != '' "
            "GROUP BY state ORDER BY cnt DESC LIMIT 10"
        )
        top_states = {r["state"]: r["cnt"] for r in await state_cur.fetchall()}

        type_cur = await conn.execute(
            "SELECT site_type, count(*) as cnt FROM towers GROUP BY site_type ORDER BY cnt DESC"
        )
        site_types = {r["site_type"]: r["cnt"] for r in await type_cur.fetchall()}

    return {
        "total": total,
        "providers": providers,
        "generations": generations,
        "top_states": top_states,
        "site_types": site_types,
    }


async def get_distinct_values() -> dict:
    """Returns unique values for UI filter helpers."""
    async with _get_conn() as conn:
        states_cur = await conn.execute(
            "SELECT DISTINCT state FROM towers WHERE state != '' ORDER BY state"
        )
        states = [r["state"] for r in await states_cur.fetchall()]

        gen_cur = await conn.execute(
            "SELECT DISTINCT generation FROM towers WHERE generation != '' ORDER BY generation"
        )
        generations = [r["generation"] for r in await gen_cur.fetchall()]

        type_cur = await conn.execute(
            "SELECT DISTINCT site_type FROM towers WHERE site_type != '' ORDER BY site_type"
        )
        site_types = [r["site_type"] for r in await type_cur.fetchall()]

    return {"states": states, "generations": generations, "site_types": site_types}


async def export_towers_csv(filters: ParsedFilters, limit: int = 10_000) -> str:
    """Returns CSV string for filtered results."""
    import csv, io

    where, params = _build_where(filters)
    sql = f"SELECT * FROM towers WHERE {where} ORDER BY state, city LIMIT ?"

    async with _get_conn() as conn:
        cur = await conn.execute(sql, params + [limit])
        rows = await cur.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    headers = [
        "tower_id", "site_id", "latitude", "longitude", "provider",
        "generation", "site_type", "active", "band_labels",
        "address", "city", "state", "zipcode",
        "rural", "first_seen", "last_seen", "geocode_accuracy",
    ]
    writer.writerow(headers)
    for row in rows:
        try:
            band_labels = "; ".join(json.loads(row["band_labels"] or "[]"))
        except Exception:
            band_labels = ""
        writer.writerow([
            row["tower_id_primary"], row["site_id_primary"],
            row["latitude"], row["longitude"], row["provider"],
            row["generation"], row["site_type"],
            "Yes" if row["active"] else "No",
            band_labels, row["address"], row["city"], row["state"], row["zipcode"],
            "Rural" if row["rural"] else "Urban",
            row["first_seen"], row["last_seen"], row["geocode_accuracy"],
        ])

    return output.getvalue()
