# AT&T Tower Search

A full-stack web portal for searching 500k+ geocoded AT&T cell tower records.

## Features

- **Smart natural language search**: type `5G towers in Florida`, `small cells in Portland`, or `39.90, -74.21`
- **Typo tolerance**: `Flrida` → Florida, `Mimai` → Miami (powered by rapidfuzz)
- **Disambiguation**: if "Portland" could be OR or ME, you're prompted to choose
- **Tower ID search**: search by site ID (`811184`) or full tower ID (`310_410_811184`)
- **Coordinate radius search**: paste `lat, lng` and search within N miles
- **Sortable, paginated results table** with 50 rows/page
- **CSV export** of filtered results
- **CellMapper links**: click any row's external link to view on CellMapper.net

---

## Local Development

### Prerequisites
- Python 3.12+
- Node.js 20.19+ or via Volta
- pnpm

### 1. Build the database (one-time, ~60 seconds)

```bash
cd tower-search
python3 -m venv venv
source venv/bin/activate
pip install -r api/requirements.txt

# Creates data/towers.db (~200 MB)
python api/import_data.py ../downloads/towers_geocoded_500k.jsonl data/towers.db
```

### 2. Start the API server

```bash
source venv/bin/activate
TOWERS_DB_PATH=data/towers.db uvicorn api.main:app --reload --port 8000
```

### 3. Start the frontend dev server

```bash
cd web
pnpm install
pnpm dev
# Opens at http://localhost:5173 (proxies /api to :8000)
```

---

## Production Build (Docker)

The JSONL file must be at `tower-search/data/towers_geocoded_500k.jsonl` for the Docker build.

```bash
# From repo root
cp downloads/towers_geocoded_500k.jsonl tower-search/data/

cd tower-search
docker build -t att-tower-search .
docker run -p 8000:8000 att-tower-search
# App is at http://localhost:8000
```

---

## Deploy to Railway

1. Push to GitHub
2. Create a new Railway project → "Deploy from GitHub repo"
3. Set root directory to `tower-search/`
4. Add environment variable: `TOWERS_DB_PATH=/app/data/towers.db`
5. Railway detects the `Dockerfile` and builds automatically

> **Note**: The SQLite DB is baked into the Docker image (~200 MB). Railway's free tier
> has a 512 MB image limit — upgrade to Hobby ($5/mo) for comfortable headroom.

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/search` | Natural language search |
| GET | `/api/towers` | Structured search (all filters as query params) |
| GET | `/api/suggest` | Autocomplete suggestions |
| GET | `/api/towers/export` | CSV download |
| GET | `/api/stats` | Dataset statistics |
| GET | `/api/filters` | Distinct values for UI |
| GET | `/api/health` | Health check |

### POST `/api/search` example

```json
{
  "query": "5G towers in Florida",
  "page": 1,
  "per_page": 50
}
```

Response:
```json
{
  "parsed": { "generation": "5G", "generation_prefix": true, "state": "FL" },
  "ambiguous": [],
  "results": [...],
  "total": 1234,
  "page": 1,
  "pages": 25,
  "query": "5G towers in Florida"
}
```

### Disambiguation example

```json
{ "query": "5G towers in Portland" }
```

Response:
```json
{
  "parsed": { "generation": "5G", "generation_prefix": true },
  "ambiguous": [{
    "term": "Portland",
    "field": "city",
    "options": [
      { "city": "Portland", "state": "OR", "count": 847 },
      { "city": "Portland", "state": "ME", "count": 312 }
    ]
  }],
  "results": [],
  "total": 0
}
```

Resolve by passing `resolved`:
```json
{
  "query": "5G towers in Portland",
  "resolved": { "city": "Portland", "state": "OR" }
}
```
