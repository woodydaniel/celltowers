# CellMapper Tower Scraper

A Python scraper for extracting cell tower data from CellMapper.net for US carriers (Verizon, AT&T, T-Mobile).

## ⚠️ Disclaimer

This tool is for **personal/research use only**. CellMapper's Terms of Service prohibit automated data extraction and commercial use. Use at your own risk. Consider contacting CellMapper for permission before large-scale scraping.

## Features

- Scrapes tower data for Verizon, AT&T, and T-Mobile
- Extracts: latitude, longitude, bands, provider name, tower type
- Supports LTE and 5G NR networks
- Geographic grid system covers entire continental US
- Progress tracking with resume capability
- Rate limiting to avoid server overload
- Multiple output formats (JSONL, CSV, SQLite)

## Data Points Collected

For each tower:
- `site_id` - CellMapper's tower identifier
- `latitude` / `longitude` - Tower location
- `bands` - LTE/NR band numbers (e.g., [2, 4, 12, 66, 71])
- `provider` - Carrier name (Verizon, AT&T, T-Mobile)
- `technology` - LTE or NR (5G)
- `tower_type` - MACRO, SMALL_CELL, etc.
- `channels` - EARFCN values
- `bandwidths` - Channel bandwidths in MHz

## Installation

```bash
# Clone/download the project
cd cellmapper

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

## IMPORTANT: Browser Cookies Required

CellMapper has bot protection (CAPTCHA). To use this scraper, you must provide cookies from your browser session:

### Step 1: Get Cookies from Browser

1. Open https://www.cellmapper.net/map in your browser
2. Open Developer Tools (F12 or right-click → Inspect)
3. Go to **Application** tab (Chrome) or **Storage** tab (Firefox)
4. Click on **Cookies** → `https://www.cellmapper.net`
5. Find the `JSESSIONID` cookie and copy its value

### Step 2: Set the Cookies

**Option A: Environment variable (recommended)**
```bash
export CELLMAPPER_COOKIES="JSESSIONID=node0abc123xyz...; visited=yes"
python main.py --test
```

**Option B: Create a .env file**
```bash
echo 'CELLMAPPER_COOKIES="JSESSIONID=node0abc123xyz..."' > .env
source .env
python main.py --test
```

### Cookie Expiration

Your session cookies may expire after some time. If you start getting `NEED_RECAPTCHA` errors, you can either:
1. Manually refresh cookies (see below), or
2. **Configure automatic CAPTCHA solving** (recommended for unattended servers)

## Automatic CAPTCHA Solving (Recommended)

For unattended server deployments, configure automatic CAPTCHA solving to prevent session expiration from stopping your scrape.

### Provider Comparison

| Provider | Success Rate | Speed | Cost | Best For |
|----------|--------------|-------|------|----------|
| **CapSolver** | 90-95% | 5-12s | $1.20/1k | Best overall, supports Turnstile |
| **CapMonster** | 85-93% | 8-20s | Paid | Solid premium alternative |
| **2Captcha** | 78-85% | 15-30s | $1.50/1k | Reliable, good documentation |
| **FlareSolverr** | 92-97% | 4-10s | Free | Self-hosted, requires Docker |

### Option A: CapSolver (Recommended)

Fastest and most reliable for hCaptcha/Cloudflare challenges.

```bash
# Sign up at https://capsolver.com
export CAPTCHA_PROVIDER=capsolver
export CAPTCHA_API_KEY=CAP-xxxxxxxxxxxxx

# Run scraper
python main.py --carrier tmobile
```

### Option B: 2Captcha

Human solvers, widely supported, slightly slower.

```bash
# Sign up at https://2captcha.com
export CAPTCHA_PROVIDER=2captcha
export CAPTCHA_API_KEY=your_api_key

python main.py --carrier tmobile
```

### Option C: FlareSolverr (Free, Self-Hosted)

Runs a headful browser via Docker. Free but requires server resources.

```bash
# Start FlareSolverr container
docker run -d -p 8191:8191 --name flaresolverr ghcr.io/flaresolverr/flaresolverr:latest

# Configure scraper
export CAPTCHA_PROVIDER=flaresolverr
export FLARESOLVERR_URL=http://localhost:8191

python main.py --carrier tmobile
```

### Server Deployment with Auto-CAPTCHA

For Hetzner/VPS deployment:

```bash
# Add to your .bashrc or systemd service
export CAPTCHA_PROVIDER=capsolver
export CAPTCHA_API_KEY=CAP-xxxxxxxxxxxxx
export RESEND_API_KEY=re_xxxxxxxxxxxxx
export ALERT_EMAIL=your@email.com

# Now the scraper will:
# 1. Auto-solve CAPTCHAs when sessions expire
# 2. Email you only for unrecoverable errors
```

### Manual Cookie Refresh (Fallback)

If automatic solving isn't configured:
1. Refresh cellmapper.net in your browser
2. Get fresh cookies
3. Update the `CELLMAPPER_COOKIES` environment variable

## High-Performance Architecture (v2)

For production deployments, the scraper now supports a **Harvester + Worker** architecture that dramatically improves throughput:

### Key Improvements

| Component | Old | New |
|-----------|-----|-----|
| **HTTP Client** | httpx (Python TLS) | tls-client (Chrome TLS fingerprint) |
| **Cookie Source** | Per-worker refresh | Shared Redis pool |
| **CAPTCHA Handling** | Browser per request | Harvester solves, workers consume |
| **Recovery** | 6-hour proxy cooldown | Instant cookie swap |

### How It Works

1. **Harvester** (single process) continuously mints cookies via FlareSolverr + CapMonster
2. **Cookie Validation** - Each minted cookie is validated via a lightweight API probe before entering the pool
3. **Workers** pull **pre-validated** cookies from Redis pool and use tls-client for API calls
4. On CAPTCHA hit, workers mark cookie as "poisoned" and immediately swap to a fresh one

**Pool Integrity**: Only API-validated cookies enter the shared Redis pool. If FlareSolverr repeatedly produces rejected cookies, the harvester backs off exponentially (60s → 30min) to conserve proxy bandwidth.

### Docker Compose Setup

```bash
# Start all services (Redis, FlareSolverr, Harvester, 6 regional workers)
docker compose up -d

# View logs
docker compose logs -f harvester
docker compose logs -f tmobile_west tmobile_east att_west att_east verizon_west verizon_east
```

### Harvester Tuning (Bandwidth-Conscious Defaults)

The harvester is the largest bandwidth consumer (it drives a browser challenge flow). The defaults are tuned to **avoid burning proxy bandwidth**:
- **1 harvester** (scale up only if the pool can’t keep up)
- **Harvest every 15 min** (`HARVEST_INTERVAL_SECONDS=900`)
- **Small pool target** (`TARGET_POOL_SIZE=10`)
- **Longer TTL** (`COOKIE_TTL_SECONDS=3600`)

If you need to scale:

```bash
docker compose up -d --build --scale harvester=1 --remove-orphans
docker compose ps
docker compose logs -f harvester
```

### Environment Variables

```bash
# Redis cookie pool
REDIS_URL=redis://localhost:6379/0

# Harvester settings
CAPTCHA_PROVIDER=capmonster
CAPTCHA_API_KEY=your_capmonster_key
HARVEST_PROXY=http://user:pass@proxy:port  # Sticky proxy for cookie minting
HARVEST_PROXY_FILE=config/proxies_tmobile_west.txt
HARVEST_URL=https://www.cellmapper.net/      # Lower bandwidth than /map (override if needed)
COOKIE_TTL_SECONDS=3600                      # 60 min TTL
HARVEST_INTERVAL_SECONDS=900                 # Harvest every 15 min
TARGET_POOL_SIZE=10                          # Small buffer target

# Cookie reuse (workers)
REDIS_COOKIE_PUTBACK_ON_SUCCESS=true         # Return good cookies to pool for sequential reuse

# Worker settings (optional)
USE_TLS_CLIENT=true                         # Use Chrome TLS fingerprint (default: true)
FAST_MODE=true                              # Faster request delays
```

### Expected Performance

- **Latency**: 150-300ms per request (vs 1.5-3s with httpx)
- **CAPTCHA Rate**: 5-15 per 1000 requests (vs 30-60)
- **Recovery Time**: Instant (vs 6-hour cooldown)
- **Throughput**: 600-800 tiles/hour/worker (vs ~120)

## Usage

### Quick Test Run
```bash
# Test with a small geographic area
python main.py --test
```

### Scrape Single Carrier
```bash
python main.py --carrier tmobile
python main.py --carrier att
python main.py --carrier verizon
```

### Scrape All Carriers
```bash
python main.py
```

### Output Formats
```bash
python main.py --format jsonl   # JSON Lines (default)
python main.py --format csv     # CSV spreadsheet
python main.py --format sqlite  # SQLite database
```

### Time Estimate
```bash
python main.py --estimate
```

### Resume After Interruption
The scraper automatically saves progress. Just run again:
```bash
python main.py  # Automatically resumes
python main.py --no-resume  # Start fresh
```

## Output Files

Data is saved to `data/towers/`:
- `towers.jsonl` - JSON Lines format (one tower per line)
- `towers.csv` - CSV format
- `towers.db` - SQLite database

Progress tracking: `data/progress.json`

## Client Dashboard (Scrape Progress)

This repo includes a **minimal, client-facing dashboard** that reads the existing persisted state:
- Tile progress files: `data/progress_<carrier>_<run_tag>.json`
- Worker snapshots: `logs/worker_metrics_<worker_id>.jsonl`

It displays:
- **Progress bar + percent complete**
- **Towers collected**
- **Projected finish date** (estimated)

### Run locally

```bash
python3 dashboard/server.py
# open: http://127.0.0.1:8088/
```

### Show live status from Hetzner (recommended: SSH tunnel, no public port)

1) Create a tunnel from your laptop → Hetzner (forwards remote `localhost:8088` to local `localhost:18088`):

```bash
ssh -i ~/.ssh/hetzner_cellmapper.pem -L 18088:127.0.0.1:8088 root@91.99.49.172
```

2) Run the local dashboard in “upstream” mode (it will proxy the tunneled status):

```bash
export DASHBOARD_UPSTREAM_STATUS_URL="http://127.0.0.1:18088/api/status"
python3 dashboard/server.py
```

## Production: Live client dashboard (Vercel + Hetzner)

### What you get
- **Client URL (Vercel)**: a clean dashboard page
- **Status API (Hetzner)**: `https://status-api.shefa7.com/api/status` (protected by a bearer token)
- **Security**: bearer token is stored only in Vercel env vars (not visible to clients)

### 1) DNS (Cloudflare / registrar)
Create an **A record**:
- `status-api.shefa7.com` → `91.99.49.172`

### 2) Hetzner (Docker)
On the Hetzner server in `/root/cellmapper`, set a strong bearer token and start the new services:

```bash
cd /root/cellmapper

# Generate a strong token (example uses openssl)
export DASHBOARD_BEARER_TOKEN="$(openssl rand -hex 32)"

docker compose up -d --build dashboard_api caddy
docker compose logs -f caddy
```

Once Caddy has issued the certificate, verify:

```bash
curl -sS -H "Authorization: Bearer $DASHBOARD_BEARER_TOKEN" \
  https://status-api.shefa7.com/api/status | head
```

### 3) Vercel (client-facing)
Deploy this repo to Vercel.

In Vercel project env vars, set:
- `SCRAPER_STATUS_URL` = `https://status-api.shefa7.com/api/status`
- `SCRAPER_STATUS_BEARER_TOKEN` = the same value as `DASHBOARD_BEARER_TOKEN` on Hetzner

The client uses the Vercel site; Vercel securely proxies live status via `/api/status`.

### Optional Basic Auth (recommended for client-facing)

```bash
export DASHBOARD_BASIC_AUTH_USER="client"
export DASHBOARD_BASIC_AUTH_PASS="change-me"
python3 dashboard/server.py
```

### Useful env vars

```bash
export DASHBOARD_HOST="0.0.0.0"          # default: 0.0.0.0
export DASHBOARD_PORT="8088"             # default: 8088
export DASHBOARD_ACTIVE_WINDOW_SEC="21600"  # default: 6h (filters out old runs)
export DASHBOARD_DATA_DIR="/root/cellmapper/data"  # override if running outside the scraper directory (e.g., Docker)
export DASHBOARD_LOGS_DIR="/root/cellmapper/logs"  # override if running outside the scraper directory (e.g., Docker)
export DASHBOARD_DEBUG="false"           # default: false (enables request logs)
export DASHBOARD_BEARER_TOKEN="..."      # optional: secure API access with Authorization: Bearer <token>
```

## Configuration

### Rate Limiting
Edit `config/settings.py`:
```python
REQUEST_DELAY_MIN = 1.5  # Minimum seconds between requests
REQUEST_DELAY_MAX = 3.0  # Maximum seconds (with jitter)
```

### Geographic Grid
```python
# Default: 0.25° tiles (~28km x 25km) - good balance of coverage vs request count
GRID_SIZE_LAT = 0.25  # Degrees latitude per tile
GRID_SIZE_LON = 0.25  # Degrees longitude per tile

# For very dense areas (NYC, LA): use 0.1° to avoid hasMore truncation
# For faster runs with some data loss: use 0.5°
```

### Carrier MCC/MNC Codes
See `config/networks.json` for all carrier identifiers.

## API Reference

The scraper uses CellMapper's internal API:

```
GET https://api.cellmapper.net/v6/getTowers
Parameters:
  - MCC: Mobile Country Code (310, 311)
  - MNC: Mobile Network Code (260=T-Mobile, 410=AT&T, 480=Verizon)
  - RAT: Technology (LTE, NR)
  - boundsNELatitude, boundsNELongitude: Northeast corner
  - boundsSWLatitude, boundsSWLongitude: Southwest corner
```

## Deployment on Hetzner

For long-running scrapes, deploy on a VPS:

```bash
# SSH to your Hetzner server
ssh user@your-server

# Clone project
git clone <your-repo> cellmapper
cd cellmapper

# Setup Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Best Practice: Docker Compose (Recommended)

For unattended multi-day runs, prefer Docker Compose over `nohup` so services auto-restart after crashes/reboots and workers wait for the cookie pool to be ready.

```bash
cd /root/cellmapper

# Start (or restart) everything
docker compose up -d --build

# Check status (should show 9 services: redis, flaresolverr, harvester, 6 workers)
docker compose ps

# Tail logs
docker compose logs -f harvester
```

### Monitoring Logs (Docker Compose)

```bash
# Watch worker logs in real-time
docker compose logs -f tmobile_west

# Quick check: are all workers up?
docker compose ps

# Search for errors or CAPTCHA events
docker compose logs --no-color | grep -E "CAPTCHA|ERROR|NEED_RECAPTCHA" | tail -50
```

### Stopping / Restarting

```bash
# Stop all services
docker compose down

# Restart all services
docker compose up -d --build
```

### Parallel Carrier Scrapes

Use the launcher script to run all 3 carriers with staggered starts:

```bash
./scripts/launch_parallel.sh              # All 3 carriers
./scripts/launch_parallel.sh tmobile att  # Specific carriers
```

### Parallel Regional Scrapes (West/East) - Recommended on cx32

If you want **safer speed-up without raising per-IP request rates**, run **2 workers per carrier**
(West + East). This avoids duplicate work by partitioning the US grid into two non-overlapping
longitude ranges and isolates progress/output/log files via `--run-tag`.

This starts **6 total workers** (tmobile_west/east, att_west/east, verizon_west/east) with 2-minute staggers:

```bash
./scripts/launch_parallel_regions.sh
```

Monitor:

```bash
tail -f logs/*_west.log logs/*_east.log
ps aux | grep "python main.py" | grep -v grep
```

Progress/output files created per worker:
- `data/progress_<carrier>_<run_tag>.json` (e.g. `progress_tmobile_west.json`)
- `data/towers/towers_<carrier>_<run_tag>.jsonl` (e.g. `towers_att_east.jsonl`)
- `logs/scraper_<carrier>_<run_tag>.log` (internal scraper logger)
- `logs/<carrier>_<run_tag>.log` (launcher stdout/stderr redirection)

Proxy pools per region are split to avoid sticky-session contention:
- `config/proxies_<carrier>_west.txt`
- `config/proxies_<carrier>_east.txt`

### Alternative: screen/tmux

```bash
# Run in screen session
screen -S cellmapper
python main.py --carrier tmobile --session-pool --proxies
# Press Ctrl+A, D to detach

# Reattach later
screen -r cellmapper
```

## Time Estimates

| Area | Tiles | Carriers | Est. Time |
|------|-------|----------|-----------|
| Test area | ~16 | 3 | ~5 min |
| Single state | ~100-500 | 3 | 1-5 hours |
| Full US (0.25° grid) | ~23,200 | 3 | 2-3+ weeks (safe settings) |
| Full US (0.25°) with West/East | ~23,200 | 6 workers | ~1-1.5 weeks (safer speed-up) |

## License

For personal/research use only. Not for commercial purposes.
CellMapper data is copyrighted by CellMapper Services Limited.

