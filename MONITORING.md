# CellMapper Scraper Monitoring Guide

Quick reference commands for monitoring worker health and performance on Hetzner.

## Latest Updates (Dec 21, 2025)

### Architecture Upgrade: Harvester + Worker Pattern

**Major Changes:**
1. **tls-client transport**: Chrome TLS fingerprint instead of Python httpx (10x faster, fewer blocks)
2. **Redis cookie pool**: Shared cookies across workers, instant CAPTCHA recovery
3. **Cookie Harvester**: Dedicated service mints cookies via FlareSolverr + CapMonster
4. **Removed per-request FlareSolverr fallback**: No more browser-per-403 explosions

**New Components:**
- `redis` container: Stores shared cookie pool with TTL
- `harvester` container: Continuously mints fresh cookies
- `scraper/cookie_pool.py`: Redis cookie get/put helpers
- `scripts/harvest_cookies.py`: Harvester script

**Performance Targets:**
- Latency: 150-300ms (vs 1.5-3s with httpx)
- CAPTCHA rate: 5-15 per 1000 (vs 30-60)
- Recovery: Instant cookie swap (vs 6-hour proxy cooldown)
- Throughput: 600-800 tiles/hr/worker (vs ~120)

## Server Connection

**Hetzner Server:** `91.99.49.172`  
**SSH User:** `root`  
**Project Path:** `/root/cellmapper`

## Monitoring Commands

### Watch Real-Time Activity

Monitor a specific worker's log output in real-time:

```bash
ssh root@91.99.49.172 "tail -f /root/cellmapper/logs/tmobile_west.log"
```

Replace `tmobile_west` with any worker:
- `tmobile_west`
- `tmobile_east`
- `att_west`
- `att_east`
- `verizon_west`
- `verizon_east`

### Check All Workers Are Still Running

Verify all 6 workers are active (should return `6`):

```bash
ssh root@91.99.49.172 "ps aux | grep 'python main.py' | grep -v grep | wc -l"
```

### View Latest Metrics Snapshots

Check performance metrics from all workers:

```bash
ssh root@91.99.49.172 "tail /root/cellmapper/logs/worker_metrics_*.jsonl"
```

New snapshot fields (performance upgrades):
- `api_success_rate`: last-15m API call success rate
- `api_avg_latency_ms_5m`: last-5m average API latency (ms)
- `error_counts`: includes machine-friendly `error_code` strings like `http_403`, `http_429`, `transport_timeout`

### Quick Health Check

Get latest activity from all 6 workers at once:

```bash
ssh root@91.99.49.172 "for log in tmobile_west tmobile_east att_west att_east verizon_west verizon_east; do echo '=== '\$log' ==='; tail -3 /root/cellmapper/logs/\${log}.log; done"
```

## Expected Metrics

- **Success Rate:** ≥90%
- **Velocity:** ≥60 tiles/hr (combined across all workers)
- **Workers Running:** 6 (one per carrier-region combination)
- **API Success Rate:** ≥90% (last 15m, from metrics JSONL)
- **API Avg Latency:** watch `api_avg_latency_ms_5m` for proxy quality/regressions

## Worker Configuration

Each worker runs with:
- **Carrier-specific proxy pools:** 40 proxies per region (240 total across 6 worker files)
  - `config/proxies_tmobile_west.txt`, `config/proxies_tmobile_east.txt`
  - `config/proxies_att_west.txt`, `config/proxies_att_east.txt`
  - `config/proxies_verizon_west.txt`, `config/proxies_verizon_east.txt`
- **Geographic bounds:** West/east split at -96.0° longitude
- **Session rotation:** 3 requests per session (optimized from 2)
- **Cookie refresh:** Auto-refresh via Playwright with 30-min cache
- **CAPTCHA cooling:** 15-min cooldown on any CAPTCHA hit
- **Progress:** Persists to carrier-region JSON files, auto-resumes

## Troubleshooting

### Workers Not Running

```bash
# Check container status (should show 6 worker services up)
ssh root@91.99.49.172 "cd /root/cellmapper && docker compose ps"

# Restart workers (and harvester) if needed
ssh root@91.99.49.172 "cd /root/cellmapper && docker compose up -d --build"
```

### Stop All Workers

```bash
ssh root@91.99.49.172 "cd /root/cellmapper && docker compose stop tmobile_west tmobile_east att_west att_east verizon_west verizon_east"
```

### View Launcher Log

```bash
ssh root@91.99.49.172 "cd /root/cellmapper && docker compose logs -f tmobile_west"
```

### Check Tile Completion Count

```bash
ssh root@91.99.49.172 "for log in tmobile_west tmobile_east att_west att_east verizon_west verizon_east; do echo \"=== \$log ===\"; grep -c 'Parsed.*towers' /root/cellmapper/logs/\${log}.log 2>/dev/null || echo 0; done"
```

### View CAPTCHA Activity

```bash
ssh root@91.99.49.172 "grep -i 'CAPTCHA' /root/cellmapper/logs/*.log | tail -20"
```

### FlareSolverr (Harvester Only)

FlareSolverr is now used **only by the Harvester** to mint cookies. Workers never spawn browsers.

```bash
# Check harvester status
docker compose logs -f harvester

# Check FlareSolverr status
docker compose logs -f flaresolverr
```

### Redis Cookie Pool

```bash
# Check cookie pool size
docker exec cellmapper-redis redis-cli --scan --pattern "cellmapper:cookie:*" | wc -l

# View all cookies with TTL
docker exec cellmapper-redis redis-cli --scan --pattern "cellmapper:cookie:*" | while read key; do
  ttl=$(docker exec cellmapper-redis redis-cli TTL "$key")
  echo "$key: TTL=$ttl"
done

# Clear cookie pool (force re-harvest)
docker exec cellmapper-redis redis-cli KEYS "cellmapper:cookie:*" | xargs docker exec -i cellmapper-redis redis-cli DEL
```

### Harvester Tuning (Bandwidth-Conscious Defaults)

The harvester is the largest bandwidth consumer. Defaults are tuned to avoid burning proxy bandwidth:
- **1 harvester**
- **Harvest every 15 min** (`HARVEST_INTERVAL_SECONDS=900`)
- **Small pool** (`TARGET_POOL_SIZE=10`)
- **Longer TTL** (`COOKIE_TTL_SECONDS=3600`)
- **Prefer lightweight URL** (`HARVEST_URL=https://www.cellmapper.net/`)

```bash
cd /root/cellmapper
docker compose up -d --build --scale harvester=1 --remove-orphans
docker compose ps
docker compose logs -f harvester
```

Operational targets:
- Keep cookie pool **≥ 20** under steady load (6 workers)
- Alert if cookie pool **< 10 for 5+ minutes**

### Grafana Alert: Cookie Pool Low

There’s no built-in Prometheus metric named `cookie_pool_count` in this repo, so the simplest approach is to export the cookie pool count (and related counters) via a Node Exporter textfile metric using the provided helper script:

```bash
# Writes Prometheus textfile format to stdout
REDIS_URL=redis://localhost:6379/0 python scripts/cookie_pool_metrics.py --prom
```

Example cron (writes to node_exporter textfile collector dir):

```bash
*/1 * * * * REDIS_URL=redis://localhost:6379/0 /usr/bin/python3 /root/cellmapper/scripts/cookie_pool_metrics.py --prom > /var/lib/node_exporter/textfile_collector/cellmapper_cookie_pool.prom
```

Then in Grafana/Prometheus alerting, trigger:

- `cellmapper_cookie_pool_count < 10` for **5m**
- Optional sanity check: `cellmapper_requests_per_cookie` should climb over time (cookie reuse working)

### Turnstile/CAPTCHA Rate (Harvester ROI Metrics)

The harvester records counters in Redis to monitor cookie quality and decide if it's worth enabling a paid solver:

- `cellmapper:counters:turnstile_hits`: how often harvesting appears blocked by Turnstile/CAPTCHA-style challenges
- `cellmapper:counters:harvest_success`: total cookies minted by FlareSolverr
- `cellmapper:counters:harvest_validated`: cookies that passed API validation (stored in pool)
- `cellmapper:counters:harvest_rejected`: cookies rejected by API (NOT stored - prevents pool poisoning)

**Validation Flow**: Each cookie is probed via `getFrequency` API call through the same proxy. Only cookies returning HTTP 200 (not `NEED_RECAPTCHA`) enter the pool.

Query with Redis directly:

```bash
docker exec cellmapper-redis redis-cli MGET \
  cellmapper:counters:turnstile_hits \
  cellmapper:counters:harvest_success \
  cellmapper:counters:harvest_validated \
  cellmapper:counters:harvest_rejected
```

Or use the helper script:

```bash
cd /root/cellmapper
source venv/bin/activate
REDIS_URL=redis://localhost:6379/0 python scripts/show_metrics.py
```

### Harvester Bandwidth Failsafe (Pause Minting When Workers Stall)

To avoid burning proxy bandwidth when workers are stuck/hung, workers emit a Redis heartbeat and the harvester will **pause cookie minting** when it detects:

- **No worker heartbeat** (workers not emitting metrics snapshots), or
- **No worker progress** (no successful tile completions for a long time)

**Redis keys (written by workers):**

- `cellmapper:workers:last_snapshot_ts` — updated on each periodic worker metrics snapshot
- `cellmapper:workers:last_success_ts` — updated on each successful tile completion
- `cellmapper:workers:last_success_meta` — JSON context for the last success (worker id, carrier, counts)

**Harvester env knobs:**

- `HARVESTER_STARTUP_GRACE_SEC` (default: `600`) — don’t pause immediately during cold start
- `HARVESTER_WORKER_HEARTBEAT_STALE_SEC` (default: `900`) — if no snapshot in this long, treat workers as dead/hung
- `HARVESTER_WORKER_STALL_THRESHOLD_SEC` (default: `1800`) — if no success in this long, treat progress as stalled

**Alerting:**

- When the harvester enters paused state, it sends a Resend email (if `RESEND_API_KEY` + `ALERT_EMAIL` are set).

**Quick inspection:**

```bash
docker exec cellmapper-redis redis-cli MGET \
  cellmapper:workers:last_snapshot_ts \
  cellmapper:workers:last_success_ts \
  cellmapper:workers:last_success_meta
```

### New Environment Variables

```bash
# Redis
REDIS_URL=redis://redis:6379/0

# Harvester
CAPTCHA_PROVIDER=capmonster
CAPTCHA_API_KEY=your_key
HARVEST_PROXY=http://user:pass@proxy:port
HARVEST_PROXY_FILE=config/proxies_tmobile_west.txt
HARVEST_URL=https://www.cellmapper.net/
COOKIE_TTL_SECONDS=3600
HARVEST_INTERVAL_SECONDS=900
TARGET_POOL_SIZE=10

# Workers
USE_TLS_CLIENT=true  # Chrome TLS fingerprint (default)
REDIS_COOKIE_PUTBACK_ON_SUCCESS=true
```

### Proxy pool via environment (optional)

Instead of managing `config/proxies*.txt`, you can supply proxies via env:
- `PROXY_LIST`: newline/comma separated list of proxies (same formats as proxy files)
- `PROXY_PROVIDER=decodo` with `DECODO_USERNAME`, `DECODO_PASSWORD`, `DECODO_PORTS` (e.g. `10001-10030`)

These are additive with file-based proxies.

### Speed mode (opt-in)

Opt-in speed mode reduces request jitter when the proxy pool is large:

```bash
export FAST_MODE=true
export FAST_MIN_PROXIES=10
export FAST_REQUEST_DELAY_MIN=1.0
export FAST_REQUEST_DELAY_MAX=2.5
```

Or via CLI:

```bash
python main.py --carrier tmobile --session-pool --proxies --fast
```

## Code Changes Summary

### 1. SessionPool Cookie Refresh (`scraper/session_pool.py`)
- Added `os` import for file modification time checks
- Modified `_refresh_session()` to skip Playwright if cached cookies < 30 min old
- Forces fresh cookies when `needs_refresh=True` (breaks CAPTCHA loop)

### 2. Proxy CAPTCHA Cooling (`scraper/proxy_manager.py`)
- Extended `mark_captcha()` to apply immediate 15-min cooldown on ANY CAPTCHA
- Prevents rapid re-triggering of same proxy after CAPTCHA

### 3. Launch Script (`scripts/launch_parallel_regions.sh`)
- Changed `REQUESTS_PER_SESSION` default from 2 to 3
- Reduces rotation overhead by 33%

### 4. Proxy Configuration (`config/proxies_*_*.txt`)
- Replaced old sessid-based proxies with new sticky ports
- 240 proxies total across 6 files (40 per carrier-region)

## Architecture

**Regional Split:**
```
US Territory (Continental)
├── West (-124.85° to -96.0°)
│   ├── T-Mobile (proxy file: config/proxies_tmobile_west.txt)
│   ├── AT&T (proxy file: config/proxies_att_west.txt)
│   └── Verizon (proxy file: config/proxies_verizon_west.txt)
└── East (-96.0° to -66.93°)
    ├── T-Mobile (proxy file: config/proxies_tmobile_east.txt)
    ├── AT&T (proxy file: config/proxies_att_east.txt)
    └── Verizon (proxy file: config/proxies_verizon_east.txt)
```

**Session Flow (v2 - Harvester/Worker):**
```
Harvester (continuous loop):
  FlareSolverr → Get cookies → CapMonster (if Turnstile) → Push to Redis (TTL=25min)

Worker:
  Start → Pull cookie from Redis
  ↓
  API request (tls-client, Chrome TLS) → Success
  ↓
  API request → Success
  ↓
  API request → NEED_RECAPTCHA?
                ├─ YES → Mark cookie poisoned (local) → Pull fresh cookie → Retry immediately
                └─ NO → Continue
  ↓
  Rotate proxy → Next proxy in pool → Repeat
```

