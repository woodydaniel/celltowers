## Scratchpad: v2 rollout (Redis cookie pool + Harvester + tls-client)

### What we implemented

- **Removed per-request FlareSolverr fallback**
  - Eliminates the "spawn a browser per 403" failure mode.

- **Added Redis cookie pool**
  - Shared cookie store with TTL so workers can reuse "good" cookies and rotate instantly.
  - Implemented in `scraper/cookie_pool.py`.

- **Added a Harvester process**
  - `scripts/harvest_cookies.py` maintains a target pool size in Redis by minting cookies (default: Playwright + stealth).
  - Config via env: `REDIS_URL`, `FLARESOLVERR_URL`, `HARVEST_PROXY`, `HARVEST_PROXY_FILE`, `COOKIE_TTL_SECONDS`, `TARGET_POOL_SIZE`.

- **Workers consume Redis cookies**
  - `scraper/cookie_manager.py` now pulls cookies from Redis first and supports "poisoning" (delete from pool).
  - `scraper/api_client.py` attempts recovery on `NEED_RECAPTCHA` by poisoning current cookie and swapping to a fresh one from Redis.

- **tls-client transport for API calls**
  - `scraper/api_client.py` uses `tls-client` (Chrome TLS fingerprint) via `run_in_executor`, with httpx fallback.
  - Env: `USE_TLS_CLIENT=true` (default true if installed).

### Turnstile/harvest ROI metrics

Harvester increments Redis counters:
- `cellmapper:counters:turnstile_hits`
- `cellmapper:counters:harvest_success`
- `cellmapper:counters:harvest_validated`
- `cellmapper:counters:harvest_rejected`

View via:
- `scripts/show_metrics.py`
- or `redis-cli MGET cellmapper:counters:turnstile_hits cellmapper:counters:harvest_success`

---

## Day-1 Hardening (2025-12-21)

### Problem Identified
FlareSolverr cookies passed Cloudflare but were **rejected by CellMapper API** (`NEED_RECAPTCHA`).
The API requires cookies minted by a full browser that executes the SPA's XHR handshake.

### Solution Implemented

1. **Switched Harvester to Playwright + Stealth**
   - `HARVEST_ENGINE=playwright` (default now)
   - Uses `playwright-stealth` to patch `navigator.webdriver` and other automation leaks
   - Waits for `canvas.leaflet-zoom-animated` selector before dumping cookies (ensures XHR handshake complete)
   - Fixed User-Agent to Chrome 125; stores UA in Redis alongside cookies

2. **Matched TLS Fingerprint**
   - `tls_client.Session(client_identifier="chrome_125")` in `api_client.py`
   - Env override: `TLS_CLIENT_IDENTIFIER=chrome_125`
   - Prevents Cloudflare from detecting fingerprint mismatch between cookie mint and API usage

3. **Cookie Validation Guard**
   - Harvester validates cookies via `getTowers` probe before pool insertion
   - Only `validated=True` cookies enter Redis (prevents pool poisoning)
   - Exponential backoff on rejection (60s → 30min)

4. **Compose Updates**
   - `HARVEST_ENGINE=playwright` set for harvester
   - `USE_TLS_CLIENT=true` already set for workers

### Files Changed
- `scraper/cookie_manager.py` – Playwright stealth + map-canvas wait + fixed UA
- `scraper/api_client.py` – `chrome_125` TLS fingerprint, env override
- `scraper/cookie_pool.py` – `validated` flag enforcement
- `scripts/harvest_cookies.py` – `HARVEST_ENGINE` switch, CookieManager integration
- `docker-compose.yml` – `COOKIE_ENGINE=playwright`

### Verified Working (2025-12-21 20:45 UTC)
- Harvester mints Playwright cookies with JSESSIONID
- Worker pulls cookies from pool
- API calls succeed: `Parsed 50 towers (hasMore=True)`, `Parsed 7 towers`
- Cookie reuse working: `returned cookie ... (TTL=1500s)`
- CAPTCHA recovery working: occasional NEED_RECAPTCHA → auto-poison → fresh cookie → success

### Operational Notes
- **Harvester rate**: ~30s per cookie (via Playwright + Decodo proxy)
- **Harvester loop**: drip-feed top-up (no burst-then-sleep). Tunables:
  - `CHECK_EVERY_SEC` (default 20s) – how often to re-check pool when full
  - `BACKOFF_SEC` (default 2s) – delay between single-cookie harvest attempts when below target
- **Workers/Harvester ratio**: depends on proxy quality + tile density; start with 2 workers per harvester and scale cautiously
- **Pool target**: 10 cookies (configurable via TARGET_POOL_SIZE)
- **Worker low-water mark**: `COOKIEPOOL_MIN` (default 3) prevents draining the pool to 0 under contention
- Some proxies fail with `ERR_TUNNEL_CONNECTION_FAILED` (proxy connectivity, not cookie issue)
- Legacy: `HARVEST_INTERVAL_SECONDS` is kept for compatibility but no longer controls primary scheduling

---

## Cookie Reuse Cap Deployment (2025-12-23 - FINAL)

### Clean Restart with Bandwidth Tracking
**Date:** 2025-12-23 19:25 UTC

**What was done:**
1. Stopped all containers
2. Ran `scripts/clean_start.sh`:
   - Archived logs to `logs/archive/2025-12-23_19-25-14`
   - Deleted all progress files (fresh 0/11600 tiles)
   - Cleared tower output files
3. Flushed Redis (all counters at 0)
4. **Enabled bandwidth tracking**: `ENABLE_PROXY_BYTES=true` (was accidentally left disabled)
5. Restarted all services with 3 harvesters

**Active Configuration:**
- `REQUESTS_PER_SESSION=1` (single-use rotation)
- `REQUEST_DELAY_MIN/MAX=12/20s` (conservative pacing)
- `MAX_COOKIE_REUSE=3` (discard cookies after 3 uses)
- `TARGET_POOL_SIZE=20` cookies
- `ENABLE_PROXY_BYTES=true` (tracking bandwidth per proxy)

**Expected Outcome:**
- CAPTCHA rate <5% (ideally <2%)
- Cookie reuse cap preventing 15-20× reuse loops
- Bandwidth data accumulating in Redis `proxy:bytes:YYYY-MM-DD` keys
- Clean metrics for accurate measurement

---

## CAPTCHA Reduction Deployment (2025-12-23)

### Changes Deployed
1. **Single-use sessions**: `REQUESTS_PER_SESSION` 2 → 1 (every proxy+cookie pair discarded after 1 request)
2. **Slower pacing**: default `REQUEST_DELAY_MIN/MAX` 8/15 → 12/20 seconds
3. **Scaled cookie supply**: 
   - `TARGET_POOL_SIZE` 6 → 20 cookies
   - Harvester replicas: 1 → 3 (harvester-1, -2, -3)
4. **Clean-start helper**: `scripts/clean_start.sh` added for full reset

### Deployment Status
- ✅ All Docker images rebuilt with new settings
- ✅ All services restarted (Redis, FlareSolverr, 3 harvesters, 6 workers)
- ✅ Redis flushed - all counters at 0
- ✅ Workers waiting for cookie pool >= 6 before starting
- ⚠️  Harvesters encountering issues:
  - Some proxies timeout (90s) loading cellmapper.net
  - Some cookies missing JSESSIONID → fail API validation
  - Pool not reaching target yet (< 6 cookies)

### Next Steps
- Monitor harvester logs for successful cookie mints
- Check proxy quality/rotation in config/proxies_tmobile_west.txt
- Verify CAPTCHA_API_KEY is set if using CapMonster/2Captcha
- Once pool >= 6, workers will start and we can measure CAPTCHA rate

---

## Proxy Bandwidth Tracking (2025-12-22)

### Feature Overview
Track data usage (bytes sent/received) per proxy endpoint to identify bandwidth-heavy proxies and optimize costs.

### How It Works
- `scraper/api_client.py` logs bytes sent/received for each API request when `ENABLE_PROXY_BYTES=true`
- Data stored in Redis hash keys: `proxy:bytes:YYYY-MM-DD`
- Each hash contains fields: `{host:port:sent}` and `{host:port:recv}` (bytes as float)
- Keys expire after 30 days automatically

### Enable Tracking
Set environment variable in `docker-compose.yml` or shell:
```bash
ENABLE_PROXY_BYTES=true
```

### View Bandwidth Usage
```bash
# Show today's usage
python scripts/show_proxy_bytes.py

# Show specific date
python scripts/show_proxy_bytes.py --date 2025-12-22

# Show all available dates
python scripts/show_proxy_bytes.py --all
```

### Example Output
```
================================================================================
PROXY BANDWIDTH USAGE - 2025-12-22
================================================================================

Total Sent:     145.32 MB
Total Received: 3.24 GB
Total:          3.39 GB
Proxies Used:   30

================================================================================
Proxy Endpoint                           Sent            Recv            Total          
================================================================================
gate.decodo.com:10001                    5.12 MB         120.45 MB       125.57 MB      
gate.decodo.com:10002                    4.98 MB         115.23 MB       120.21 MB      
...

TOP 5 BANDWIDTH CONSUMERS:
--------------------------------------------------------------------------------
1. gate.decodo.com:10001: 125.57 MB (3.7% of total)
2. gate.decodo.com:10002: 120.21 MB (3.5% of total)
...
```

### Notes
- Byte counts are estimates (includes request/response headers + body)
- Tracking overhead is minimal (~1-2% of request time)
- Data is stored per-worker, aggregated by date
- Use this to identify proxy issues, optimize rotation, or negotiate bandwidth costs

---

## Anti-CAPTCHA + Bandwidth Tuning (2025-12-23)

### Change: Single-use proxy/cookie sessions
- Set `REQUESTS_PER_SESSION=1` to rotate after every request, minimizing per-IP burst patterns and reducing CAPTCHA triggers.

### Change: Slower default request jitter
- Updated defaults to `REQUEST_DELAY_MIN=12s` and `REQUEST_DELAY_MAX=20s` (env overrides still take precedence).

### Change: Scale cookie supply
- Updated `docker-compose.yml` harvester to `TARGET_POOL_SIZE=20` and set `deploy.replicas=3` (for `docker compose --scale harvester=3` or swarm usage).

### Ops: Clean-start helper
- Added `scripts/clean_start.sh` to archive `logs/` + reset local progress/towers + clear Redis keys (`cellmapper:counters:*`, `cellmapper:cookie:*`, `proxy:bytes:*`).

### Validation
- `python -m compileall`: OK
- `pytest`: 11 passed

---

## Cookie Reuse Cap (2025-12-23)

### Change: Track cookie use_count + preserve created_at
- Added `use_count` to pooled cookie JSON and `PooledCookie` object.
- `put_back()` increments `use_count`, preserves original `created_at`, and discards cookies once `use_count >= MAX_COOKIE_REUSE` (default 3) to reduce NEED_RECAPTCHA without scaling harvesters.

### Config knob
- Added `MAX_COOKIE_REUSE` (env, default 3) in `config/settings.py`.

### Compose
- Passed `MAX_COOKIE_REUSE` into all worker services in `docker-compose.yml`.

### Metrics
- Added Redis counter `cellmapper:counters:cookie_reuse_limit` (incremented when a cookie is discarded for hitting `MAX_COOKIE_REUSE`) and displayed in `scripts/show_metrics.py`.

### Early on-server check (post-deploy)
- After ~10 minutes: `NEED_RECAPTCHA` rate trended down to ~2.7% (4 / 146), with `cookie_reuse_limit` discarding 32 cookies.

---

## Systematic CAPTCHA/Bandwidth Testing (2025-12-28)

### Overview
Implemented systematic testing framework to break the CAPTCHA-vs-bandwidth loop.

### Files Created/Modified

**New Files:**
- `EXPERIMENT_LOG.md` - Structured experiment tracking document
- `scripts/snapshot_metrics.py` - Metrics snapshot tool for before/after comparison
- `scripts/run_experiment.sh` - Experiment runner script
- `docker-compose.test-a.yml` - Test A config (no proxy for harvester)
- `docker-compose.test-b.yml` - Test B config (persistent harvester)
- `docker-compose.test-c.yml` - Test C config (combined A+B)

**Modified Files:**
- `scraper/cookie_manager.py` - Added `PersistentHarvester` class
- `scripts/harvest_cookies.py` - Added persistent harvester mode + bandwidth tracking

### Test Approaches

| Test | Approach | Goal |
|------|----------|------|
| A | Harvester uses Hetzner direct IP (no proxy) | Reduce harvester bandwidth cost |
| B | Persistent Playwright profile (reuse context) | Reduce bandwidth per cookie mint |
| C | Combined A + B | Best of both |
| D | Workers use Hetzner direct IP (no proxy), harvester unchanged | Reduce worker proxy bandwidth cost |
| E | Combined D + B | Best case cost reduction |

### New Environment Variables

```bash
# Test B: Persistent harvester mode
USE_PERSISTENT_HARVESTER=true|false  # Default: false
PERSISTENT_MINTS_PER_CONTEXT=10      # Rotate context every N mints
```

### New Metrics

- `cellmapper:counters:harvest_bytes_total` - Total harvester bandwidth
- `harvester:bytes:YYYY-MM-DD` - Daily harvester bandwidth hash

### Running Experiments

```bash
# Run a test (30 minutes each)
./scripts/run_experiment.sh test-a
./scripts/run_experiment.sh test-b
./scripts/run_experiment.sh test-c
./scripts/run_experiment.sh test-d
./scripts/run_experiment.sh test-e

# Reset between tests
./scripts/run_experiment.sh reset

# Take manual snapshots
python scripts/snapshot_metrics.py --label "my_snapshot"

# Compare snapshots
python scripts/snapshot_metrics.py --compare test_a_start test_a_end
```

### Next Steps
1. SSH to Hetzner and pull latest code
2. Run `./scripts/run_experiment.sh reset`
3. Run `./scripts/run_experiment.sh test-d` (20 min) - your hypothesis
4. Check results in `experiment_results/` folder
5. If D works, run `test-e`; if D fails, try `test-a`

### Output Auto-Save (2025-12-28)
- All test output is automatically saved to `experiment_results/<test>_<timestamp>.txt`
- Duration changed from 30 min → 20 min per test
- Results persist even after Redis reset

