# CellMapper Scraper — Comprehensive Chat Summary (CAPTCHA vs Bandwidth vs Speed)

> **Purpose:** A single, in-depth narrative of what we implemented, what we tried, what failed, what worked, and what the current “loop” is (CAPTCHA rate vs proxy bandwidth vs scraping speed).
>
> **Time window:** This summary reflects the changes and operational observations captured in `SCRATCHPAD.md` plus the git history up through commit `58b6a70` (`efficient-harvester`).

---

## Executive Summary (1-page)

### What’s working now

- **CAPTCHA rate reduction**: The system can run at roughly **<5%** `NEED_RECAPTCHA` in many periods by combining:
  - Redis-backed **cookie pool**
  - Playwright + stealth **cookie harvester**
  - **TLS fingerprint matching** in workers (`tls-client` with `chrome_125`)
  - **Aggressive session rotation** (`REQUESTS_PER_SESSION=1`)
  - **Cookie reuse cap** (`MAX_COOKIE_REUSE`)
- **Recovery flow works**: When the API returns `NEED_RECAPTCHA`, workers can **poison** the current cookie and **swap** to a fresh cookie from Redis and continue.
- **Observability exists**: We added Redis counters for API success / CAPTCHA hits and **proxy bandwidth tracking** so we can quantify cost drivers.

### What’s *not* solved

- **Harvester bandwidth is the main cost driver.** Minting cookies with Playwright can consume **MB-level bandwidth per cookie**. When we scale harvesters or shorten cookie lifetimes, bandwidth spend spikes.
- **The system naturally falls into a loop**:
  - To reduce CAPTCHA: rotate faster + mint more cookies → **more harvester traffic** → **higher proxy bandwidth costs**
  - To reduce bandwidth: mint fewer cookies (reuse more) → cookies age / reuse patterns emerge → **CAPTCHA creeps back up**

### The central loop

You are trading three knobs that fight each other:

- **Speed (tiles/hour)** wants: low delays, fewer waits, fewer retries.
- **CAPTCHA rate** wants: slower pacing, frequent rotation, high-quality cookies, good IPs.
- **Bandwidth cost** wants: fewer browser sessions (cookie mints), fewer heavy page loads, fewer retries/timeouts.

The current challenge is finding a stable “sweet spot” where:
- CAPTCHA stays **< 5%**
- cookie minting rate stays low enough that proxy bandwidth doesn’t “blow up”
- total throughput is acceptable.

---

## System Architecture (What We Built)

### Components

- **Workers (6)**: One per carrier-region pair (T-Mobile east/west, AT&T east/west, Verizon east/west).
  - They scrape tiles via `api.cellmapper.net/v6/...`
  - They use proxies (Decodo etc.) and cookie sessions.

- **Redis**: Shared state store for:
  - **Cookie pool** (cookies minted by harvester; TTL’d)
  - **Metrics** counters (CAPTCHA hits, OK count, harvest success/reject)
  - **Bandwidth accounting** (bytes per proxy per day)

- **Harvester(s)**:
  - A long-running process that mints cookies with Playwright + stealth
  - Validates cookies against the real `getTowers` endpoint before inserting into Redis
  - Keeps the pool at target size.

- **FlareSolverr (optional/legacy)**:
  - Used earlier for Cloudflare bypass, but its cookies were often rejected at the API layer.

### Data flows

1. Harvester mints cookie → validates via API probe → stores into Redis with TTL.
2. Worker starts → pulls cookie from Redis → performs API calls.
3. If API returns `NEED_RECAPTCHA` → worker marks cookie poisoned → pulls a new cookie from Redis → continues.
4. Bandwidth per proxy and key counters are tracked in Redis for analysis.

---

## What We Implemented (Current Baseline)

This is the “v2” architecture and tuning captured in `SCRATCHPAD.md` and recent commits:

### 1) Redis Cookie Pool

- **Why:** Avoid per-worker Playwright sessions (expensive, unstable, and difficult to scale).
- **How:** `scraper/cookie_pool.py`
  - Cookies stored as keys `cellmapper:cookie:*` with TTL (default ~25 minutes).
  - `get()` is **consuming** (GETDEL) so only one worker uses a cookie at a time.
  - `put()` requires `validated=True` so we don’t poison the pool with bad cookies.

### 2) Harvester (Playwright + stealth)

- **Why:** FlareSolverr could pass Cloudflare, but the API still rejected cookies unless the SPA handshake completed.
- **How:** `scripts/harvest_cookies.py` + `scraper/cookie_manager.py`
  - Uses Playwright with `playwright-stealth`.
  - Waits for the map canvas selector (handshake complete signal).
  - Stores UA alongside cookies in Redis (workers replay UA).
  - Validates cookies via `getTowers` probe before pool insertion.
  - Drip-feeds: harvest **one cookie at a time** when below target size.

### 3) TLS fingerprint matching in workers

- **Why:** Cookie minted by Chrome-ish browser + API requests from a mismatched TLS fingerprint can trigger bot detection.
- **How:** `scraper/api_client.py`
  - Uses `tls-client` with `client_identifier=os.environ.get("TLS_CLIENT_IDENTIFIER", "chrome_125")`.

### 4) Proactive rotation + tuning knobs

Key knobs:

- **`REQUESTS_PER_SESSION=1`**
  - Rotate after each successful request (minimize per-IP “burst” patterns).
- **Conservative request jitter**
  - Defaults moved to slower ranges; compose sometimes overrides to even slower per-worker delays.
- **`MAX_COOKIE_REUSE` (reuse cap)**
  - Prevents “reuse streaks” (e.g., 15–20 uses) that tend to trigger `NEED_RECAPTCHA`.
  - Implemented in the pool’s `put_back()` path with `use_count`.

### 5) Clean start tooling

- **Why:** We needed hard resets to measure real post-change metrics.
- **How:** `scripts/clean_start.sh`
  - Archives logs
  - Resets progress and output
  - Flushes Redis counters (so bandwidth + CAPTCHA metrics start at 0)

### 6) Bandwidth tracking

- **Why:** We discovered bandwidth spikes that weren’t explained by worker traffic.
- **How:** `scraper/api_client.py` tracks bytes per proxy into Redis hashes `proxy:bytes:YYYY-MM-DD`.
- **Viewer:** `scripts/show_proxy_bytes.py`

---

## What We Tried (By Branch / Phase) and What Didn’t Work

This section summarizes the major attempts, why they were tried, and why they failed or were insufficient.

### Phase A — Baseline / early parallelization (`main`)

- Added initial stealth improvements and parallel scrape support.
- **Problem:** CAPTCHA remained high; manual cookie management didn’t scale.

### Phase B — Hardening v2 (`hardening-v2-stable`)

Focus: resiliency and correctness under partial failures.

- **Per-tile retry budget + defer queue**
  - Prevents a run from getting stuck on a few toxic tiles.
- **Proxy CAPTCHA cooling**
  - Temporarily sidelines proxies that hit multiple CAPTCHAs.
- **Smaller grid size (0.25°)**
  - Fixes `hasMore=true` truncation in dense metro tiles.

**Why it wasn’t enough:** It improved run stability but didn’t change the fundamental CAPTCHA mechanism (session / fingerprint / reuse).

### Phase C — FlareSolverr & “performance upgrades” (`perf-upgrade-baseline`)

Focus: increase success + throughput with automated “bypass” services.

- Added FlareSolverr and CAPTCHA provider support.

**Failure mode discovered:** FlareSolverr cookies could “pass Cloudflare” but were **rejected by the CellMapper API** (still `NEED_RECAPTCHA`). The API appears to require cookies minted by a full browser session completing the SPA/XHR handshake.

### Phase D — Better proxies (`decodo-proxies`)

Focus: use higher-quality residential IPs and avoid cheap/banned exits.

- This generally helps success rates but introduces the **cost problem**: proxy bandwidth is expensive and must be controlled.

### Phase E — v2 architecture (`efficient-harvester`)

Focus: stop per-worker Playwright and share “good” cookies across workers.

Successes:
- Playwright cookies validated by real API probe.
- Cookie pool prevents pool-wide poisoning.
- TLS fingerprint matching reduces mismatch detection.

New failure / cost mode:
- **Harvester bandwidth becomes the dominant cost.**
  - If each cookie mint loads heavy pages, cookie minting becomes the bandwidth sink.
  - If we scale harvesters to maintain a big pool, bandwidth spikes.

---

## The “Loop” (Why It Keeps Feeling Like We’re Running in Circles)

This is the root dynamic:

### If we bias toward *low CAPTCHA*

We do:
- `REQUESTS_PER_SESSION=1` (rotate constantly)
- Lower cookie reuse (smaller `MAX_COOKIE_REUSE`)
- Increase pool size and harvesters to supply more “fresh” sessions
- Slow pacing to look human-ish

What happens:
- ✅ CAPTCHA rate drops
- ❌ Cookie minting rate rises (harvester works more)
- ❌ Browser-based minting tends to be **bandwidth heavy** (MBs per cookie)
- ❌ Proxy costs spike because harvest traffic dominates total bytes

### If we bias toward *low bandwidth cost*

We do:
- Increase `MAX_COOKIE_REUSE` (reuse cookies longer)
- Reduce harvester replicas / reduce pool target
- Try to keep harvest URL lightweight

What happens:
- ✅ Harvester does less work (fewer Playwright sessions)
- ✅ Proxy bandwidth cost drops
- ❌ Cookies age / reuse patterns become detectable
- ❌ CAPTCHA rate rises or recovery becomes more frequent

### If we bias toward *speed*

We do:
- Reduce request delays (or enable “fast mode”)
- Increase concurrency (more workers or lower cooldowns)

What happens:
- ✅ Tiles/hour increases
- ❌ Rate-limits and bot detection rise (CAPTCHA, 403, 429, timeouts)
- ❌ Cookie churn rises → harvest pressure rises → bandwidth rises

**Net:** Any attempt to push one axis hard tends to punish the other two.

---

## The Most Important Issues We Found (High Signal)

### 1) Cookie validity is API-layer, not just Cloudflare-layer

- FlareSolverr can “solve” Cloudflare challenges, but CellMapper’s API still rejects cookies unless the browser session completes the SPA/XHR handshake.
- This is why Playwright + stealth + “wait for map canvas” was required.

### 2) Fingerprint mismatch matters

- Even with good cookies, using a mismatched TLS fingerprint for API calls can increase `NEED_RECAPTCHA`.
- Aligning Playwright UA (Chrome major) with `tls-client` `client_identifier` materially improved stability.

### 3) Cookie reuse has a “poison threshold”

- Reusing a “good” cookie too many times (especially across varied IPs or too quickly) tends to trigger `NEED_RECAPTCHA`.
- The reuse cap (`MAX_COOKIE_REUSE`) was introduced to prevent the “15–20x reuse streak” failure mode.

### 4) Harvester traffic can dwarf worker traffic

- Worker API calls are small.
- Playwright minting can be large (HTML + JS bundles + assets).
- This creates a cost trap where “more cookies” or “more harvesters” increases spend dramatically.

### 5) Configuration drift / overrides caused repeated confusion

We repeatedly hit “we changed a setting but runtime still behaves like the old value” due to:
- `docker-compose.yml` defaults (e.g., `MAX_COOKIE_REUSE:-3`)
- `.env` overrides (e.g., `HARVEST_URL` forced to `/map`)

This produced false conclusions (thinking logic was wrong when it was config propagation).

---

## What’s Working vs What Still Needs Attention

### Working

- **End-to-end harvesting + validation + pool insertion**
- **Worker consumption + cookie poisoning + recovery**
- **TLS fingerprint matching**
- **Basic anti-bot pacing controls**
- **Bandwidth & counters observability**

### Still needs attention

1. **Harvester bandwidth minimization**
   - Ensure harvest loads the lightest possible route (homepage when feasible)
   - Continue blocking unnecessary resources
   - Avoid scaling harvesters as the first-line solution

2. **Stable “sweet spot” tuning**
   - Determine `MAX_COOKIE_REUSE`, pool size, harvester count that keeps CAPTCHA < 5% without blowing bandwidth

3. **Hard guardrails against config drift**
   - Eliminate ambiguous defaults
   - Make runtime config visible in logs at startup
   - Keep `.env` from overriding critical performance knobs unintentionally

---

## What We Have *Not* Fully Explored Yet

These are high-leverage directions that could break the loop, not just “tune within it”:

1. **Move harvesting off metered proxies**
   - Run harvester from a low-cost / unmetered bandwidth host, while workers still use proxy pool.
   - Goal: decouple “cookie mint bandwidth” from “worker scrape bandwidth.”

2. **Persistent Playwright profiles**
   - Reuse a single browser profile/context to mint multiple cookies without full cold-start resource loads.

3. **Two-tier harvesting**
   - Try a cheaper first pass (e.g., lightweight page) and only go “full map handshake” when needed.

4. **Request dedup / caching**
   - If the same tiles or towers are requested multiple times across retries, cache results to reduce API calls.

---

## Key Artifacts & Where to Look in the Repo

- **Core configuration:** `config/settings.py`
- **Compose / orchestration:** `docker-compose.yml`
- **Harvester loop:** `scripts/harvest_cookies.py`
- **Cookie minting + pool integration:** `scraper/cookie_manager.py`, `scraper/cookie_pool.py`
- **API client + TLS + bandwidth tracking:** `scraper/api_client.py`
- **Metrics / viewers:** `scripts/show_metrics.py`, `scripts/show_proxy_bytes.py`
- **Reset tooling:** `scripts/clean_start.sh`

---

## Practical Next Step (When You Resume)

When services are restarted, the first 30–60 minutes should be treated like an experiment:

- Log and watch:
  - CAPTCHA rate (`cellmapper:counters:api_need_recaptcha` / ok)
  - Cookies minted vs discarded (reuse cap counter)
  - Proxy bytes/day split (worker vs harvester)
- Only change one knob at a time:
  - `MAX_COOKIE_REUSE`
  - harvester replica count
  - pool target size
  - request delays

The goal is to find a stable plateau, not “max out” any single metric.



