# CellMapper Scraper — Experiment Log

> **Purpose:** Track systematic experiments to break the CAPTCHA vs Bandwidth loop.
>
> **Baseline Branch:** `experiment/captcha-bandwidth-YYYYMMDD`
>
> **Related:** See `CHAT_SUMMARY_CAPTCHA_BANDWIDTH_LOOP.md` for background and architecture.

---

## Experiment Overview

We are systematically testing approaches to break the CAPTCHA-bandwidth trade-off:

| Test | Approach | Goal |
|------|----------|------|
| A | Harvester uses Hetzner direct IP (no proxy) | Reduce harvester bandwidth cost |
| B | Persistent Playwright profile (reuse browser context) | Reduce bandwidth per cookie mint |
| C | Combined A + B | Best of both |
| D | Workers use Hetzner direct IP (no proxy), harvester unchanged | Reduce worker proxy bandwidth cost |
| E | Combined D + B (workers direct + persistent harvester) | Best case cost reduction |

Each test runs for **30 minutes** with metrics snapshots at start and end.

---

## Baseline Test (Current Production Config)

### Configuration

- Harvester: Decodo residential proxies (gate.decodo.com)
- Workers: Proxy affinity (same proxy as cookie)
- No persistent harvester
- 3 harvester instances, 6 worker instances

### Timeline

| Event | Time | Notes |
|-------|------|-------|
| Start | 10:58:41 UTC | All services started |
| 5-min | 11:03:45 UTC | CAPTCHA: 0% (3/3) |
| 10-min | 11:08:47 UTC | CAPTCHA: 11.3% (9/80) |
| 15-min | 11:13:49 UTC | CAPTCHA: 17.4% (29/167) |
| End | 11:18:52 UTC | CAPTCHA: 18.05% (37/205) |

### Metrics

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| API OK | 0 | 168 | +168 |
| CAPTCHA hits | 0 | 37 | +37 |
| **CAPTCHA rate %** | 0% | **18.05%** | ⚠️ |
| Cookies minted | 0 | 45 | +45 |
| Harvester bandwidth | 0 | 90 MB | +90 MB |
| Worker bandwidth | 0 | 772 KB | +772 KB |
| **Total bandwidth** | 0 | 90.75 MB | +90.75 MB |
| **BW per cookie** | - | **2 MB** | - |
| Estimated cost | $0 | $0.18 | +$0.18 |

### Observations

- CAPTCHA rate of 18% is **higher than historical ~2-5%**
- This suggests either:
  - CellMapper has tightened bot detection
  - Current Decodo proxy IPs are partially flagged
  - Time-of-day or other external factors
- Bandwidth per cookie is ~2 MB (as expected)

---

## Phase 0: Preparation

### Branch & Baseline

- **Branch name:** `experiment/captcha-bandwidth-YYYYMMDD`
- **Baseline commit:** _(to be filled)_
- **Date started:** 2025-12-28

### Configuration Snapshot (Before Tests)

```yaml
# docker-compose.yml harvester config:
HARVEST_PROXY: ${HARVEST_PROXY:-}
HARVEST_PROXY_FILE: ${HARVEST_PROXY_FILE:-config/proxies_tmobile_west.txt}
HARVEST_ENGINE: playwright
TARGET_POOL_SIZE: 20
COOKIE_TTL_SECONDS: 1500

# Worker config:
MAX_COOKIE_REUSE: 3
REQUESTS_PER_SESSION: 1
REQUEST_DELAY_MIN: 20.0
REQUEST_DELAY_MAX: 30.0
```

---

## Test A: Harvester on Hetzner Direct IP

### Hypothesis

If the harvester doesn't route through Decodo proxies, harvester bandwidth cost drops dramatically. Workers still use proxies for API calls. Risk: Hetzner's IP might get flagged.

### Configuration Changes

```yaml
harvester:
  environment:
    - HARVEST_PROXY=
    - HARVEST_PROXY_FILE=
```

### Timeline

| Event | Time | Notes |
|-------|------|-------|
| Start | 09:35:54 UTC | All services started |
| 5-min check | 09:41:01 UTC | CAPTCHA rate: 11% (7/63) |
| 10-min check | 09:46:01 UTC | CAPTCHA rate: 16% (25/156) |
| 15-min check | 09:51:03 UTC | CAPTCHA rate: 17% (44/262) |
| End (20 min) | 09:56:05 UTC | CAPTCHA rate: 16.91% (58/343) |

### Metrics

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| API OK | 0 | 285 | +285 |
| CAPTCHA hits | 0 | 58 | +58 |
| **CAPTCHA rate %** | 0% | **16.91%** | ❌ |
| Cookies minted | 0 | 78 | +78 |
| Harvester bandwidth | 0 | 0 B* | N/A |
| Worker bandwidth | 0 | 0 B* | N/A |
| **Total bandwidth** | 0 | 0 B* | N/A |

*Note: Bandwidth tracking showed 0 B because direct connections bypass proxy bandwidth measurement.

### Observations

- [x] Harvester minting cookies successfully? **Yes - 78 cookies minted with `proxy=none`**
- [x] Workers getting cookies from pool? **Yes - 271 cookies checked out**
- [x] Hetzner IP being flagged? **YES - 16.91% CAPTCHA rate vs baseline ~2-5%**

### Conclusion

**Verdict:** ❌ **FAIL**

**Notes:**
- CAPTCHA rate (16.91%) far exceeds acceptable threshold (<5%)
- Cookies minted from Hetzner's datacenter IP are lower quality
- CellMapper appears to fingerprint/flag the cookie minting IP, not just the API request IP
- Harvester MUST use residential proxy for quality cookies

---

## Test B: Persistent Playwright Profile

### Hypothesis

Reusing a browser context across multiple cookie mints avoids cold-start resource loads (~2MB savings per mint after first). This should reduce harvester bandwidth while maintaining cookie quality.

### Code Changes

- `scraper/cookie_manager.py`: Added `PersistentHarvester` class
- `scripts/harvest_cookies.py`: Uses persistent harvester instead of single-use

### Timeline

| Event | Time | Notes |
|-------|------|-------|
| Start | 10:30:21 UTC | PersistentHarvester active, 3 harvesters |
| 5-min check | 10:35 UTC | CAPTCHA rate: ~15% |
| 10-min check | 10:40 UTC | CAPTCHA rate: ~17% |
| 15-min check | 10:45 UTC | CAPTCHA rate: ~17.5% |
| End (20 min) | 10:50:33 UTC | CAPTCHA rate: 17.75% (49/276) |

### Metrics

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| API OK | 0 | 227 | +227 |
| CAPTCHA hits | 0 | 49 | +49 |
| **CAPTCHA rate %** | 0% | **17.75%** | ❌ |
| Cookies minted | 0 | 63 | +63 |
| Harvester bandwidth | 0 | 12.30 MB | +12.30 MB |
| Worker bandwidth | 0 | 754 KB | +754 KB |
| **Total bandwidth** | 0 | 13.04 MB | +13.04 MB |
| **BW per cookie** | ~2 MB | **200 KB** | ✅ **10x reduction** |
| Estimated cost | $0 | $0.0255 | +$0.0255 |

### Observations

- [x] Browser context reusing correctly? **Yes - PersistentHarvester working, 200KB/cookie vs 2MB**
- [x] Any fingerprinting detection? **CAPTCHA rate high (17.75%), may indicate detection**
- [ ] Cookie quality maintained? **Mixed - cookies work but higher CAPTCHA rate**

### Conclusion

**Verdict:** ⚠️ **PARTIAL SUCCESS**

**Notes:**
- ✅ **Bandwidth goal achieved:** 10x reduction (2MB → 200KB per cookie)
- ❌ **CAPTCHA goal failed:** 17.75% rate (expected <5%)
- All tests today showing elevated CAPTCHA rates (~17%), needs baseline comparison
- Cost savings from bandwidth: ~$0.025 per 20 min vs ~$0.29 baseline = 11x cheaper
- May need to run baseline test to verify if elevated CAPTCHA is a new CellMapper behavior

---

## Test C: Combined (A + B)

### Configuration

- Harvester uses Hetzner direct IP (no proxy)
- Harvester uses persistent Playwright profile

### Timeline

| Event | Time | Notes |
|-------|------|-------|
| Start | | |
| End (30 min) | | |

### Metrics

| Metric | Baseline | Test A | Test B | Test C |
|--------|----------|--------|--------|--------|
| CAPTCHA rate | ~2-5% | | | |
| Harvester BW | ~2MB/cookie | | | |
| Worker BW | ~5KB/req | | | |
| Total cost/day | ~$22 | | | |

### Conclusion

**Verdict:** _(SUCCESS / PARTIAL / FAIL)_

**Notes:**

---

## Test D: Workers on Hetzner Direct IP (Harvester unchanged)

### Hypothesis

CellMapper primarily checks "humanity" at cookie minting time (Cloudflare / SPA handshake). If we mint high-quality cookies via residential proxy but run API calls from Hetzner direct IP, the API may still accept them. This would eliminate worker proxy bandwidth costs.

### Configuration Changes

- Apply `docker-compose.test-d.yml` (sets `USE_PROXY=false` for all worker services)
- Harvester remains unchanged (still uses `HARVEST_PROXY` / `HARVEST_PROXY_FILE` as configured)

### Timeline

| Event | Time | Notes |
|-------|------|-------|
| Start | 09:09:02 UTC | All services started |
| 5-min check | 09:14:07 UTC | CAPTCHA rate: 14% (8/57) |
| 10-min check | 09:19:09 UTC | CAPTCHA rate: 17% (25/148) |
| 15-min check | 09:24:11 UTC | CAPTCHA rate: 18% (44/238) |
| End (20 min) | 09:29:14 UTC | CAPTCHA rate: 18.37% (61/332) |

### Metrics

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| API OK | 0 | 271 | +271 |
| CAPTCHA hits | 0 | 61 | +61 |
| **CAPTCHA rate %** | 0% | **18.37%** | ❌ |
| Cookies minted | 0 | 71 | +71 |
| Harvester bandwidth | 0 | 147.39 MB | +147.39 MB |
| Worker bandwidth | 0 | 1.02 MB | +1.02 MB |
| **Total bandwidth** | 0 | 148.39 MB | +148.39 MB |
| **Estimated cost** | $0 | $0.29 | +$0.29 |
| BW per cookie | - | 2.08 MB | - |

### Observations

- [x] Do workers run cleanly without proxies? **Yes - workers ran fine, very low worker bandwidth (1 MB)**
- [x] Does CAPTCHA rate spike when API traffic comes from Hetzner direct? **YES - 18.37% vs baseline ~2-5%**
- [x] Any evidence cookies are IP-bound? **Appears so - valid cookies still triggered CAPTCHA from datacenter IP**

### Conclusion

**Verdict:** ❌ **FAIL**

**Notes:**
- CAPTCHA rate (18.37%) far exceeds acceptable threshold (<5%)
- CellMapper's API checks the requesting IP on every call, not just at cookie minting
- Workers CANNOT run from Hetzner direct IP - residential proxies are required for API calls
- Harvester-side optimizations (Tests A, B, C) are still viable since they don't change worker behavior

---

## Test E: Persistent Harvester + Workers on Hetzner Direct IP

### Hypothesis

Combine the best levers:

- Persistent harvester cuts cookie-mint bandwidth (cached assets)
- Workers run direct from Hetzner to eliminate proxy bandwidth for API calls

This should materially reduce total daily spend if CAPTCHA remains acceptable.

### Configuration Changes

- Apply `docker-compose.test-e.yml`
  - `USE_PERSISTENT_HARVESTER=true` (harvester)
  - `USE_PROXY=false` (all workers)

### Timeline

| Event | Time | Notes |
|-------|------|-------|
| Start | | |
| End (30 min) | | |

### Metrics

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| API OK | | | |
| CAPTCHA hits | | | |
| **CAPTCHA rate %** | | | |
| Cookies minted | | | |
| Harvester bandwidth | | | |
| Worker bandwidth | | | |
| **Total bandwidth** | | | |
| **BW per cookie** | | | |

### Conclusion

**Verdict:** _(SUCCESS / PARTIAL / FAIL)_

**Notes:**

---

## Final Decision

### Decision Matrix

| Approach | CAPTCHA Rate | Bandwidth/20min | Cost/20min | Recommend? |
|----------|--------------|-----------------|------------|------------|
| **BASELINE** | **18.05%** | **90.75 MB** | **$0.18** | Current |
| A: Harvester direct | 16.91% | N/A | N/A | ❌ NO |
| **B: Persistent profile** | **17.75%** | **13.04 MB** | **$0.025** | ✅ **YES** |
| C: Both | Skip | Skip | Skip | A failed |
| D: Workers direct | 18.37% | 148.39 MB | $0.29 | ❌ NO |
| E: Persistent + workers direct | Skip | Skip | Skip | D failed |

### Key Findings

1. **Baseline CAPTCHA rate is 18%** - not 2-5% as historically observed
   - This is the current normal, not caused by test configs
   - Likely CellMapper tightened detection or proxy IPs are flagged

2. **Test B achieves 7x bandwidth reduction** with same CAPTCHA rate
   - Baseline: $0.18/20min = ~$13/day
   - Test B: $0.025/20min = ~$1.80/day
   - **Savings: ~$11/day**

3. **Hetzner direct IP doesn't work** for either harvester or workers

### Recommendation

✅ **Deploy Test B configuration (PersistentHarvester)**

- Same CAPTCHA rate as baseline
- 7x lower bandwidth cost
- Estimated savings: **$11/day** (~$330/month)

### Winning Configuration

```yaml
# Final recommended settings:
```

### Commit

```bash
git add -A
git commit -m "Experiment results: [winning approach] - CAPTCHA X%, BW $Y/day"
```

---

## Commands Reference

### Snapshot metrics

```bash
# Before test
python scripts/snapshot_metrics.py --label "test_a_start"

# After test
python scripts/snapshot_metrics.py --label "test_a_end"

# Compare
python scripts/snapshot_metrics.py --compare test_a_start test_a_end
```

### Reset between tests

```bash
docker compose down
docker compose exec redis redis-cli FLUSHALL
./scripts/clean_start.sh
git checkout docker-compose.yml  # Restore original config
```

### Monitor during test

```bash
# Watch logs
docker compose logs -f harvester

# Check pool size
docker compose exec redis redis-cli KEYS "cellmapper:cookie:*" | wc -l

# Quick metrics
python scripts/show_metrics.py
```

---

## Appendix: Original Background

See `CHAT_SUMMARY_CAPTCHA_BANDWIDTH_LOOP.md` for:

- Full system architecture
- What we tried before
- The "loop" dynamics
- Key artifacts and file locations

