# CellMapper Scraper — Experiment Log (Phase 2: Optimize Baseline B)

> **Purpose:** Iterate on the new baseline (PersistentHarvester) to further reduce bandwidth cost without hurting CAPTCHA rate or throughput.
>
> **Baseline for Phase 2 (“Baseline B”)**: `USE_PERSISTENT_HARVESTER=true`, `PERSISTENT_MINTS_PER_CONTEXT=10`, Decodo residential proxies.
>
> **Reference:** Phase 1 results are in `EXPERIMENT_LOG.md`.

---

## Experiment Overview

Phase 2 tests are “B-variants” that tweak Baseline B:

| Test | Change vs Baseline B | Goal |
|------|----------------------|------|
| baseline-b | Current production config (Baseline B) | Establish reference metrics |
| b1 | `PERSISTENT_MINTS_PER_CONTEXT=25` | Reduce BW/cookie further via more cache reuse |
| b2 | `PERSISTENT_MINTS_PER_CONTEXT=50` | Push cache reuse further (watch fingerprinting) |
| b3 | `MAX_COOKIE_REUSE=20` | Reduce cookie churn (fewer harvests) |
| b4 | Webshare proxies (rotating) | Reduce $/GB (quality unknown) |
| b5 | Best combination | “Best overall” after we learn from b1–b4 |

### Metrics captured (same flow as Phase 1)

- **CAPTCHA rate** = `api_need_recaptcha / (api_requests_ok + api_need_recaptcha)`
- **Bandwidth** (from Redis): worker proxy total + harvester total
- **BW per cookie** (harvester_total / harvest_validated)
- **Throughput**: API OK, cookies minted, cookies checked out

---

## baseline-b: Baseline B (PersistentHarvester)

### Configuration

- `docker-compose.yml` harvester env:
  - `USE_PERSISTENT_HARVESTER=true`
  - `PERSISTENT_MINTS_PER_CONTEXT=10`
- Proxies: Decodo residential (sticky via proxy list)

### Timeline

| Event | Time | Notes |
|-------|------|-------|
| Start | 11:43:14 UTC | All services started |
| 5-min | 11:48 UTC | CAPTCHA rate: ~9% |
| 10-min | 11:53 UTC | CAPTCHA rate: ~15% |
| 15-min | 11:58 UTC | CAPTCHA rate: ~17% |
| End (20 min) | 12:03:26 UTC | CAPTCHA rate: 18.04% |

### Metrics (start → end)

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| api_requests_ok | 0 | 268 | +268 |
| api_need_recaptcha | 0 | 59 | +59 |
| **CAPTCHA rate %** | 0% | **18.04%** | - |
| harvest_validated | 0 | 74 | +74 |
| cookies_checked_out | 0 | 258 | +258 |
| Harvester bandwidth | 0 | 14.45 MB | +14.45 MB |
| Worker proxy bandwidth | 0 | 1.11 MB | +1.11 MB |
| **Total bandwidth** | 0 | 15.56 MB | +15.56 MB |
| **Harvester BW per cookie** | - | **200 KB** | - |
| Estimated cost (@ $2/GB) | $0 | $0.0304 | +$0.0304 |

### Observations

- [x] Cookie pool stable? Yes, maintained 3-10 cookies
- [x] Any proxy 5xx spikes? Minor at start, recovered quickly
- [x] CAPTCHA rate comparable to Phase 1 baseline? Yes, ~18% (same as Phase 1)

### Conclusion

**Verdict:** ✅ SUCCESS (establishes Phase 2 baseline)

**Notes:**
- This is the new baseline for Phase 2 tests
- 200 KB/cookie confirms PersistentHarvester working
- CAPTCHA rate consistent with Phase 1 (~18%)

---

## b1: Increase mints per context (25)

### Change

- `PERSISTENT_MINTS_PER_CONTEXT=25` (up from 10)

### Timeline

| Event | Time | Notes |
|-------|------|-------|
| Start | 12:07:48 UTC | All services started, data reset to 0 |
| 5-min | 12:13 UTC | CAPTCHA rate: ~16% |
| 10-min | 12:18 UTC | CAPTCHA rate: ~18% |
| 15-min | 12:23 UTC | CAPTCHA rate: ~18% |
| End (20 min) | 12:28:01 UTC | CAPTCHA rate: 18.55% |

### Metrics (start → end)

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| api_requests_ok | 0 | 224 | +224 |
| api_need_recaptcha | 0 | 51 | +51 |
| **CAPTCHA rate %** | 0% | **18.55%** | - |
| harvest_validated | 0 | 60 | +60 |
| cookies_checked_out | 0 | 217 | +217 |
| Harvester bandwidth | 0 | 11.72 MB | +11.72 MB |
| Worker proxy bandwidth | 0 | 857 KB | +857 KB |
| **Total bandwidth** | 0 | 12.56 MB | +12.56 MB |
| **Harvester BW per cookie** | - | **200 KB** | - |
| Estimated cost (@ $2/GB) | $0 | $0.0245 | +$0.0245 |

### Comparison to Baseline-B

| Metric | Baseline-B | B1 | Change |
|--------|------------|-----|--------|
| CAPTCHA rate | 18.04% | 18.55% | +0.5% (same) |
| Harvester BW | 14.45 MB | 11.72 MB | -19% |
| BW/cookie | 200 KB | 200 KB | Same |
| Total BW | 15.56 MB | 12.56 MB | -19% |
| Cost | $0.0304 | $0.0245 | -19% |

### Conclusion

**Verdict:** ⚠️ PARTIAL - No improvement in BW/cookie

**Notes:**
- BW per cookie remains 200 KB (no improvement from more context reuse)
- Total BW lower due to fewer cookies minted (60 vs 74)
- CAPTCHA rate same as baseline (~18%)
- The 25 mints/context didn't reduce per-cookie bandwidth as hoped

---

## b2: Increase mints per context (50)

### Change

- `PERSISTENT_MINTS_PER_CONTEXT=50`

### Timeline / Metrics / Conclusion

(same template as baseline-b)

---

## b3: Increase cookie reuse (20)

### Change

- Workers: `MAX_COOKIE_REUSE=20` (up from 10)

### Timeline

| Event | Time | Notes |
|-------|------|-------|
| Start | 12:32:33 UTC | All services started, data reset to 0 |
| 5-min | 12:37 UTC | CAPTCHA rate: ~15% |
| 10-min | 12:42 UTC | CAPTCHA rate: ~17% |
| 15-min | 12:47 UTC | CAPTCHA rate: ~18% |
| End (20 min) | 12:52:45 UTC | CAPTCHA rate: 18.46% |

### Metrics (start → end)

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| api_requests_ok | 0 | 212 | +212 |
| api_need_recaptcha | 0 | 48 | +48 |
| **CAPTCHA rate %** | 0% | **18.46%** | - |
| harvest_validated | 0 | 62 | +62 |
| cookies_checked_out | 0 | 200 | +200 |
| cookie_reuse_limit | 0 | 0 | 0 |
| Harvester bandwidth | 0 | 12.11 MB | +12.11 MB |
| Worker proxy bandwidth | 0 | 758 KB | +758 KB |
| **Total bandwidth** | 0 | 12.85 MB | +12.85 MB |
| **Harvester BW per cookie** | - | **200 KB** | - |
| Estimated cost (@ $2/GB) | $0 | $0.0251 | +$0.0251 |

### Comparison to Baseline-B

| Metric | Baseline-B | B3 | Change |
|--------|------------|-----|--------|
| CAPTCHA rate | 18.04% | 18.46% | +0.4% (same) |
| Cookies minted | 74 | 62 | -16% |
| Cookies checked out | 258 | 200 | -22% |
| Total BW | 15.56 MB | 12.85 MB | -17% |
| Cost | $0.0304 | $0.0251 | -17% |

### Conclusion

**Verdict:** ⚠️ PARTIAL - Cookie reuse limit not hit

**Notes:**
- `cookie_reuse_limit` counter stayed at 0 - no cookies reached 20 uses
- Lower throughput (200 vs 258 cookies checked out)
- CAPTCHA rate same as baseline (~18%)
- The higher reuse limit didn't provide measurable benefit in 20 min
- Cookies may be expiring or getting CAPTCHA'd before hitting 20 uses

---

## b4: Webshare proxies (rotating)

### Change

- Harvester proxy list switched to Webshare rotating proxies.
- Workers continue using cookie→proxy affinity (they’ll follow whatever proxy URL is embedded in cookies).

### Timeline / Metrics / Conclusion

(same template as baseline-b)

---

## b5: Best combination

### Change

Combine best-performing settings from b1–b4 (document exact env + rationale here).

### Timeline / Metrics / Conclusion

(same template as baseline-b)

---

## Phase 2 Decision Matrix

| Test | CAPTCHA rate | BW/cookie | Total BW/20m | Cost/20m | Recommend? |
|------|--------------|-----------|--------------|----------|------------|
| baseline-b | 18.04% | 200 KB | 15.56 MB | $0.030 | Current |
| b1 | 18.55% | 200 KB | 12.56 MB | $0.025 | No improvement |
| b2 | SKIP | SKIP | SKIP | SKIP | Skipped |
| b3 | 18.46% | 200 KB | 12.85 MB | $0.025 | No improvement |
| b4 | | | | | Pending (Webshare) |
| b5 | | | | | Pending |


