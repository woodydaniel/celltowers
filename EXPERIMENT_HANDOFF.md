# CellMapper — Experiment Handoff (New Agent Ready)

## What this repo is doing

We scrape CellMapper tower data at scale. The core blocker is a three-way tradeoff:

- **CAPTCHA rate**: keep `NEED_RECAPTCHA` low
- **Bandwidth cost**: proxy bandwidth is expensive (Decodo/Smartproxy)
- **Throughput**: tiles/hour

The system uses:

- **Harvester**: mints real browser cookies (Playwright + stealth), validates via API probe, stores into Redis.
- **Workers**: pull cookies from Redis and call `api.cellmapper.net` (tls-client fingerprint).
- **Redis**: cookie pool + counters + bandwidth accounting.

Key docs:

- `CHAT_SUMMARY_CAPTCHA_BANDWIDTH_LOOP.md`: full background + “why”
- `EXPERIMENT_LOG.md`: the canonical record of experiment results
- `SCRATCHPAD.md`: operational history and prior deployments

## Current experiment framework

We run 30-minute tests with start/end snapshots:

- Snapshots: `scripts/snapshot_metrics.py`
- Runner: `scripts/run_experiment.sh`
- Per-test compose overrides: `docker-compose.test-*.yml`

### How to run a test

On the server (Hetzner):

```bash
./scripts/run_experiment.sh reset
./scripts/run_experiment.sh test-a
./scripts/run_experiment.sh reset
./scripts/run_experiment.sh test-b
./scripts/run_experiment.sh reset
./scripts/run_experiment.sh test-c
./scripts/run_experiment.sh reset
./scripts/run_experiment.sh test-d
./scripts/run_experiment.sh reset
./scripts/run_experiment.sh test-e
```

### What “success” looks like

- **CAPTCHA rate** stays \(< 5%\) during the 30-min window (ideally \(< 2%\))
- **Bandwidth deltas** match the intended lever:
  - Test A/C: harvester bandwidth cost drops (no proxy for harvester)
  - Test B/E: harvester bandwidth per cookie drops (persistent browser)
  - Test D/E: worker proxy bandwidth drops (workers go direct)

## Test overview (what each does)

| Test | Compose override | Main lever |
|------|-----------------|------------|
| A | `docker-compose.test-a.yml` | Harvester uses Hetzner direct (no proxy) |
| B | `docker-compose.test-b.yml` | Persistent harvester (reuse context) |
| C | `docker-compose.test-c.yml` | A + B combined |
| D | `docker-compose.test-d.yml` | Workers direct from Hetzner, harvester unchanged |
| E | `docker-compose.test-e.yml` | B + D combined |

## Critical implementation details (don’t lose these)

### 1) Cookie pool + validation

- Harvester pushes only `validated=True` cookies into Redis (`scraper/cookie_pool.py`).
- Validation probes the real endpoint (`getTowers`) via `CookieManager._validate_cookies_via_api(...)`.

### 2) TLS fingerprint alignment

- Workers use `tls-client` fingerprint `chrome_125` to match Playwright UA and reduce bot detection.

### 3) Cookie → proxy affinity (important for Test D/E)

By default, workers *may* attempt to reuse the proxy that a cookie was minted with (cookie carries `proxy_url` in Redis).

For **Test D/E**, workers must run **direct** even if cookies were minted via proxy. This is controlled via `USE_PROXY=false` in the worker containers (set in `docker-compose.test-d.yml` / `docker-compose.test-e.yml`).

If you see workers still using proxies during Test D/E, re-check the code path in `scraper/api_client.py` where `pooled_proxy = cookie_manager.get_proxy_url()` is applied.

## Where to look for results

- Stored snapshots: `python scripts/snapshot_metrics.py --show-all`
- Compare snapshots: `python scripts/snapshot_metrics.py --compare test_d_start test_d_end`
- Proxy bandwidth: `python scripts/show_proxy_bytes.py`
- Harvester/worker logs: `docker compose logs -f harvester` and worker service logs

## Common gotchas

- **Config drift**: env overrides can silently change behavior; always log config at startup.
- **Cookies can be IP-bound**: if cookies minted via proxy stop working from Hetzner, Test D will fail quickly.
- **Proxy quality variance**: some exits time out; watch for transport errors vs `NEED_RECAPTCHA`.

## Decision tree (quick)

1. If Test A succeeds (low CAPTCHA) → harvesting off proxy is the biggest win; consider running harvesters direct permanently.
2. If Test A fails but Test B succeeds → keep proxies but reduce BW via persistent harvester.
3. If Test D succeeds → move workers direct and keep proxy only for cookie minting (high leverage).
4. If Test E succeeds → best-case cost reduction; likely the “final” configuration.


