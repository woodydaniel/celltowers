#!/usr/bin/env python3
"""
Batch reverse-geocoder for CellMapper tower data.

Source (and only output): downloads/towers_all_classified.jsonl

What this script does:
  1. Reads every record from towers_all_classified.jsonl
  2. Renames `skipped` -> `rural` on every record (idempotent)
  3. For the first --limit records where rural=false AND geocode_status="pending",
     calls the Smarty.com US Reverse Geocoding API concurrently
  4. Adds fields to every processed record:
       address, city, state, zipcode,
       geocode_distance (meters), geocode_accuracy (Rooftop/Parcel/Zip9/etc.),
       geocode_status (success | failed | invalid_coords | no_result)
  5. Atomically rewrites towers_all_classified.jsonl only — no splitting

NOTE: The Smarty US Reverse Geo API has no batch endpoint (GET only, one
      coordinate per call). Concurrency is achieved via asyncio + httpx.AsyncClient.

Crash/kill safety:
  - Results are applied in-place to the records list immediately after each API
    call completes (not held in a results dict until the end).
  - The master file is flushed to disk every CHECKPOINT_EVERY completions.
  - SIGTERM and SIGINT both trigger a final flush before exit, so the worst-case
    data loss is at most one checkpoint window (default: 1,000 records).
  - All results are also written to logs/geocode_audit.jsonl in real time, so
    even records lost between checkpoints can be recovered from that file.

Resume support:
  - Records with geocode_status != "pending" are skipped on restart.
  - Progress checkpointed to downloads/.geocode_progress.json every
    CHECKPOINT_EVERY completions.

Logging:
  - Console: INFO level (timestamps + messages)
  - File:    DEBUG level → logs/geocode_batch.log (rotating, 5 × 10 MB)
  - Audit:   Per-record JSONL → logs/geocode_audit.jsonl (appended each run)
  - Report:  Post-run quality report → logs/geocode_report.txt

Usage:
  source venv/bin/activate
  python scripts/geocode_batch.py                      # geocode up to 250,000 pending urban records
  python scripts/geocode_batch.py --limit 100          # test run: 100 records
  python scripts/geocode_batch.py --limit 100 --concurrency 10  # conservative test
  python scripts/geocode_batch.py --dry-run            # validate without calling API
"""

import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import random
import signal
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

SMARTY_AUTH_ID     = os.getenv("SMARTY_AUTH_ID")
SMARTY_AUTH_TOKEN  = os.getenv("SMARTY_AUTH_TOKEN")
SMARTY_API_URL     = "https://us-reverse-geo.api.smarty.com/lookup"
SMARTY_TIMEOUT     = 15    # seconds per request
SMARTY_MAX_RETRIES = 3
SMARTY_RETRY_DELAY = 1.5   # seconds between retries (exponential per attempt)

DEFAULT_LIMIT       = 250000  # full plan quota
DEFAULT_CONCURRENCY = 25      # parallel requests — sweet spot to avoid Smarty rate limiting
CHECKPOINT_EVERY    = 1000    # flush master file to disk every N completions

BASE_DIR       = Path(__file__).resolve().parent.parent
_DEFAULT_MASTER = BASE_DIR / "downloads" / "towers_all_classified.jsonl"
LOGS_DIR       = BASE_DIR / "logs"
AUDIT_LOG_FILE = LOGS_DIR / "geocode_audit.jsonl"
BATCH_LOG_FILE = LOGS_DIR / "geocode_batch.log"
REPORT_FILE    = LOGS_DIR / "geocode_report.txt"

# These are set dynamically in main() after --input is parsed
MASTER_FILE:   Path = _DEFAULT_MASTER
PROGRESS_FILE: Path = BASE_DIR / "downloads" / ".geocode_progress.json"
TEMP_MASTER:   Path = BASE_DIR / "downloads" / ".towers_all_classified.tmp"

# Continental US coordinate bounds
US_LAT_MIN, US_LAT_MAX =  24.0,  50.0
US_LON_MIN, US_LON_MAX = -125.0, -66.0


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger("geocode_batch")
    logger.setLevel(logging.DEBUG)

    # Console: INFO, concise
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S"
    ))

    # File: DEBUG, rotating (5 backups × 10 MB each)
    fh = logging.handlers.RotatingFileHandler(
        BATCH_LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s"))

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def validate_credentials(logger: logging.Logger):
    if not SMARTY_AUTH_ID or not SMARTY_AUTH_TOKEN:
        logger.critical("Smarty credentials missing. Set SMARTY_AUTH_ID and SMARTY_AUTH_TOKEN in .env")
        sys.exit(1)
    logger.info(f"Credentials OK  auth-id={SMARTY_AUTH_ID[:8]}...")


def is_valid_us_coord(lat, lon) -> bool:
    try:
        return (US_LAT_MIN <= float(lat) <= US_LAT_MAX and
                US_LON_MIN <= float(lon) <= US_LON_MAX)
    except (TypeError, ValueError):
        return False


def empty_geo_fields(status: str) -> dict:
    return {
        "address":          "",
        "city":             "",
        "state":            "",
        "zipcode":          "",
        "geocode_distance": None,
        "geocode_accuracy": "",
        "geocode_status":   status,
    }


def load_progress(logger: logging.Logger) -> int:
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text())
            completed = int(data.get("api_calls_completed", 0))
            if completed:
                logger.info(f"Resume: {completed:,} API calls already done in prior run(s)")
            return completed
        except (json.JSONDecodeError, ValueError):
            pass
    return 0


def save_progress(done: int):
    PROGRESS_FILE.write_text(json.dumps({
        "api_calls_completed": done,
        "last_updated":        datetime.now(timezone.utc).isoformat(),
    }, indent=2))


def flush_master_file(records: list, logger: logging.Logger):
    """
    Atomically write all records to the master JSONL file.

    Writes to a temp file first, then renames over the master so the file is
    never left in a partial state. Called both at checkpoints and on exit.
    """
    with open(TEMP_MASTER, "w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")
    TEMP_MASTER.replace(MASTER_FILE)
    logger.debug(f"Master file flushed ({len(records):,} records)")


def _tower_id_str(raw) -> str:
    if isinstance(raw, list):
        return raw[0] if raw else "unknown"
    return str(raw) if raw is not None else "unknown"


# ---------------------------------------------------------------------------
# Async geocoding
# ---------------------------------------------------------------------------

async def reverse_geocode_async(
    client: httpx.AsyncClient,
    lat: float,
    lon: float,
    semaphore: asyncio.Semaphore,
    abort_event: asyncio.Event,
    logger: logging.Logger,
) -> Optional[dict]:
    """
    Call Smarty reverse geo API for one coordinate.

    Returns a dict with geo fields plus three private meta keys:
      _attempts         int   — how many HTTP attempts were made
      _response_time_ms int   — wall-clock ms from semaphore entry to response
      _error            str?  — error label if request failed, else None

    Returns None if the abort_event was set (fatal API error or pre-abort).
    """
    params = {
        "auth-id":    SMARTY_AUTH_ID,
        "auth-token": SMARTY_AUTH_TOKEN,
        "latitude":   lat,
        "longitude":  lon,
    }

    async with semaphore:
        if abort_event.is_set():
            return None

        t0         = time.monotonic()
        last_error: Optional[str] = None

        for attempt in range(1, SMARTY_MAX_RETRIES + 1):
            try:
                resp = await client.get(
                    SMARTY_API_URL, params=params, timeout=SMARTY_TIMEOUT
                )
                elapsed_ms = round((time.monotonic() - t0) * 1000)

                if resp.status_code == 401:
                    if not abort_event.is_set():
                        logger.critical("FATAL 401: Authentication failed — aborting batch")
                    abort_event.set()
                    return None

                if resp.status_code == 402:
                    if not abort_event.is_set():
                        logger.critical("FATAL 402: Quota exhausted — aborting batch")
                    abort_event.set()
                    return None

                if resp.status_code != 200:
                    last_error = f"HTTP_{resp.status_code}"
                    logger.debug(
                        f"HTTP {resp.status_code} attempt={attempt} "
                        f"lat={lat:.5f} lon={lon:.5f}"
                    )
                    if attempt < SMARTY_MAX_RETRIES:
                        await asyncio.sleep(SMARTY_RETRY_DELAY * attempt)
                        continue
                    return {
                        **empty_geo_fields("failed"),
                        "_attempts":         attempt,
                        "_response_time_ms": elapsed_ms,
                        "_error":            last_error,
                    }

                data    = resp.json()
                results = data.get("results", [])

                if not results:
                    return {
                        "address":           "",
                        "city":              "",
                        "state":             "",
                        "zipcode":           "",
                        "geocode_distance":  None,
                        "geocode_accuracy":  "",
                        "_attempts":         attempt,
                        "_response_time_ms": elapsed_ms,
                        "_error":            None,
                    }

                # results[0] is closest (sorted by distance meters ascending)
                best       = results[0]
                addr       = best.get("address", {})
                coord_meta = best.get("coordinate", {})

                return {
                    "address":           addr.get("street", ""),
                    "city":              addr.get("city", ""),
                    "state":             addr.get("state_abbreviation", ""),
                    "zipcode":           addr.get("zipcode", ""),
                    "geocode_distance":  best.get("distance"),
                    "geocode_accuracy":  coord_meta.get("accuracy", ""),
                    "_attempts":         attempt,
                    "_response_time_ms": elapsed_ms,
                    "_error":            None,
                }

            except httpx.TimeoutException:
                last_error = "timeout"
                logger.debug(f"Timeout attempt={attempt} lat={lat:.5f} lon={lon:.5f}")
                if attempt < SMARTY_MAX_RETRIES:
                    await asyncio.sleep(SMARTY_RETRY_DELAY * attempt)

            except httpx.RequestError as exc:
                last_error = f"request_error:{type(exc).__name__}"
                logger.debug(
                    f"RequestError attempt={attempt} lat={lat:.5f} lon={lon:.5f}: {exc}"
                )
                if attempt < SMARTY_MAX_RETRIES:
                    await asyncio.sleep(SMARTY_RETRY_DELAY * attempt)

            except (json.JSONDecodeError, KeyError) as exc:
                last_error = f"parse_error:{type(exc).__name__}"
                logger.debug(f"ParseError lat={lat:.5f} lon={lon:.5f}: {exc}")
                return {
                    **empty_geo_fields("failed"),
                    "_attempts":         attempt,
                    "_response_time_ms": round((time.monotonic() - t0) * 1000),
                    "_error":            last_error,
                }

    # Fell through all retries
    return {
        **empty_geo_fields("failed"),
        "_attempts":         SMARTY_MAX_RETRIES,
        "_response_time_ms": round((time.monotonic() - t0) * 1000),
        "_error":            last_error or "max_retries_exceeded",
    }


async def geocode_all(
    records:       list,
    indices:       list,
    concurrency:   int,
    audit_fh,
    logger:        logging.Logger,
    status_counts: dict,
) -> int:
    """
    Geocode records at `indices` concurrently.

    Results are applied directly (in-place) to `records[idx]` as each call
    completes, and the master file is flushed to disk every CHECKPOINT_EVERY
    completions. This means a kill/crash between checkpoints loses at most
    CHECKPOINT_EVERY records.

    Returns the number of records that were processed (including failures).
    Populates `status_counts` dict in place for the caller.
    """
    semaphore   = asyncio.Semaphore(concurrency)
    abort_event = asyncio.Event()
    done_count  = 0
    start_time  = time.time()

    async with httpx.AsyncClient() as client:

        async def process_one(idx: int):
            nonlocal done_count

            if abort_event.is_set():
                return

            rec      = records[idx]
            lat      = rec.get("latitude")
            lon      = rec.get("longitude")
            tower_id = _tower_id_str(rec.get("tower_id"))

            # Base audit entry — filled in below
            audit_entry = {
                "timestamp":        datetime.now(timezone.utc).isoformat(),
                "tower_id":         tower_id,
                "latitude":         lat,
                "longitude":        lon,
                "geocode_status":   None,
                "address":          "",
                "city":             "",
                "state":            "",
                "zipcode":          "",
                "geocode_distance": None,
                "geocode_accuracy": "",
                "response_time_ms": 0,
                "attempts":         0,
                "error":            None,
            }

            if not is_valid_us_coord(lat, lon):
                geo = empty_geo_fields("invalid_coords")
                audit_entry.update({
                    "geocode_status": "invalid_coords",
                    "error":          "out_of_us_bounds",
                })
                logger.debug(f"invalid_coords tower={tower_id} lat={lat} lon={lon}")

            else:
                result = await reverse_geocode_async(
                    client, lat, lon, semaphore, abort_event, logger
                )

                if result is None:
                    # abort_event fired; skip this record entirely
                    return

                # Extract and remove private meta keys
                attempts      = result.pop("_attempts", 1)
                response_time = result.pop("_response_time_ms", 0)
                error         = result.pop("_error", None)

                # Classify result
                if result.get("geocode_status") == "failed":
                    geo = result
                elif result.get("address") == "" and result.get("city") == "":
                    geo = empty_geo_fields("no_result")
                    geo["geocode_distance"] = result.get("geocode_distance")
                    geo["geocode_accuracy"] = result.get("geocode_accuracy", "")
                else:
                    geo = {
                        "address":          result["address"],
                        "city":             result["city"],
                        "state":            result["state"],
                        "zipcode":          result["zipcode"],
                        "geocode_distance": result["geocode_distance"],
                        "geocode_accuracy": result["geocode_accuracy"],
                        "geocode_status":   "success",
                    }

                audit_entry.update({
                    "geocode_status":   geo["geocode_status"],
                    "address":          geo.get("address", ""),
                    "city":             geo.get("city", ""),
                    "state":            geo.get("state", ""),
                    "zipcode":          geo.get("zipcode", ""),
                    "geocode_distance": geo.get("geocode_distance"),
                    "geocode_accuracy": geo.get("geocode_accuracy", ""),
                    "response_time_ms": response_time,
                    "attempts":         attempts,
                    "error":            error,
                })

                if error:
                    logger.debug(
                        f"geocode={geo['geocode_status']} tower={tower_id} "
                        f"attempts={attempts} error={error}"
                    )

            # Apply result in-place immediately (crash-safe)
            records[idx].update(geo)
            s = geo.get("geocode_status", "?")
            status_counts[s] = status_counts.get(s, 0) + 1

            # Write audit entry
            audit_fh.write(json.dumps(audit_entry) + "\n")
            audit_fh.flush()

            done_count += 1
            total      = len(indices)
            elapsed    = time.time() - start_time
            rate       = done_count / elapsed if elapsed > 0 else 0
            eta_sec    = (total - done_count) / rate if rate > 0 else 0
            print(
                f"\r  {done_count}/{total}  "
                f"{rate:.1f}/sec  "
                f"ETA {eta_sec / 60:.1f}min    ",
                end="", flush=True
            )

            # Checkpoint: flush master file + save progress counter
            if done_count % CHECKPOINT_EVERY == 0:
                save_progress(done_count)
                flush_master_file(records, logger)
                logger.debug(f"Checkpoint {done_count:,}: master file saved to disk")

        tasks = [process_one(idx) for idx in indices]
        await asyncio.gather(*tasks)

    print()  # newline after the \r progress line
    return done_count


# ---------------------------------------------------------------------------
# Post-run validation report
# ---------------------------------------------------------------------------

def validate_and_report(logger: logging.Logger, run_geocoded: int):
    """
    Re-reads the master JSONL file and the audit log to produce:
      1. File integrity check + status breakdown
      2. Timing statistics for this run (from audit log)
      3. Top 10 states by geocoded count
      4. Quality flags (empty city/state, distance > 1000 m)
      5. 5 random sample geocoded records

    Outputs to console (INFO), batch log (DEBUG for extras), and REPORT_FILE.
    """
    report_lines: list[str] = []

    def rlog(msg: str = ""):
        if msg.strip():
            logger.info(msg)
        report_lines.append(msg)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    rlog("=" * 64)
    rlog(f"POST-RUN VALIDATION REPORT  [{ts}]")
    rlog("=" * 64)

    # ---- 1. File integrity + status breakdown --------------------------------
    rlog("FILE INTEGRITY & GEOCODE STATUS")
    rlog("-" * 40)

    total_records:   int        = 0
    parse_errors:    int        = 0
    status_counts:   dict       = {}
    state_counts:    dict       = {}
    quality_flags:   list[str]  = []
    success_records: list[dict] = []

    try:
        with open(MASTER_FILE) as fh:
            for line_num, raw in enumerate(fh, 1):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    rec = json.loads(raw)
                    total_records += 1
                    s = rec.get("geocode_status", "missing")
                    status_counts[s] = status_counts.get(s, 0) + 1

                    if s == "success":
                        success_records.append(rec)
                        state = rec.get("state", "")
                        if state:
                            state_counts[state] = state_counts.get(state, 0) + 1

                        if not rec.get("city") or not rec.get("state"):
                            tid = _tower_id_str(rec.get("tower_id"))
                            quality_flags.append(
                                f"  empty city/state: line {line_num}  tower_id={tid}"
                            )

                        dist = rec.get("geocode_distance")
                        if dist is not None:
                            try:
                                if float(dist) > 1000:
                                    tid = _tower_id_str(rec.get("tower_id"))
                                    quality_flags.append(
                                        f"  distance {float(dist):.0f}m > 1000m: "
                                        f"line {line_num}  tower_id={tid}  "
                                        f"{rec.get('city','')},{rec.get('state','')}"
                                    )
                            except (TypeError, ValueError):
                                pass

                except json.JSONDecodeError:
                    parse_errors += 1
                    quality_flags.append(f"  JSON parse error at line {line_num}")

    except FileNotFoundError:
        rlog(f"  ERROR: {MASTER_FILE} not found!")
        return

    rlog(f"  Total records      : {total_records:,}")
    rlog(f"  JSON parse errors  : {parse_errors}")
    rlog("")
    for s, c in sorted(status_counts.items()):
        pct = c / total_records * 100 if total_records else 0
        rlog(f"  {s:<25} {c:>8,}  ({pct:.2f}%)")
    rlog("")

    # ---- 2. Timing statistics from audit log ---------------------------------
    rlog("THIS RUN — TIMING STATISTICS")
    rlog("-" * 40)

    response_times: list[float] = []
    attempt_counts: list[int]   = []

    if AUDIT_LOG_FILE.exists():
        all_entries = []
        with open(AUDIT_LOG_FILE) as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    all_entries.append(json.loads(raw))
                except json.JSONDecodeError:
                    pass

        # The last `run_geocoded` lines correspond to this run
        session_entries = all_entries[-run_geocoded:] if run_geocoded > 0 else all_entries

        for entry in session_entries:
            rt = entry.get("response_time_ms")
            if rt is not None:
                response_times.append(float(rt))
            att = entry.get("attempts")
            if att is not None:
                attempt_counts.append(int(att))

        if response_times:
            sorted_rt = sorted(response_times)
            p95_idx   = max(0, int(len(sorted_rt) * 0.95) - 1)
            retried   = sum(1 for a in attempt_counts if a > 1)
            rlog(f"  Records this run   : {len(response_times):,}")
            rlog(f"  Min                : {min(response_times):,.0f} ms")
            rlog(f"  Mean               : {statistics.mean(response_times):,.0f} ms")
            rlog(f"  Median             : {statistics.median(response_times):,.0f} ms")
            rlog(f"  p95                : {sorted_rt[p95_idx]:,.0f} ms")
            rlog(f"  Max                : {max(response_times):,.0f} ms")
            rlog(f"  Retried (>1 attempt): {retried:,}")
        else:
            rlog("  No API timing data for this run (invalid_coords only?).")
    else:
        rlog(f"  Audit log not found: {AUDIT_LOG_FILE}")
    rlog("")

    # ---- 3. Geographic distribution ------------------------------------------
    rlog("TOP 10 STATES (by geocoded count)")
    rlog("-" * 40)
    for state, count in sorted(state_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        rlog(f"  {state:<10} {count:>8,}")
    rlog("")

    # ---- 4. Quality flags ----------------------------------------------------
    rlog("QUALITY FLAGS")
    rlog("-" * 40)
    if quality_flags:
        for flag in quality_flags[:25]:
            rlog(flag)
        if len(quality_flags) > 25:
            rlog(f"  ... and {len(quality_flags) - 25} more (see {BATCH_LOG_FILE.name})")
            for flag in quality_flags[25:]:
                logger.debug(f"quality_flag: {flag}")
    else:
        rlog("  None — all checks passed.")
    rlog("")

    # ---- 5. Sample geocoded records ------------------------------------------
    rlog("SAMPLE GEOCODED RECORDS (5 random from all-time successes)")
    rlog("-" * 40)
    sample = random.sample(success_records, min(5, len(success_records)))
    for rec in sample:
        tid = _tower_id_str(rec.get("tower_id"))
        rlog(
            f"  {tid:<25}  "
            f"{rec.get('address','')} — "
            f"{rec.get('city','')}, {rec.get('state','')} {rec.get('zipcode','')}"
        )
        dist = rec.get("geocode_distance")
        dist_str = f"{float(dist):.0f}m" if dist is not None else "?"
        rlog(
            f"    lat={rec.get('latitude', 0):.4f} "
            f"lon={rec.get('longitude', 0):.4f}  "
            f"dist={dist_str}  "
            f"accuracy={rec.get('geocode_accuracy','?')}"
        )
    rlog("")

    # Write report file
    LOGS_DIR.mkdir(exist_ok=True)
    REPORT_FILE.write_text("\n".join(report_lines), encoding="utf-8")
    logger.info(f"Report saved → {REPORT_FILE}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logger = setup_logging()

    parser = argparse.ArgumentParser(
        description="Reverse-geocode a tower JSONL file via Smarty API"
    )
    parser.add_argument(
        "--input", default=None,
        help="Input JSONL file (default: downloads/towers_all_classified.jsonl)"
    )
    parser.add_argument(
        "--limit", type=int, default=DEFAULT_LIMIT,
        help=f"Max pending urban records to geocode (default {DEFAULT_LIMIT:,})"
    )
    parser.add_argument(
        "--concurrency", type=int, default=DEFAULT_CONCURRENCY,
        help=f"Parallel API requests (default {DEFAULT_CONCURRENCY})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Rename field + validate counts without calling API"
    )
    args = parser.parse_args()

    # Resolve file paths based on --input
    global MASTER_FILE, PROGRESS_FILE, TEMP_MASTER
    if args.input:
        MASTER_FILE = Path(args.input).resolve()
    else:
        MASTER_FILE = _DEFAULT_MASTER
    PROGRESS_FILE = MASTER_FILE.parent / f".{MASTER_FILE.stem}_progress.json"
    TEMP_MASTER   = MASTER_FILE.parent / f".{MASTER_FILE.stem}.tmp"

    logger.info("=" * 64)
    logger.info("CellMapper Batch Reverse Geocoder")
    logger.info("=" * 64)
    if args.dry_run:
        logger.info("*** DRY-RUN: no API calls ***")
    logger.info(f"Source      : {MASTER_FILE.name}")
    logger.info(f"Limit       : {args.limit:,} records")
    logger.info(f"Concurrency : {args.concurrency} parallel requests")
    logger.info(f"Checkpoint  : flush to disk every {CHECKPOINT_EVERY:,} completions")
    logger.info(f"Batch log   : {BATCH_LOG_FILE}")
    logger.info(f"Audit log   : {AUDIT_LOG_FILE}")

    if not args.dry_run:
        validate_credentials(logger)

    if not MASTER_FILE.exists():
        logger.critical(f"Master file not found: {MASTER_FILE}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Phase 1: Load all records, rename skipped -> rural
    # ------------------------------------------------------------------
    load_progress(logger)

    logger.info("Loading towers_all_classified.jsonl ...")
    records = []
    with open(MASTER_FILE, "r") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            rec = json.loads(raw)

            # Rename field (idempotent)
            if "skipped" in rec and "rural" not in rec:
                rec["rural"] = rec.pop("skipped")

            # Backfill any missing geo fields on older records
            rec.setdefault("address",          "")
            rec.setdefault("city",             "")
            rec.setdefault("state",            "")
            rec.setdefault("zipcode",          "")
            rec.setdefault("geocode_distance", None)
            rec.setdefault("geocode_accuracy", "")
            rec.setdefault("geocode_status",   "pending")

            records.append(rec)

    total = len(records)
    logger.info(f"Loaded {total:,} records")

    # ------------------------------------------------------------------
    # Phase 2: Identify records to geocode this run
    # ------------------------------------------------------------------
    pending_urban = [
        i for i, r in enumerate(records)
        if not r.get("rural", True) and r.get("geocode_status") == "pending"
    ][:args.limit]

    already_done = sum(
        1 for r in records
        if not r.get("rural", True) and r.get("geocode_status") != "pending"
    )
    urban_total = sum(1 for r in records if not r.get("rural", True))

    logger.info(f"Urban records total   : {urban_total:,}")
    logger.info(f"Already geocoded      : {already_done:,}")
    logger.info(f"Pending this run      : {len(pending_urban):,}")

    if not pending_urban:
        logger.info("Nothing to geocode. Exiting.")
        return

    # ------------------------------------------------------------------
    # Phase 3: Geocode (or dry-run)
    # ------------------------------------------------------------------
    run_geocoded = 0

    if args.dry_run:
        logger.info("DRY-RUN: would geocode the records listed above.")
        logger.info("Re-run without --dry-run to proceed.")
    else:
        logger.info(
            f"Geocoding {len(pending_urban):,} records "
            f"({args.concurrency} concurrent, checkpoint every {CHECKPOINT_EVERY:,}) ..."
        )
        LOGS_DIR.mkdir(exist_ok=True)
        t0 = time.time()

        status_counts: dict = {}
        interrupted = False

        # Register SIGTERM handler so `kill <pid>` triggers a clean flush
        # instead of losing all in-memory results.
        def _sigterm_handler(signum, frame):
            logger.warning("SIGTERM received — stopping and flushing progress ...")
            sys.exit(0)  # raises SystemExit, caught by except below

        signal.signal(signal.SIGTERM, _sigterm_handler)

        try:
            with open(AUDIT_LOG_FILE, "a", encoding="utf-8") as audit_fh:
                run_geocoded = asyncio.run(
                    geocode_all(
                        records, pending_urban, args.concurrency,
                        audit_fh, logger, status_counts,
                    )
                )
        except (KeyboardInterrupt, SystemExit):
            interrupted = True
            logger.warning("Run interrupted — flushing geocoded records to disk ...")
            # Count how many records were actually applied in-place before interruption
            run_geocoded = sum(
                1 for r in records
                if not r.get("rural", True) and r.get("geocode_status") != "pending"
            ) - already_done
        finally:
            # Always flush — this is the guarantee. Whether the run completed,
            # was killed, or crashed, whatever results are in `records` get saved.
            flush_master_file(records, logger)
            save_progress(already_done + run_geocoded)
            if interrupted:
                logger.info(
                    f"Emergency save complete: {run_geocoded:,} records saved this run "
                    f"(master file is consistent and resumable)"
                )

        elapsed = time.time() - t0

        if not interrupted:
            logger.info("-" * 64)
            logger.info("GEOCODING RESULTS")
            logger.info("-" * 64)
            for status, count in sorted(status_counts.items()):
                logger.info(f"  {status:<20} {count:>7,}")
            rate = run_geocoded / elapsed if elapsed > 0 else 0
            logger.info(f"  {'Total':<20} {run_geocoded:>7,}")
            logger.info(f"  Elapsed: {elapsed:.1f}s  ({rate:.1f} records/sec)")
        else:
            logger.info(
                f"Partial run: {run_geocoded:,} records in {elapsed:.1f}s — "
                f"re-run to continue from where this left off"
            )

    # ------------------------------------------------------------------
    # Phase 4: Post-run validation report (skipped if nothing was geocoded)
    # ------------------------------------------------------------------
    if not args.dry_run and run_geocoded > 0:
        logger.info("")
        validate_and_report(logger, run_geocoded)

    logger.info("")
    logger.info("When ready to split into urban/rural files, run:")
    logger.info("  python scripts/split_towers.py")


if __name__ == "__main__":
    main()
