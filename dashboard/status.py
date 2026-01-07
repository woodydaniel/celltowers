from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


ROOT_DIR = Path(__file__).resolve().parent.parent


def _resolve_dir(env_key: str, default: Path) -> Path:
    raw = os.environ.get(env_key, "").strip()
    if not raw:
        return default
    p = Path(raw).expanduser()
    return p if p.is_absolute() else (ROOT_DIR / p)


DATA_DIR = _resolve_dir("DASHBOARD_DATA_DIR", ROOT_DIR / "data")
LOGS_DIR = _resolve_dir("DASHBOARD_LOGS_DIR", ROOT_DIR / "logs")


def _utc_iso(ts: Optional[float] = None) -> str:
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _safe_read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _tail_last_jsonl_obj(path: Path, max_bytes: int = 64 * 1024) -> Optional[dict]:
    """
    Read the last valid JSON object from a JSONL file efficiently.
    """
    try:
        size = path.stat().st_size
        start = max(0, size - max_bytes)
        with open(path, "rb") as f:
            f.seek(start)
            data = f.read()
        text = data.decode("utf-8", errors="ignore")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        for ln in reversed(lines):
            try:
                return json.loads(ln)
            except Exception:
                continue
        return None
    except Exception:
        return None


def _human_eta(hours: Optional[float]) -> str:
    if hours is None:
        return "—"
    if hours < 0:
        return "—"
    if hours < 1:
        return f"~{int(hours * 60)}m"
    if hours < 48:
        return f"~{hours:.1f}h"
    days = hours / 24.0
    return f"~{days:.1f}d"


@dataclass
class ProgressAgg:
    total_tiles: int = 0
    completed_tiles: int = 0
    pending_tiles: int = 0
    deferred_tiles: int = 0
    towers_collected: int = 0

    @property
    def completion_percent(self) -> float:
        return (self.completed_tiles / self.total_tiles * 100.0) if self.total_tiles else 0.0


def _find_progress_files() -> list[Path]:
    if not DATA_DIR.exists():
        return []
    # Includes progress.json and carrier-specific progress_* files.
    return sorted(DATA_DIR.glob("progress*.json"))


def _find_worker_metrics_files() -> list[Path]:
    if not LOGS_DIR.exists():
        return []
    return sorted(LOGS_DIR.glob("worker_metrics_*.jsonl"))


def _is_recent(path: Path, active_window_sec: int) -> bool:
    try:
        return (time.time() - path.stat().st_mtime) <= active_window_sec
    except Exception:
        return False


def build_status(
    *,
    include_all: bool = False,
    include_workers: bool = False,
    include_debug: bool = False,
) -> dict:
    """
    Build a single JSON payload for the dashboard frontend.

    include_all:
      - False: show only "active" (recently modified) files (recommended for client view)
      - True: include all progress/worker files found (debug)
    """
    active_window_sec = int(os.environ.get("DASHBOARD_ACTIVE_WINDOW_SEC", "21600"))  # 6h

    all_progress_files = _find_progress_files()
    all_metrics_files = _find_worker_metrics_files()

    progress_files = all_progress_files
    metrics_files = all_metrics_files

    if not include_all and active_window_sec > 0:
        progress_files = [p for p in progress_files if _is_recent(p, active_window_sec)]
        metrics_files = [p for p in metrics_files if _is_recent(p, active_window_sec)]

    # If nothing is recent, fall back to the newest file(s) so the UI isn't blank.
    if not include_all and not progress_files and all_progress_files:
        progress_files = [max(all_progress_files, key=lambda p: p.stat().st_mtime)]
    if not include_all and not metrics_files and all_metrics_files:
        metrics_files = [max(all_metrics_files, key=lambda p: p.stat().st_mtime)]

    progress = ProgressAgg()
    progress_sources: list[dict] = []

    for pf in progress_files:
        data = _safe_read_json(pf) or {}
        total = int(data.get("total_tiles", 0) or 0)
        completed = int(data.get("completed_tiles", 0) or 0)
        deferred = int(data.get("deferred_tiles", 0) or 0)

        # Compute "towers collected" from per-tile tower_count.
        # This corresponds to the number of tower records written per tile. For client UX we
        # intentionally keep the label simple and avoid internal tech distinctions.
        tiles = data.get("tiles", {}) or {}
        towers = 0
        if isinstance(tiles, dict):
            for _tid, td in tiles.items():
                if isinstance(td, dict):
                    towers += int(td.get("tower_count", 0) or 0)

        progress.total_tiles += total
        progress.completed_tiles += completed
        progress.deferred_tiles += deferred
        progress.towers_collected += towers

        progress_sources.append(
            {
                "file": str(pf.relative_to(ROOT_DIR)),
                "mtime": _utc_iso(pf.stat().st_mtime),
                "total_tiles": total,
                "completed_tiles": completed,
                "deferred_tiles": deferred,
                "towers_collected": towers,
            }
        )

    progress.pending_tiles = max(0, progress.total_tiles - progress.completed_tiles)

    workers: list[dict] = []
    total_velocity = 0.0
    newest_worker_ts: Optional[str] = None

    for mf in metrics_files:
        snap = _tail_last_jsonl_obj(mf)
        if not snap:
            continue

        # snapshot timestamp is iso; keep the "newest" one lexicographically (iso sorts).
        ts_iso = str(snap.get("timestamp", "") or "")
        if ts_iso and (newest_worker_ts is None or ts_iso > newest_worker_ts):
            newest_worker_ts = ts_iso

        worker_id = str(snap.get("worker", mf.stem.replace("worker_metrics_", "")) or "")
        velocity = snap.get("velocity_tiles_per_hour", None)
        api_sr = snap.get("api_success_rate", None)
        is_stalled = bool(snap.get("is_stalled", False))
        time_since = snap.get("time_since_last_success_sec", None)

        # Infer "last_success_ts" as now - time_since (best-effort).
        last_success_ts = None
        if isinstance(time_since, (int, float)):
            last_success_ts = int(time.time() - float(time_since))

        workers.append(
            {
                "worker_id": worker_id,
                "snapshot_ts": ts_iso or None,
                "file": str(mf.relative_to(ROOT_DIR)),
                "velocity_tiles_per_hour": float(velocity) if isinstance(velocity, (int, float)) else None,
                "api_success_rate": float(api_sr) if isinstance(api_sr, (int, float)) else None,
                "is_stalled": is_stalled,
                "time_since_last_success_sec": int(time_since) if isinstance(time_since, (int, float)) else None,
                "last_success_ts": last_success_ts,
            }
        )

        if isinstance(velocity, (int, float)) and velocity > 0:
            total_velocity += float(velocity)

    # ETA based on tiles remaining / aggregate tiles/hr velocity.
    eta_hours: Optional[float] = None
    projected_finish_at: Optional[str] = None
    if progress.pending_tiles > 0 and total_velocity > 0:
        eta_hours = progress.pending_tiles / total_velocity
        projected_finish_at = _utc_iso(time.time() + eta_hours * 3600.0)
    elif progress.pending_tiles == 0 and progress.total_tiles > 0:
        eta_hours = 0.0
        projected_finish_at = _utc_iso(time.time())

    # "Live" detection: any progress/metrics file updated recently.
    live = False
    if include_all:
        live = bool(all_progress_files or all_metrics_files)
    else:
        if active_window_sec <= 0:
            # Treat presence of any file as live if window disabled.
            live = bool(all_progress_files or all_metrics_files)
        else:
            live = any(_is_recent(p, active_window_sec) for p in all_progress_files) or any(
                _is_recent(p, active_window_sec) for p in all_metrics_files
            )

    # Health summary (client-friendly)
    total_workers = len(workers)
    stalled_workers = sum(1 for w in workers if w.get("is_stalled"))
    degraded_workers = sum(
        1
        for w in workers
        if (not w.get("is_stalled"))
        and isinstance(w.get("api_success_rate"), (int, float))
        and float(w["api_success_rate"]) < 80.0
    )
    healthy_workers = max(0, total_workers - stalled_workers - degraded_workers)

    if total_workers == 0:
        health_state = "idle"
    elif stalled_workers > 0:
        health_state = "bad"
    elif degraded_workers > 0:
        health_state = "warn"
    else:
        health_state = "good"

    # Make a stable-ish run id that looks clean to a client.
    # If multiple progress sources exist (parallel workers), show "multi".
    if progress_sources:
        run_id = "multi" if len(progress_sources) > 1 else Path(progress_sources[0]["file"]).stem
    else:
        run_id = "—"

    return {
        "ok": True,
        "active": live,
        "run_id": run_id,
        "generated_at": _utc_iso(),
        "server_time": _utc_iso(),
        "progress": {
            "total_tiles": progress.total_tiles,
            "completed_tiles": progress.completed_tiles,
            "pending_tiles": progress.pending_tiles,
            "deferred_tiles": progress.deferred_tiles,
            "completion_percent": round(progress.completion_percent, 3),
            "towers_collected": progress.towers_collected,
        }
        if progress.total_tiles > 0
        else None,
        "velocity": {
            "tiles_per_hour": round(total_velocity, 3) if total_velocity > 0 else None,
        },
        "health": {
            "state": health_state,  # idle|good|warn|bad
            "workers_total": total_workers,
            "workers_healthy": healthy_workers,
            "workers_degraded": degraded_workers,
            "workers_stalled": stalled_workers,
            "newest_worker_snapshot_ts": newest_worker_ts,
        },
        "eta": {
            "eta_hours": round(eta_hours, 3) if eta_hours is not None else None,
            "eta_human": _human_eta(eta_hours),
            "projected_finish_at": projected_finish_at,
        },
        "workers": (
            sorted(workers, key=lambda w: (w.get("is_stalled", False), w.get("worker_id", "")))
            if include_workers
            else []
        ),
        "debug": (
            {
                "active_window_sec": active_window_sec,
                "data_dir": str(DATA_DIR),
                "logs_dir": str(LOGS_DIR),
                "progress_files": progress_sources,
                "worker_metrics_files": [str(p) for p in metrics_files],
                "include_all": include_all,
                "include_workers": include_workers,
            }
            if include_debug
            else None
        ),
    }


