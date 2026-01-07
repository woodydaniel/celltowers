from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _human_eta(hours: float | None) -> str:
    if hours is None or hours < 0:
        return "—"
    if hours < 1:
        return f"~{int(hours * 60)}m"
    if hours < 48:
        return f"~{hours:.1f}h"
    return f"~{hours / 24.0:.1f}d"


def _normalize_upstream(data: dict) -> dict:
    """
    Normalize multiple possible upstream schemas into the dashboard schema expected by index.html:
      - ok, active, run_id, generated_at, server_time
      - progress: { completion_percent, towers_collected, total_tiles, completed_tiles, pending_tiles }
      - eta: { eta_hours, eta_human, projected_finish_at }
      - health: { state }  (kept simple for UI mapping)
    """
    # Already in our native schema?
    if "generated_at" in data and "progress" in data and "eta" in data:
        # Ensure required keys exist / are safe.
        data.pop("debug", None)
        data["workers"] = []
        return data

    # Example alternate schema (your Hetzner API):
    # {
    #   "timestamp": "...Z",
    #   "progress": {"total_completed_tiles": 123, "total_tiles": 456, "overall_progress_pct": 27.0},
    #   "towers": {"total": 215000},
    #   "estimate": {"remaining_hours": 100, "remaining_days": 4.2},
    #   "status": "running"
    # }
    ts = str(data.get("timestamp") or data.get("generated_at") or "") or None
    status = str(data.get("status") or "").lower().strip()
    active = status in ("running", "live", "active")

    prog = data.get("progress") if isinstance(data.get("progress"), dict) else {}
    total_tiles = prog.get("total_tiles", None)
    completed_tiles = prog.get("total_completed_tiles", None)
    pct = prog.get("overall_progress_pct", None)
    if pct is None and isinstance(total_tiles, (int, float)) and total_tiles:
        if isinstance(completed_tiles, (int, float)):
            pct = float(completed_tiles) / float(total_tiles) * 100.0

    towers = data.get("towers") if isinstance(data.get("towers"), dict) else {}
    towers_total = towers.get("total", None)

    est = data.get("estimate") if isinstance(data.get("estimate"), dict) else {}
    remaining_hours = est.get("remaining_hours", None)

    now_iso = _utc_now_iso()
    projected_finish_at = None
    if isinstance(remaining_hours, (int, float)) and remaining_hours >= 0:
        projected_finish_at = datetime.now(timezone.utc).timestamp() + float(remaining_hours) * 3600.0
        projected_finish_at = datetime.fromtimestamp(projected_finish_at, tz=timezone.utc).isoformat()

    # If upstream gives percent but not totals, keep totals null.
    pending_tiles = None
    if isinstance(total_tiles, (int, float)) and isinstance(completed_tiles, (int, float)):
        pending_tiles = max(0, int(total_tiles) - int(completed_tiles))

    health_state = "good" if active else "idle"

    return {
        "ok": True,
        "active": bool(active),
        "run_id": "live",
        "generated_at": ts or now_iso,
        "server_time": now_iso,
        "progress": {
            "total_tiles": int(total_tiles) if isinstance(total_tiles, (int, float)) else None,
            "completed_tiles": int(completed_tiles) if isinstance(completed_tiles, (int, float)) else None,
            "pending_tiles": pending_tiles,
            "deferred_tiles": None,
            "completion_percent": float(pct) if isinstance(pct, (int, float)) else None,
            "towers_collected": int(towers_total) if isinstance(towers_total, (int, float)) else 0,
        },
        "velocity": {"tiles_per_hour": None},
        "health": {"state": health_state},
        "eta": {
            "eta_hours": float(remaining_hours) if isinstance(remaining_hours, (int, float)) else None,
            "eta_human": _human_eta(float(remaining_hours)) if isinstance(remaining_hours, (int, float)) else "—",
            "projected_finish_at": projected_finish_at,
        },
        "workers": [],
        "debug": None,
    }


class handler(BaseHTTPRequestHandler):
    """
    Vercel Python Function entrypoint.

    This uses the supported BaseHTTPRequestHandler pattern. The frontend calls:
      GET /api/status

    Env vars (set in Vercel):
      - SCRAPER_STATUS_URL or CELLMAPPER_API_URL
      - SCRAPER_STATUS_BEARER_TOKEN or CELLMAPPER_API_TOKEN
      - ALLOW_INSECURE_STATUS_URL ("true" to allow http://)
    """

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        # Support both naming conventions.
        url = (os.environ.get("SCRAPER_STATUS_URL") or os.environ.get("CELLMAPPER_API_URL", "")).strip()
        token = (os.environ.get("SCRAPER_STATUS_BEARER_TOKEN") or os.environ.get("CELLMAPPER_API_TOKEN", "")).strip()
        allow_insecure = os.environ.get("ALLOW_INSECURE_STATUS_URL", "false").lower() == "true"

        if not url:
            self._send_json(
                500,
                {"ok": False, "error": "missing_env", "env": "SCRAPER_STATUS_URL|CELLMAPPER_API_URL"},
            )
            return
        if not token:
            self._send_json(
                500,
                {"ok": False, "error": "missing_env", "env": "SCRAPER_STATUS_BEARER_TOKEN|CELLMAPPER_API_TOKEN"},
            )
            return

        parsed = urlparse(url)
        if parsed.scheme not in ("https", "http") or not parsed.netloc:
            self._send_json(400, {"ok": False, "error": "invalid_url"})
            return
        if parsed.scheme == "http" and not allow_insecure:
            self._send_json(400, {"ok": False, "error": "insecure_url_not_allowed"})
            return

        req = urllib.request.Request(url, method="GET")
        req.add_header("Accept", "application/json")
        req.add_header("Authorization", f"Bearer {token}")

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                data = json.loads(raw)
        except Exception:
            self._send_json(502, {"ok": False, "error": "upstream_unreachable"})
            return

        if not isinstance(data, dict):
            self._send_json(502, {"ok": False, "error": "invalid_upstream_payload"})
            return

        normalized = _normalize_upstream(data)
        normalized["workers"] = []
        normalized.pop("debug", None)
        self._send_json(200, normalized)


