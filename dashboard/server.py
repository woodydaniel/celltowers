#!/usr/bin/env python3
"""
CellMapper Dashboard Status API

A lightweight read-only API that exposes scraper progress and metrics.
Runs on Hetzner and is consumed by the Vercel dashboard.

Endpoints:
    GET /api/status - Returns current scraper status (JSON)
    GET /health     - Health check for monitoring

Security:
    Requires Authorization: Bearer <token> header (configurable via API_TOKEN env var)
    If API_TOKEN is not set, auth is disabled (not recommended for production)
"""

import json
import os
import glob
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

# Configuration
PORT = int(os.environ.get("DASHBOARD_PORT", "8088"))
API_TOKEN = os.environ.get("DASHBOARD_BEARER_TOKEN", "").strip()
DATA_DIR = os.environ.get("DASHBOARD_DATA_DIR", "/app/data")
LOGS_DIR = os.environ.get("DASHBOARD_LOGS_DIR", "/app/logs")


def get_progress_data() -> dict:
    """Read all progress files and aggregate status."""
    progress_files = glob.glob(os.path.join(DATA_DIR, "progress_*.json"))
    
    workers = []
    total_completed = 0
    total_tiles = 0
    
    for pf in progress_files:
        try:
            with open(pf, "r") as f:
                data = json.load(f)
            
            # Extract worker name from filename (e.g., progress_tmobile_west.json -> tmobile_west)
            worker_name = os.path.basename(pf).replace("progress_", "").replace(".json", "")
            
            completed = data.get("completed_tiles", 0)
            total = data.get("total_tiles", 0)
            deferred = data.get("deferred_tiles", 0)
            
            pct = round(completed / total * 100, 1) if total > 0 else 0
            
            workers.append({
                "name": worker_name,
                "completed_tiles": completed,
                "total_tiles": total,
                "deferred_tiles": deferred,
                "progress_pct": pct,
            })
            
            total_completed += completed
            total_tiles += total
            
        except Exception as e:
            print(f"Error reading {pf}: {e}")
    
    overall_pct = round(total_completed / total_tiles * 100, 1) if total_tiles > 0 else 0
    
    return {
        "workers": workers,
        "total_completed_tiles": total_completed,
        "total_tiles": total_tiles,
        "overall_progress_pct": overall_pct,
    }


def get_tower_counts() -> dict:
    """Count towers in each JSONL file."""
    tower_files = glob.glob(os.path.join(DATA_DIR, "towers", "*.jsonl"))
    
    carriers = {}
    total_towers = 0
    
    for tf in tower_files:
        try:
            # Count lines (each line = 1 tower)
            with open(tf, "r") as f:
                count = sum(1 for _ in f)
            
            # Extract carrier name from filename (e.g., towers_tmobile_west.jsonl -> tmobile_west)
            name = os.path.basename(tf).replace("towers_", "").replace(".jsonl", "")
            carriers[name] = count
            total_towers += count
            
        except Exception as e:
            print(f"Error reading {tf}: {e}")
    
    return {
        "by_worker": carriers,
        "total": total_towers,
    }


def estimate_completion(progress: dict) -> dict:
    """Estimate time to completion based on current progress."""
    # Simple estimate: assume constant velocity
    # In reality, we'd read velocity from metrics, but this is a reasonable approximation
    
    remaining_tiles = progress["total_tiles"] - progress["total_completed_tiles"]
    
    # Assume ~50 tiles/hour per worker (conservative estimate based on observed data)
    # With 6 workers, that's ~300 tiles/hour combined
    estimated_velocity = 300  # tiles per hour
    
    if remaining_tiles <= 0:
        return {"remaining_hours": 0, "estimated_finish": None}
    
    remaining_hours = round(remaining_tiles / estimated_velocity, 1)
    
    return {
        "remaining_tiles": remaining_tiles,
        "estimated_velocity_tiles_per_hour": estimated_velocity,
        "remaining_hours": remaining_hours,
        "remaining_days": round(remaining_hours / 24, 1),
    }


def get_status() -> dict:
    """Aggregate all status data."""
    progress = get_progress_data()
    towers = get_tower_counts()
    estimate = estimate_completion(progress)
    
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "progress": progress,
        "towers": towers,
        "estimate": estimate,
        "status": "running" if progress["overall_progress_pct"] < 100 else "complete",
    }


class StatusHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the status API."""
    
    def _send_json(self, data: dict, status: int = 200):
        """Send JSON response."""
        body = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()
        self.wfile.write(body)
    
    def _check_auth(self) -> bool:
        """Check Bearer token authorization."""
        if not API_TOKEN:
            return True  # Auth disabled
        
        auth_header = self.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            return token == API_TOKEN
        return False
    
    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()
    
    def do_GET(self):
        """Handle GET requests."""
        path = urlparse(self.path).path
        
        # Health check (no auth required)
        if path == "/health":
            self._send_json({"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"})
            return
        
        # Status endpoint (auth required)
        if path == "/api/status":
            if not self._check_auth():
                self._send_json({"error": "Unauthorized"}, 401)
                return
            
            try:
                status = get_status()
                self._send_json(status)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        
        # 404 for everything else
        self._send_json({"error": "Not found"}, 404)
    
    def log_message(self, format, *args):
        """Custom logging format."""
        print(f"[{datetime.utcnow().isoformat()}] {args[0]}")


def main():
    """Start the dashboard API server."""
    print(f"Starting CellMapper Dashboard API on port {PORT}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Auth: {'enabled' if API_TOKEN else 'DISABLED (set API_TOKEN to enable)'}")
    
    server = HTTPServer(("0.0.0.0", PORT), StatusHandler)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
