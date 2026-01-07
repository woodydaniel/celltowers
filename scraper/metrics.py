"""
Worker Metrics Tracker

Provides time-windowed metrics for monitoring scraper health and performance.
Enables early detection of stalls, CAPTCHA storms, and proxy failures.

Usage:
    metrics = WorkerMetrics(worker_id="tmobile_west")
    metrics.record_tile_completed("tile_0001_0002", tower_count=15, data_bytes=1234)
    metrics.record_error("407", "Proxy Authentication Required")
    
    # Periodic snapshot (every 5 minutes)
    snapshot = metrics.get_periodic_snapshot()
    # Write to JSONL file for monitoring
"""

from __future__ import annotations

import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class TimestampedEvent:
    """A timestamped event for time-windowed tracking."""
    timestamp: float
    value: float = 1.0


class RollingCounter:
    """
    Counter that tracks events within a sliding time window.
    
    Maintains counts for multiple windows (5m, 15m, 60m) efficiently.
    """
    
    def __init__(self, max_window_sec: float = 3600):
        self._events: deque[TimestampedEvent] = deque()
        self._max_window = max_window_sec
        self._total = 0.0
    
    def record(self, value: float = 1.0) -> None:
        """Record an event with the current timestamp."""
        now = time.time()
        self._events.append(TimestampedEvent(timestamp=now, value=value))
        self._total += value
        self._prune_old(now)
    
    def _prune_old(self, now: float) -> None:
        """Remove events older than max window."""
        cutoff = now - self._max_window
        while self._events and self._events[0].timestamp < cutoff:
            self._events.popleft()
    
    def count_in_window(self, window_sec: float) -> float:
        """Count events within the specified time window."""
        now = time.time()
        self._prune_old(now)
        cutoff = now - window_sec
        return sum(e.value for e in self._events if e.timestamp >= cutoff)
    
    def count_last_5m(self) -> float:
        return self.count_in_window(300)
    
    def count_last_15m(self) -> float:
        return self.count_in_window(900)
    
    def count_last_60m(self) -> float:
        return self.count_in_window(3600)
    
    @property
    def total(self) -> float:
        return self._total


@dataclass
class WorkerMetrics:
    """
    Comprehensive metrics tracker for a single scraper worker.
    
    Tracks velocity, health indicators, and error patterns with
    time-windowed counters for trend analysis.
    """
    
    worker_id: str
    logs_dir: Optional[Path] = None
    
    # Velocity counters
    tiles_completed: RollingCounter = field(default_factory=RollingCounter)
    towers_found: RollingCounter = field(default_factory=RollingCounter)
    data_written_bytes: RollingCounter = field(default_factory=RollingCounter)
    
    # Health counters
    captcha_hits: RollingCounter = field(default_factory=RollingCounter)
    session_successes: RollingCounter = field(default_factory=RollingCounter)
    session_failures: RollingCounter = field(default_factory=RollingCounter)
    
    # API request-level metrics (lets us tune delays/proxies safely)
    api_requests: RollingCounter = field(default_factory=RollingCounter)
    api_failures: RollingCounter = field(default_factory=RollingCounter)
    api_latency_ms: RollingCounter = field(default_factory=RollingCounter)  # sum of ms
    
    # Error tracking by type
    error_counts: dict[str, RollingCounter] = field(default_factory=dict)
    
    # Timing
    start_time: float = field(default_factory=time.time)
    last_success_time: float = field(default_factory=time.time)
    last_snapshot_time: float = field(default_factory=time.time)
    
    # Proxy stats (updated externally)
    active_proxies: int = 0
    bad_proxies: int = 0
    cooling_proxies: int = 0
    
    def __post_init__(self):
        if self.logs_dir is None:
            self.logs_dir = Path("logs")
        self.logs_dir = Path(self.logs_dir)
    
    def record_tile_completed(
        self,
        tile_id: str,
        tower_count: int = 0,
        data_bytes: int = 0,
    ) -> None:
        """Record a successfully completed tile."""
        self.tiles_completed.record(1)
        self.towers_found.record(tower_count)
        self.data_written_bytes.record(data_bytes)
        self.last_success_time = time.time()
    
    def record_error(self, error_type: str, details: str = "") -> None:
        """Record an error by type (e.g., '407', '522', 'timeout', 'captcha')."""
        if error_type not in self.error_counts:
            self.error_counts[error_type] = RollingCounter()
        self.error_counts[error_type].record(1)
        
        if error_type.lower() == "captcha":
            self.captcha_hits.record(1)
    
    def record_session_result(self, success: bool, proxy_id: str = "") -> None:
        """Record a session/request result (success or failure)."""
        if success:
            self.session_successes.record(1)
            self.last_success_time = time.time()
        else:
            self.session_failures.record(1)
    
    def record_api_result(
        self,
        success: bool,
        request_time_sec: float = 0.0,
        error_code: str = "",
    ) -> None:
        """Record a single API call outcome and latency (for tuning)."""
        self.api_requests.record(1)
        if request_time_sec and request_time_sec > 0:
            self.api_latency_ms.record(request_time_sec * 1000.0)
        if not success:
            self.api_failures.record(1)
            if error_code:
                self.record_error(error_code, error_code)
    
    def update_proxy_stats(
        self,
        active: int = 0,
        bad: int = 0,
        cooling: int = 0,
    ) -> None:
        """Update current proxy health stats."""
        self.active_proxies = active
        self.bad_proxies = bad
        self.cooling_proxies = cooling
    
    def get_velocity_tiles_per_hour(self) -> float:
        """Calculate current tile completion velocity (tiles/hour)."""
        tiles_60m = self.tiles_completed.count_last_60m()
        return tiles_60m  # Already per hour
    
    def get_session_success_rate(self) -> float:
        """Calculate session success rate over last 15 minutes."""
        successes = self.session_successes.count_last_15m()
        failures = self.session_failures.count_last_15m()
        total = successes + failures
        return (successes / total * 100) if total > 0 else 100.0
    
    def get_api_success_rate(self) -> float:
        """API call success rate over last 15 minutes."""
        req = self.api_requests.count_last_15m()
        fail = self.api_failures.count_last_15m()
        return ((req - fail) / req * 100) if req > 0 else 100.0
    
    def get_api_avg_latency_ms_5m(self) -> float:
        """Average API latency over last 5 minutes."""
        req = self.api_requests.count_last_5m()
        if req <= 0:
            return 0.0
        total_ms = self.api_latency_ms.count_last_5m()
        return total_ms / req
    
    def get_time_since_last_success(self) -> float:
        """Get seconds since last successful operation."""
        return time.time() - self.last_success_time
    
    def is_stalled(self, threshold_sec: float = 600) -> bool:
        """
        Check if worker appears stalled (no progress in threshold seconds).
        
        Default threshold: 10 minutes without any successful tile completion.
        """
        return self.get_time_since_last_success() > threshold_sec
    
    def get_periodic_snapshot(self) -> dict:
        """
        Generate a comprehensive metrics snapshot for periodic logging.
        
        Returns a JSON-serializable dict suitable for JSONL output.
        """
        now = time.time()
        
        # Collect error counts by type
        error_summary = {}
        for error_type, counter in self.error_counts.items():
            count_5m = counter.count_last_5m()
            if count_5m > 0:
                error_summary[error_type] = int(count_5m)
        
        snapshot = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "worker": self.worker_id,
            "uptime_hours": (now - self.start_time) / 3600,
            
            # Velocity metrics
            "tiles_completed_total": int(self.tiles_completed.total),
            "tiles_completed_last_5m": int(self.tiles_completed.count_last_5m()),
            "tiles_completed_last_60m": int(self.tiles_completed.count_last_60m()),
            "velocity_tiles_per_hour": round(self.get_velocity_tiles_per_hour(), 1),
            
            "towers_found_total": int(self.towers_found.total),
            "towers_found_last_5m": int(self.towers_found.count_last_5m()),
            
            "data_written_mb": round(self.data_written_bytes.total / (1024 * 1024), 2),
            
            # Health indicators
            "session_success_rate": round(self.get_session_success_rate(), 1),
            "api_success_rate": round(self.get_api_success_rate(), 1),
            "api_avg_latency_ms_5m": round(self.get_api_avg_latency_ms_5m(), 1),
            "captcha_hits_last_5m": int(self.captcha_hits.count_last_5m()),
            "captcha_hits_last_60m": int(self.captcha_hits.count_last_60m()),
            
            "error_counts": error_summary,
            
            # Proxy health
            "active_proxies": self.active_proxies,
            "bad_proxies": self.bad_proxies,
            "cooling_proxies": self.cooling_proxies,
            
            # Staleness
            "time_since_last_success_sec": int(self.get_time_since_last_success()),
            "is_stalled": self.is_stalled(),
        }
        
        self.last_snapshot_time = now
        return snapshot
    
    def write_snapshot_to_file(self) -> None:
        """Write current snapshot to JSONL metrics file."""
        snapshot = self.get_periodic_snapshot()
        metrics_file = self.logs_dir / f"worker_metrics_{self.worker_id}.jsonl"
        
        try:
            self.logs_dir.mkdir(parents=True, exist_ok=True)
            with open(metrics_file, "a") as f:
                f.write(json.dumps(snapshot) + "\n")
        except Exception as e:
            logger.warning(f"Failed to write metrics snapshot: {e}")
    
    def should_write_snapshot(self, interval_sec: float = 300) -> bool:
        """Check if enough time has passed to write another snapshot."""
        return (time.time() - self.last_snapshot_time) >= interval_sec
    
    def get_velocity_report(self) -> str:
        """Generate a formatted velocity report for logging."""
        velocity = self.get_velocity_tiles_per_hour()
        towers_5m = int(self.towers_found.count_last_5m())
        success_rate = self.get_session_success_rate()
        
        return (
            f"velocity={velocity:.1f} tiles/hr | "
            f"+{towers_5m} towers (5m) | "
            f"success={success_rate:.0f}%"
        )

