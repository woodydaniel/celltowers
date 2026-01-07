"""
Email Notification System for CellMapper Scraper

Sends alerts via Resend when:
- Critical errors stop the scrape (CAPTCHA, auth failure, unhandled exceptions)
- Repeated warnings occur (5+ consecutive request failures)

Each email identifies the scrape instance (carrier, hostname, PID) for debugging.
"""

from __future__ import annotations

import logging
import os
import socket
import time
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Try to import resend, gracefully handle if not installed
try:
    import resend
    RESEND_AVAILABLE = True
except ImportError:
    RESEND_AVAILABLE = False
    logger.warning("Resend package not installed. Email notifications disabled.")


class ResendNotifier:
    """
    Email notification handler using Resend.
    
    Features:
    - Rate limiting to prevent spam (max 1 email per 5 minutes per error type)
    - Instance identification (hostname, carrier, PID)
    - Graceful fallback if Resend unavailable
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        recipient_email: Optional[str] = None,
        rate_limit_seconds: int = 300,  # 5 minutes
    ):
        """
        Initialize the notifier.
        
        Args:
            api_key: Resend API key (defaults to RESEND_API_KEY env var)
            recipient_email: Email to send alerts to (defaults to ALERT_EMAIL env var)
            rate_limit_seconds: Minimum seconds between emails of same type
        """
        self.api_key = api_key or os.environ.get("RESEND_API_KEY", "")
        self.recipient_email = recipient_email or os.environ.get("ALERT_EMAIL", "")
        self.rate_limit_seconds = rate_limit_seconds
        
        # Track last send time per error type to prevent spam
        self._last_sent: dict[str, float] = {}
        
        # Instance identification
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.carrier: Optional[str] = None
        
        # Progress tracking (updated externally)
        self.tiles_completed = 0
        self.towers_collected = 0
        
        # Initialize Resend if available
        self.enabled = False
        if RESEND_AVAILABLE and self.api_key and self.recipient_email:
            resend.api_key = self.api_key
            self.enabled = True
            logger.info(f"Email notifications enabled -> {self.recipient_email}")
        elif not self.api_key:
            logger.info("Email notifications disabled (no API key)")
        elif not self.recipient_email:
            logger.info("Email notifications disabled (no recipient email)")
    
    def set_carrier(self, carrier: str) -> None:
        """Set the current carrier being scraped."""
        self.carrier = carrier
    
    def update_progress(self, tiles: int, towers: int) -> None:
        """Update progress tracking for email context."""
        self.tiles_completed = tiles
        self.towers_collected = towers
    
    def _can_send(self, error_type: str) -> bool:
        """Check if we can send an email (rate limiting)."""
        now = time.time()
        last_sent = self._last_sent.get(error_type, 0)
        return (now - last_sent) >= self.rate_limit_seconds
    
    def _mark_sent(self, error_type: str) -> None:
        """Mark that we sent an email for this error type."""
        self._last_sent[error_type] = time.time()
    
    def _get_instance_info(self) -> str:
        """Get formatted instance identification."""
        carrier = self.carrier or "unknown"
        return f"{carrier} @ {self.hostname} (PID: {self.pid})"
    
    def send_critical_error(
        self,
        error_type: str,
        details: str = "",
        carrier: Optional[str] = None,
    ) -> bool:
        """
        Send alert for critical error that stops the scrape.
        
        Args:
            error_type: Type of error (e.g., "CAPTCHA_REQUIRED", "AUTH_FAILED")
            details: Additional error details
            carrier: Override carrier name for this alert
            
        Returns:
            True if email sent, False otherwise
        """
        if carrier:
            self.carrier = carrier
        
        if not self.enabled:
            logger.warning(f"[ALERT DISABLED] Critical error: {error_type} - {details}")
            return False
        
        error_key = f"critical_{error_type}"
        if not self._can_send(error_key):
            logger.debug(f"Rate limited: {error_type}")
            return False
        
        subject = f"[CellMapper] CRITICAL: Scrape stopped - {self._get_instance_info()}"
        
        body = self._format_critical_email(error_type, details)
        
        success = self._send_email(subject, body)
        if success:
            self._mark_sent(error_key)
        return success
    
    def send_warning_threshold(
        self,
        consecutive_failures: int,
        last_error: str = "",
        carrier: Optional[str] = None,
    ) -> bool:
        """
        Send alert when warning threshold is reached.
        
        Args:
            consecutive_failures: Number of consecutive failures
            last_error: Last error message
            carrier: Override carrier name for this alert
            
        Returns:
            True if email sent, False otherwise
        """
        if carrier:
            self.carrier = carrier
        
        if not self.enabled:
            logger.warning(
                f"[ALERT DISABLED] Warning threshold: {consecutive_failures} failures"
            )
            return False
        
        error_key = "warning_threshold"
        if not self._can_send(error_key):
            logger.debug("Rate limited: warning threshold")
            return False
        
        subject = f"[CellMapper] WARNING: {consecutive_failures} consecutive failures - {self._get_instance_info()}"
        
        body = self._format_warning_email(consecutive_failures, last_error)
        
        success = self._send_email(subject, body)
        if success:
            self._mark_sent(error_key)
        return success

    def send_harvester_paused(
        self,
        reason: str,
        details: str = "",
    ) -> bool:
        """
        Send alert when the harvester pauses minting due to stalled workers.

        This is intentionally separate from critical errors: the scrape may still be running,
        but harvest bandwidth is being protected until workers recover.
        """
        # Identify this as a harvester-side event
        self.carrier = "harvester"

        if not self.enabled:
            logger.warning(f"[ALERT DISABLED] Harvester paused: {reason} - {details}")
            return False

        error_key = "harvester_paused"
        if not self._can_send(error_key):
            logger.debug("Rate limited: harvester paused")
            return False

        subject = f"[CellMapper] PAUSED: Harvester minting paused - {self.hostname}"

        body = f"""
CellMapper Scraper - Harvester Minting Paused

Reason: {reason}
Details: {details or 'No additional details'}

Server: {self.hostname}
PID: {self.pid}
Time: {datetime.utcnow().isoformat()}Z

Why this happened:
- Workers appear stalled or not making progress, so the harvester is pausing to avoid burning proxy bandwidth.

What to do:
- Check worker logs/metrics and restart workers if needed.
- Once workers resume progress, harvesting will automatically resume.

Quick checks:
  ssh root@{self.hostname} 'docker compose ps'
  ssh root@{self.hostname} 'docker compose logs --tail=200 tmobile_west'
  ssh root@{self.hostname} 'docker compose logs --tail=200 harvester'
"""

        success = self._send_email(subject, body)
        if success:
            self._mark_sent(error_key)
        return success
    
    def send_test_email(self) -> bool:
        """Send a test email to verify configuration."""
        if not self.enabled:
            logger.error("Cannot send test email: notifications not enabled")
            return False
        
        subject = f"[CellMapper] Test Alert - {self.hostname}"
        body = f"""
CellMapper Scraper - Test Email

This is a test email to verify your notification configuration.

Server: {self.hostname}
Time: {datetime.utcnow().isoformat()}Z
Recipient: {self.recipient_email}

If you received this email, your notifications are configured correctly!
"""
        return self._send_email(subject, body)
    
    def send_completion(
        self,
        duration_hours: float = 0,
        carrier: Optional[str] = None,
    ) -> bool:
        """
        Send notification when scrape completes successfully.
        
        Args:
            duration_hours: How long the scrape took
            carrier: Override carrier name for this alert
            
        Returns:
            True if email sent, False otherwise
        """
        if carrier:
            self.carrier = carrier
        
        if not self.enabled:
            logger.info(
                f"[ALERT DISABLED] Scrape completed: {self.carrier}"
            )
            return False
        
        subject = f"[CellMapper] SUCCESS: {self.carrier} scrape completed - {self.hostname}"
        
        body = self._format_completion_email(duration_hours)
        
        return self._send_email(subject, body)
    
    def _format_completion_email(self, duration_hours: float) -> str:
        """Format completion notification email body."""
        return f"""
CellMapper Scraper - Scrape Completed Successfully!

Carrier: {self.carrier or 'unknown'}
Server: {self.hostname}
PID: {self.pid}
Completed At: {datetime.utcnow().isoformat()}Z

Final Stats:
- Tiles completed: {self.tiles_completed:,}
- Towers collected: {self.towers_collected:,}
- Duration: {duration_hours:.1f} hours

The scrape for {self.carrier} has finished successfully.
Data file: /root/cellmapper/data/towers/towers_{self.carrier}.jsonl

To download:
  scp root@{self.hostname}:/root/cellmapper/data/towers/towers_{self.carrier}.jsonl ./
"""
    
    def _format_critical_email(self, error_type: str, details: str) -> str:
        """Format critical error email body."""
        return f"""
CellMapper Scraper - Critical Error

Scrape Instance: {self.carrier or 'unknown'}
Server: {self.hostname}
PID: {self.pid}
Time: {datetime.utcnow().isoformat()}Z

Error Type: {error_type}
Details: {details or 'No additional details'}

Last Progress:
- Tiles completed: {self.tiles_completed:,}
- Towers collected: {self.towers_collected:,}

Action Required: Check logs and restart if needed.

SSH Command:
  ssh root@{self.hostname} 'tail -100 /root/cellmapper/logs/scraper.log'
"""
    
    def _format_warning_email(self, consecutive_failures: int, last_error: str) -> str:
        """Format warning threshold email body."""
        return f"""
CellMapper Scraper - Warning Threshold Reached

Scrape Instance: {self.carrier or 'unknown'}
Server: {self.hostname}
PID: {self.pid}
Time: {datetime.utcnow().isoformat()}Z

Consecutive Failures: {consecutive_failures}
Last Error: {last_error or 'Unknown'}

Current Progress:
- Tiles completed: {self.tiles_completed:,}
- Towers collected: {self.towers_collected:,}

The scraper is still running but experiencing issues.
If failures continue, the scrape may stop.

SSH Command:
  ssh root@{self.hostname} 'tail -50 /root/cellmapper/logs/scraper.log'
"""
    
    def _send_email(self, subject: str, body: str) -> bool:
        """Send email via Resend API."""
        if not RESEND_AVAILABLE:
            logger.error("Resend package not available")
            return False
        
        try:
            params = {
                "from": "CellMapper Scraper <onboarding@resend.dev>",
                "to": [self.recipient_email],
                "subject": subject,
                "text": body,
            }
            
            response = resend.Emails.send(params)
            
            if response and response.get("id"):
                logger.info(f"Email sent successfully: {response['id']}")
                return True
            else:
                logger.error(f"Email send failed: {response}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False


# Module-level notifier instance for easy access
_notifier: Optional[ResendNotifier] = None


def get_notifier() -> ResendNotifier:
    """Get or create the global notifier instance."""
    global _notifier
    if _notifier is None:
        _notifier = ResendNotifier()
    return _notifier

