#!/bin/bash
# Daily progress backup script
# Run via cron: 0 */6 * * * /root/cellmapper/scripts/backup_progress.sh
#
# Keeps last 7 days of backups in /root/cellmapper/backups/daily/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${PROJECT_DIR}/data"
BACKUP_DIR="${PROJECT_DIR}/backups/daily"
KEEP_DAYS=7

# Create backup directory if needed
mkdir -p "$BACKUP_DIR"

# Timestamp for this backup
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_SUBDIR="${BACKUP_DIR}/${TIMESTAMP}"

# Only backup if progress files exist
if ls "${DATA_DIR}"/progress_*.json 1>/dev/null 2>&1; then
    mkdir -p "$BACKUP_SUBDIR"
    
    # Copy progress files
    cp "${DATA_DIR}"/progress_*.json "$BACKUP_SUBDIR/" 2>/dev/null || true
    
    # Also backup tower line counts for reference
    for f in "${DATA_DIR}"/towers/*.jsonl; do
        if [[ -f "$f" ]]; then
            basename "$f" >> "${BACKUP_SUBDIR}/tower_counts.txt"
            wc -l < "$f" >> "${BACKUP_SUBDIR}/tower_counts.txt"
        fi
    done
    
    echo "[$(date -Iseconds)] Backup created: $BACKUP_SUBDIR"
else
    echo "[$(date -Iseconds)] No progress files to backup"
    exit 0
fi

# Clean up old backups (keep last N days)
find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -type d -mtime +${KEEP_DAYS} -exec rm -rf {} \; 2>/dev/null || true

echo "[$(date -Iseconds)] Cleanup complete. Keeping last ${KEEP_DAYS} days of backups."
