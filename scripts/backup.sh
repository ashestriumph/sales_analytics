#!/usr/bin/env bash
# ============================================================
# backup.sh — Daily PostgreSQL dump to local backup directory
# Schedule with cron: 0 2 * * * /path/to/backup.sh
# ============================================================
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/var/backups/sales_analytics}"
DB_NAME="sales_analytics"
DB_USER="dataeng"
RETENTION_DAYS=7
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="$BACKUP_DIR/${DB_NAME}_${TIMESTAMP}.dump"

mkdir -p "$BACKUP_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "Starting backup → $BACKUP_FILE"
docker exec sales_dw pg_dump \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -F c \
    -f "/tmp/${DB_NAME}_${TIMESTAMP}.dump"

docker cp "sales_dw:/tmp/${DB_NAME}_${TIMESTAMP}.dump" "$BACKUP_FILE"
log "Backup written: $(du -sh "$BACKUP_FILE" | cut -f1)"

# Rotate old backups
log "Removing backups older than ${RETENTION_DAYS} days..."
find "$BACKUP_DIR" -name "*.dump" -mtime +"$RETENTION_DAYS" -delete

log "Backup complete."
