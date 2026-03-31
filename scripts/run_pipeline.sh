#!/usr/bin/env bash
# ============================================================
# run_pipeline.sh — Start DB + run ETL pipeline
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "Starting PostgreSQL container..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d

log "Waiting for PostgreSQL to be ready..."
until docker exec sales_dw pg_isready -U dataeng -d sales_analytics &>/dev/null; do
    sleep 1
done
log "PostgreSQL is ready."

log "Generating source data..."
python3 "$PROJECT_DIR/data/generate_data.py"

log "Running ETL pipeline..."
cd "$PROJECT_DIR"
python3 -m etl.pipeline

log "Pipeline finished. Logs: $PROJECT_DIR/etl_run.log"
