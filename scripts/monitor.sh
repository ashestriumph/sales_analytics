#!/usr/bin/env bash
# ============================================================
# monitor.sh — Operational data quality monitoring
# Checks row counts, DQ pass rate, last ETL run
# ============================================================
set -euo pipefail

DB_USER="dataeng"
DB_NAME="sales_analytics"

psql() { docker exec -i sales_dw psql -U "$DB_USER" -d "$DB_NAME" "$@"; }

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "============================================"
log "  DATA PLATFORM HEALTH CHECK"
log "============================================"

log "--- Row counts ---"
psql -c "
SELECT schemaname, tablename,
       n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE schemaname IN ('raw','staging','warehouse')
ORDER BY schemaname, tablename;
"

log "--- Data quality pass rate (staging.stg_sales) ---"
psql -c "
SELECT
    COUNT(*)                                                  AS total,
    SUM(CASE WHEN _dq_passed THEN 1 ELSE 0 END)              AS passed,
    SUM(CASE WHEN NOT _dq_passed THEN 1 ELSE 0 END)          AS failed,
    ROUND(
        SUM(CASE WHEN _dq_passed THEN 1 ELSE 0 END)::numeric
        / NULLIF(COUNT(*),0) * 100, 2)                        AS pass_rate_pct
FROM staging.stg_sales;
"

log "--- Schema migrations applied ---"
psql -c "SELECT version, description, applied_at FROM public.schema_migrations ORDER BY version;"

log "--- Warehouse fact_sales by partition (year) ---"
psql -c "
SELECT
    d.year,
    COUNT(*)                    AS rows,
    ROUND(SUM(net_amount), 2)   AS total_revenue
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON d.date_key = f.date_key
GROUP BY d.year
ORDER BY d.year;
"

log "Health check complete."
