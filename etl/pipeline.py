
# Main ETL orchestrator — runs the full Raw → Staging → Warehouse pipeline.
# Designed to be idempotent: safe to re-run without duplicating data.

# Usage:
#     python -m etl.pipeline

import logging
import sys
import time

import pandas as pd

from .config import get_dsn
from .extract import extract_sales_csv, extract_products_json, extract_customers_xml
from .transform import transform_sales, transform_products, transform_customers, build_date_dimension
from .quality import check_sales
from .load import (
    get_connection,
    load_raw_sales, load_raw_products, load_raw_customers,
    load_staging_sales, load_staging_products, load_staging_customers,
    load_dim_date, load_dim_product, load_dim_customer, load_dim_region,
    load_fact_sales,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_run.log"),
    ]
)
logger = logging.getLogger(__name__)


def run_pipeline():
    start = time.time()
    logger.info("=" * 60)
    logger.info("  SALES ANALYTICS ETL PIPELINE — START")
    logger.info("=" * 60)

    conn = get_connection()

    try:
        # ── EXTRACT ──────────────────────────────────────────────────────────
        logger.info("--- PHASE 1: EXTRACT ---")
        raw_sales     = extract_sales_csv()
        raw_products  = extract_products_json()
        raw_customers = extract_customers_xml()

        # ── RAW LOAD ─────────────────────────────────────────────────────────
        logger.info("--- PHASE 2: LOAD RAW ---")
        load_raw_sales(raw_sales, conn)
        load_raw_products(raw_products, conn)
        load_raw_customers(raw_customers, conn)

        # ── TRANSFORM ────────────────────────────────────────────────────────
        logger.info("--- PHASE 3: TRANSFORM ---")
        t_products  = transform_products(raw_products)
        t_customers = transform_customers(raw_customers)

        # Data quality on sales before transform
        passed_sales, failed_sales, dq_report = check_sales(raw_sales)
        logger.info(f"[DQ] Pass rate: {dq_report.pass_rate}% | {dq_report.failed} rows quarantined")

        t_sales = transform_sales(passed_sales)
        dim_date = build_date_dimension(2022, 2025)

        # ── STAGING LOAD ──────────────────────────────────────────────────────
        logger.info("--- PHASE 4: LOAD STAGING ---")
        load_staging_products(t_products, conn)
        load_staging_customers(t_customers, conn)

        # Load both passed and failed sales to staging (with DQ flags)
        passed_sales["_dq_passed"] = True
        passed_sales["_dq_notes"]  = ""
        combined = pd.concat([passed_sales, failed_sales], ignore_index=True)
        load_staging_sales(combined, conn)

        # ── WAREHOUSE LOAD ────────────────────────────────────────────────────
        logger.info("--- PHASE 5: LOAD WAREHOUSE (Star Schema) ---")
        load_dim_date(dim_date, conn)
        load_dim_product(t_products, conn)
        load_dim_customer(t_customers, conn)
        load_dim_region(conn)
        fact_rows = load_fact_sales(conn)

        elapsed = round(time.time() - start, 2)
        logger.info("=" * 60)
        logger.info(f"  PIPELINE COMPLETE in {elapsed}s")
        logger.info(f"  fact_sales rows loaded: {fact_rows}")
        logger.info(f"  DQ pass rate: {dq_report.pass_rate}%")
        logger.info("=" * 60)

    except Exception as e:
        conn.rollback()
        logger.error(f"PIPELINE FAILED: {e}", exc_info=True)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
