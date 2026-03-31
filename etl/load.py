
# Load layer — writes DataFrames to PostgreSQL (raw → staging → warehouse).
# Uses psycopg2 for bulk COPY operations (faster than INSERT row-by-row).


import io
import logging

import pandas as pd
import psycopg2
import psycopg2.extras

from .config import get_dsn

logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(get_dsn())


# ── Generic bulk loader ───────────────────────────────────────────────────────

def bulk_copy(df: pd.DataFrame, schema: str, table: str, conn) -> int:
    """Use COPY FROM for high-performance bulk inserts (on-premise best practice)."""
    if df.empty:
        logger.warning(f"[LOAD] {schema}.{table} — empty DataFrame, skipping")
        return 0

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)

    cols = ", ".join(df.columns)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {schema}.{table} ({cols}) FROM STDIN WITH CSV NULL '\\N'",
            buf
        )
    conn.commit()
    logger.info(f"[LOAD] {schema}.{table} ← {len(df)} rows")
    return len(df)


# ── Raw layer ─────────────────────────────────────────────────────────────────

def load_raw_sales(df: pd.DataFrame, conn):
    cols = ["order_id","order_date","customer_id","product_id",
            "quantity","unit_price","discount","region","channel","_source_file"]
    bulk_copy(df[cols], "raw", "sales", conn)


def load_raw_products(df: pd.DataFrame, conn):
    cols = ["product_id","product_name","category","sub_category",
            "unit_cost","supplier_id","_source_file"]
    bulk_copy(df[cols], "raw", "products", conn)


def load_raw_customers(df: pd.DataFrame, conn):
    cols = ["customer_id","customer_name","email","phone",
            "city","country","segment","_source_file"]
    bulk_copy(df[cols], "raw", "customers", conn)


# ── Staging layer ─────────────────────────────────────────────────────────────

def load_staging_sales(df: pd.DataFrame, conn):
    cols = ["order_id","order_date","customer_id","product_id",
            "quantity","unit_price","discount","region","channel",
            "_dq_passed","_dq_notes"]
    bulk_copy(df[cols], "staging", "stg_sales", conn)


def load_staging_products(df: pd.DataFrame, conn):
    cols = ["product_id","product_name","category","sub_category",
            "unit_cost","supplier_id"]
    bulk_copy(df[cols], "staging", "stg_products", conn)


def load_staging_customers(df: pd.DataFrame, conn):
    cols = ["customer_id","customer_name","email",
            "city","country","segment"]
    bulk_copy(df[cols], "staging", "stg_customers", conn)


# ── Warehouse layer ───────────────────────────────────────────────────────────

def load_dim_date(df: pd.DataFrame, conn):
    bulk_copy(df, "warehouse", "dim_date", conn)


def load_dim_product(df: pd.DataFrame, conn):
    cols = ["product_id","product_name","category","sub_category","unit_cost","supplier_id"]
    bulk_copy(df[cols], "warehouse", "dim_product", conn)


def load_dim_customer(df: pd.DataFrame, conn):
    cols = ["customer_id","customer_name","email","city","country","segment"]
    bulk_copy(df[cols], "warehouse", "dim_customer", conn)


def load_dim_region(conn):
    regions = [
        ("North", "Asia", "Asia/Ho_Chi_Minh"),
        ("South", "Asia", "Asia/Ho_Chi_Minh"),
        ("East",  "Asia", "Asia/Bangkok"),
        ("West",  "Asia", "Asia/Singapore"),
        ("Central","Asia","Asia/Ho_Chi_Minh"),
    ]
    with conn.cursor() as cur:
        for r in regions:
            cur.execute(
                "INSERT INTO warehouse.dim_region (region_name, country_group, timezone) "
                "VALUES (%s, %s, %s) ON CONFLICT (region_name) DO NOTHING",
                r
            )
    conn.commit()
    logger.info(f"[LOAD] warehouse.dim_region ← {len(regions)} rows")


def load_fact_sales(conn):
    """
    Build fact_sales from staging using SQL JOIN to resolve surrogate keys.
    This keeps the Python layer thin and pushes set-based logic to PostgreSQL.
    """
    sql = """
    INSERT INTO warehouse.fact_sales
        (date_key, product_key, customer_key, region_key,
         order_id, channel, quantity, unit_price, unit_cost, discount)
    SELECT
        dd.date_key,
        dp.product_key,
        dc.customer_key,
        dr.region_key,
        s.order_id,
        s.channel,
        s.quantity,
        s.unit_price,
        sp.unit_cost,
        s.discount
    FROM staging.stg_sales s
    JOIN warehouse.dim_date     dd ON dd.full_date    = s.order_date
    JOIN warehouse.dim_product  dp ON dp.product_id   = s.product_id  AND dp.is_current
    JOIN warehouse.dim_customer dc ON dc.customer_id  = s.customer_id AND dc.is_current
    LEFT JOIN warehouse.dim_region dr ON dr.region_name = s.region
    LEFT JOIN staging.stg_products sp ON sp.product_id = s.product_id
    WHERE s._dq_passed = TRUE
    ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.rowcount
    conn.commit()
    logger.info(f"[LOAD] warehouse.fact_sales ← {rows} rows inserted")
    return rows
