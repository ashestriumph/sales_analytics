
# Transform layer — cast types, build dimension records, generate date spine.
# Raw strings in → typed DataFrames out.

import logging
from datetime import date, timedelta

import pandas as pd

logger = logging.getLogger(__name__)


# ── Sales ────────────────────────────────────────────────────────────────────

def transform_sales(df: pd.DataFrame) -> pd.DataFrame:
    t = df.copy()
    t["order_date"]  = pd.to_datetime(t["order_date"]).dt.date
    t["quantity"]    = pd.to_numeric(t["quantity"]).astype(int)
    t["unit_price"]  = pd.to_numeric(t["unit_price"]).round(2)
    t["discount"]    = pd.to_numeric(t["discount"]).round(4)
    t["region"]      = t["region"].str.strip().str.title()
    t["channel"]     = t["channel"].str.strip().str.title()
    logger.info(f"[TRANSFORM] sales → {len(t)} rows typed and cleaned")
    return t


# ── Products ─────────────────────────────────────────────────────────────────

def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    t = df.copy()
    t["unit_cost"] = pd.to_numeric(t["unit_cost"], errors="coerce").round(2)
    t["category"]  = t["category"].str.strip().str.title()
    t["sub_category"] = t["sub_category"].str.strip().str.title()
    logger.info(f"[TRANSFORM] products → {len(t)} rows")
    return t


# ── Customers ────────────────────────────────────────────────────────────────

def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    t = df.copy()
    t["customer_name"] = t["customer_name"].str.strip().str.title()
    t["email"]    = t["email"].str.strip().str.lower()
    t["city"]     = t["city"].str.strip().str.title()
    t["country"]  = t["country"].str.strip().str.title()
    t["segment"]  = t["segment"].str.strip().str.title()
    logger.info(f"[TRANSFORM] customers → {len(t)} rows")
    return t


# ── Date spine ────────────────────────────────────────────────────────────────

def build_date_dimension(start_year: int = 2022, end_year: int = 2025) -> pd.DataFrame:
    """Generate a full date dimension from Jan 1 start_year to Dec 31 end_year."""
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    days  = [(start + timedelta(days=i)) for i in range((end - start).days + 1)]

    rows = []
    for d in days:
        rows.append({
            "date_key":     int(d.strftime("%Y%m%d")),
            "full_date":    d,
            "day_of_week":  d.isoweekday(),          # 1=Mon, 7=Sun
            "day_name":     d.strftime("%A"),
            "day_of_month": d.day,
            "day_of_year":  d.timetuple().tm_yday,
            "week_of_year": int(d.strftime("%W")),
            "month_num":    d.month,
            "month_name":   d.strftime("%B"),
            "quarter":      (d.month - 1) // 3 + 1,
            "year":         d.year,
            "is_weekend":   d.isoweekday() >= 6,
            "is_month_end": (d + timedelta(days=1)).month != d.month,
        })

    df = pd.DataFrame(rows)
    logger.info(f"[TRANSFORM] dim_date → {len(df)} rows ({start_year}–{end_year})")
    return df
