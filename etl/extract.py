
# Extract layer — reads CSV, JSON, XML source files.
# Returns raw DataFrames without any transformation.


import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from .config import DATA_DIR

logger = logging.getLogger(__name__)


def extract_sales_csv() -> pd.DataFrame:
    path = DATA_DIR / "sales.csv"
    df = pd.read_csv(path, dtype=str)           # read everything as string → no implicit casting
    df["_source_file"] = path.name
    logger.info(f"[EXTRACT] sales.csv → {len(df)} rows")
    return df


def extract_products_json() -> pd.DataFrame:
    path = DATA_DIR / "products.json"
    with open(path) as f:
        records = json.load(f)
    df = pd.DataFrame(records).astype(str)
    df["_source_file"] = path.name
    logger.info(f"[EXTRACT] products.json → {len(df)} rows")
    return df


def extract_customers_xml() -> pd.DataFrame:
    """Parse CRM XML export — demonstrates XML processing as required by JD."""
    path = DATA_DIR / "customers.xml"
    tree = ET.parse(path)
    root = tree.getroot()
    rows = []
    for customer in root.findall("Customer"):
        rows.append({
            "customer_id":   _text(customer, "CustomerID"),
            "customer_name": _text(customer, "CustomerName"),
            "email":         _text(customer, "Email"),
            "phone":         _text(customer, "Phone"),
            "city":          _text(customer, "City"),
            "country":       _text(customer, "Country"),
            "segment":       _text(customer, "Segment"),
            "_source_file":  path.name,
        })
    df = pd.DataFrame(rows)
    logger.info(f"[EXTRACT] customers.xml → {len(df)} rows")
    return df


def _text(element, tag: str) -> str:
    child = element.find(tag)
    return child.text.strip() if child is not None and child.text else ""
