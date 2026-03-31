
#  sales data in 3 formats:
#   - sales.csv       (main transactional data)
#   - products.json   (product catalog)
#   - customers.xml   (CRM export — XML format as per JD requirement)

import csv
import json
import random
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

RAW_DIR = Path(__file__).parent / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ── Reference data ──────────────────────────────────────────────────────────

REGIONS = ["North", "South", "East", "West", "Central"]
CHANNELS = ["Online", "Retail", "Wholesale", "Direct"]
SEGMENTS = ["Consumer", "Corporate", "SMB"]
COUNTRIES = ["Vietnam", "Thailand", "Singapore", "Malaysia", "Indonesia"]

CATEGORIES = {
    "Electronics":  ["Smartphones", "Laptops", "Accessories", "Tablets"],
    "Furniture":    ["Chairs", "Desks", "Shelves", "Lamps"],
    "Office":       ["Stationery", "Paper", "Printers", "Software"],
    "Clothing":     ["Shirts", "Pants", "Shoes", "Accessories"],
}

SUPPLIERS = [f"SUP{str(i).zfill(3)}" for i in range(1, 11)]


def random_date(start_year=2022, end_year=2025):
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


# ── 1. Products (JSON) ──────────────────────────────────────────────────────

products = []
product_ids = []
for i in range(1, 51):
    pid = f"PRD{str(i).zfill(4)}"
    product_ids.append(pid)
    cat = random.choice(list(CATEGORIES.keys()))
    sub = random.choice(CATEGORIES[cat])
    unit_cost = round(random.uniform(5, 500), 2)
    products.append({
        "product_id":   pid,
        "product_name": f"{sub} Model {chr(64 + (i % 26) + 1)}{i}",
        "category":     cat,
        "sub_category": sub,
        "unit_cost":    unit_cost,
        "supplier_id":  random.choice(SUPPLIERS),
    })

with open(RAW_DIR / "products.json", "w") as f:
    json.dump(products, f, indent=2)
print(f"[OK] products.json — {len(products)} records")


# ── 2. Customers (XML — CRM export format) ──────────────────────────────────

root = ET.Element("Customers")
customer_ids = []
for i in range(1, 201):
    cid = f"CUS{str(i).zfill(5)}"
    customer_ids.append(cid)
    c = ET.SubElement(root, "Customer")
    ET.SubElement(c, "CustomerID").text    = cid
    ET.SubElement(c, "CustomerName").text  = f"Customer {i}"
    ET.SubElement(c, "Email").text         = f"customer{i}@example.com"
    ET.SubElement(c, "Phone").text         = f"+84{random.randint(300000000, 999999999)}"
    ET.SubElement(c, "City").text          = random.choice(["Hanoi", "HCMC", "Danang", "Hue", "Cantho"])
    ET.SubElement(c, "Country").text       = random.choice(COUNTRIES)
    ET.SubElement(c, "Segment").text       = random.choice(SEGMENTS)

tree = ET.ElementTree(root)
ET.indent(tree, space="  ")
tree.write(RAW_DIR / "customers.xml", encoding="unicode", xml_declaration=True)
print(f"[OK] customers.xml — {len(customer_ids)} records")


# ── 3. Sales (CSV — main transactional data) ─────────────────────────────────

sales_rows = []
for i in range(1, 5001):
    oid = f"ORD{str(i).zfill(6)}"
    pid = random.choice(product_ids)
    product = next(p for p in products if p["product_id"] == pid)
    unit_cost = product["unit_cost"]
    unit_price = round(unit_cost * random.uniform(1.2, 2.5), 2)
    quantity = random.randint(1, 50)
    discount = random.choice([0, 0.05, 0.10, 0.15, 0.20])
    sales_rows.append({
        "order_id":    oid,
        "order_date":  str(random_date()),
        "customer_id": random.choice(customer_ids),
        "product_id":  pid,
        "quantity":    quantity,
        "unit_price":  unit_price,
        "discount":    discount,
        "region":      random.choice(REGIONS),
        "channel":     random.choice(CHANNELS),
    })

# Inject ~3% dirty rows to demonstrate data quality checks
dirty_indices = random.sample(range(len(sales_rows)), k=int(len(sales_rows) * 0.03))
for idx in dirty_indices:
    fault = random.choice(["null_date", "negative_qty", "bad_price"])
    if fault == "null_date":
        sales_rows[idx]["order_date"] = ""
    elif fault == "negative_qty":
        sales_rows[idx]["quantity"] = -1
    elif fault == "bad_price":
        sales_rows[idx]["unit_price"] = -99.99

with open(RAW_DIR / "sales.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=sales_rows[0].keys())
    writer.writeheader()
    writer.writerows(sales_rows)

dirty_count = len(dirty_indices)
print(f"[OK] sales.csv — {len(sales_rows)} records ({dirty_count} dirty rows injected for DQ demo)")
