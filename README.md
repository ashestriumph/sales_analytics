# Sales Analytics Data Platform

An on-premise data platform built with **PostgreSQL**, **Python**, and **Bash** to demonstrate production-grade data engineering practices: layered architecture, Kimball Star Schema, schema versioning, data quality checks, and advanced SQL analytics.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCE SYSTEMS                                                      │
│  sales.csv (OLTP)  │  products.json (catalog)  │  customers.xml (CRM)│
└──────────┬──────────┴────────────┬──────────────┴──────────┬────────┘
           │                       │                          │
           ▼                       ▼                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  RAW LAYER (schema: raw)                                             │
│  Append-only landing tables — no transformations, full audit trail   │
└──────────────────────────────────────┬───────────────────────────────┘
                                       │  Python ETL + DQ checks
                                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STAGING LAYER (schema: staging)                                     │
│  Typed, cleaned, DQ-flagged rows — failed rows quarantined in-place  │
└──────────────────────────────────────┬───────────────────────────────┘
                                       │  SQL JOIN → surrogate keys
                                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  WAREHOUSE LAYER (schema: warehouse) — Kimball Star Schema           │
│                                                                      │
│  dim_date ──┐                                                        │
│  dim_product┼──► fact_sales (PARTITIONED BY year)                   │
│  dim_customer┘       generated columns: gross_amount, net_amount,   │
│  dim_region ─────────  gross_margin                                  │
└──────────────────────────────────────────────────────────────────────┘
```

**Modeling standard:** Kimball Star Schema — optimized for analytical queries (low join depth, fast aggregation).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Database | PostgreSQL 15 (on-premise via Docker) |
| ETL / Transform | Python 3 (pandas, psycopg2) |
| Orchestration | Bash scripts (cron-ready) |
| Schema versioning | Flyway-style migration files (V001–V005) |
| Data formats | CSV, JSON, XML |
| Query engine | PostgreSQL (EXPLAIN ANALYZE, partition pruning) |

---

## Star Schema — Data Model

```
                    ┌──────────────┐
                    │  dim_date    │
                    │  date_key PK │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴────────────────────┐    ┌───────────────┐
│ dim_product  │    │       fact_sales           │    │ dim_customer  │
│ product_key  ├────│  date_key      FK          ├────│ customer_key  │
│ product_id   │    │  product_key   FK          │    │ customer_id   │
│ product_name │    │  customer_key  FK          │    │ customer_name │
│ category     │    │  region_key    FK          │    │ segment       │
│ sub_category │    │  order_id                  │    │ country       │
│ unit_cost    │    │  quantity                  │    └───────────────┘
│ supplier_id  │    │  unit_price                │
│ is_current   │    │  discount                  │    ┌───────────────┐
└──────────────┘    │  gross_amount (computed)   │    │ dim_region    │
                    │  net_amount   (computed)   ├────│ region_key    │
                    │  gross_margin (computed)   │    │ region_name   │
                    └────────────────────────────┘    └───────────────┘
```

`fact_sales` is **range-partitioned by `date_key`** (year) for query pruning performance.

---

## Data Dictionary

### `warehouse.fact_sales`

| Column | Type | Description |
|---|---|---|
| `sale_id` | BIGSERIAL | Surrogate primary key |
| `date_key` | INT | FK → dim_date (YYYYMMDD) |
| `product_key` | INT | FK → dim_product (surrogate) |
| `customer_key` | INT | FK → dim_customer (surrogate) |
| `region_key` | INT | FK → dim_region |
| `order_id` | VARCHAR(20) | Source system order identifier |
| `channel` | VARCHAR(30) | Sales channel (Online/Retail/Wholesale/Direct) |
| `quantity` | INT | Number of units sold |
| `unit_price` | NUMERIC(12,2) | Selling price per unit at time of sale |
| `unit_cost` | NUMERIC(12,2) | Cost price per unit (from product dim) |
| `discount` | NUMERIC(5,4) | Discount rate applied (0.00–1.00) |
| `gross_amount` | NUMERIC(14,2) | `quantity × unit_price` (generated column) |
| `net_amount` | NUMERIC(14,2) | `gross_amount × (1 - discount)` (generated) |
| `gross_margin` | NUMERIC(14,2) | `quantity × (unit_price - unit_cost)` (generated) |

### `warehouse.dim_product`

| Column | Type | Description |
|---|---|---|
| `product_key` | SERIAL | Surrogate key (SCD Type 2 ready) |
| `product_id` | VARCHAR(20) | Natural key from source system |
| `product_name` | VARCHAR(200) | Full product name |
| `category` | VARCHAR(100) | Product category |
| `sub_category` | VARCHAR(100) | Product sub-category |
| `unit_cost` | NUMERIC(12,2) | Cost price |
| `supplier_id` | VARCHAR(20) | Supplier reference |
| `effective_from` | DATE | SCD Type 2: validity start |
| `effective_to` | DATE | SCD Type 2: validity end (NULL = current) |
| `is_current` | BOOLEAN | SCD Type 2: current record flag |

### `warehouse.dim_customer`

| Column | Type | Description |
|---|---|---|
| `customer_key` | SERIAL | Surrogate key (SCD Type 2 ready) |
| `customer_id` | VARCHAR(20) | Natural key from CRM (XML source) |
| `customer_name` | VARCHAR(200) | Full name |
| `email` | VARCHAR(200) | Contact email |
| `city` | VARCHAR(100) | City |
| `country` | VARCHAR(100) | Country |
| `segment` | VARCHAR(50) | Customer segment (Consumer/Corporate/SMB) |

### `warehouse.dim_date`

| Column | Type | Description |
|---|---|---|
| `date_key` | INT | Surrogate key in YYYYMMDD format |
| `full_date` | DATE | Calendar date |
| `day_of_week` | SMALLINT | 1=Monday, 7=Sunday |
| `month_num` | SMALLINT | Month number (1–12) |
| `quarter` | SMALLINT | Quarter (1–4) |
| `year` | SMALLINT | Calendar year |
| `is_weekend` | BOOLEAN | True if Saturday or Sunday |
| `is_month_end` | BOOLEAN | True if last day of month |

### `staging.stg_sales` — Data Quality flags

| Column | Description |
|---|---|
| `_dq_passed` | True if all quality rules passed |
| `_dq_notes` | Pipe-separated error messages for failed rows |

---

## Schema Versioning

Migrations follow the **Flyway naming convention** (`VXXX__description.sql`) and are tracked in `public.schema_migrations`.

| Version | Description |
|---|---|
| V001 | Initialize schemas: raw, staging, warehouse |
| V002 | Create raw landing tables |
| V003 | Create staging tables with type casting and DQ flags |
| V004 | Create Kimball Star Schema with year partitioning |
| V005 | Add performance indexes on warehouse tables |

---

## Data Quality Rules

Applied at the staging layer before loading to warehouse:

| Rule | Column | Condition |
|---|---|---|
| Valid date | `order_date` | Must parse as a valid ISO date |
| Positive quantity | `quantity` | Must be > 0 |
| Non-negative price | `unit_price` | Must be ≥ 0 |
| Non-null FK | `customer_id`, `product_id` | Must not be null |

Failed rows are **quarantined in staging** with `_dq_passed = FALSE` and a human-readable `_dq_notes` message. They are excluded from warehouse load but retained for audit.

---

## Data Lineage

```
sales.csv        → raw.sales        → staging.stg_sales (DQ checked)
                                            │
                                            ▼ (DQ passed only)
products.json    → raw.products     → staging.stg_products
                                            │
customers.xml    → raw.customers    → staging.stg_customers
                                            │
                              ┌─────────────┼──────────────────┐
                              ▼             ▼                  ▼
                     warehouse.dim_product  dim_customer  dim_date
                              │             │                  │
                              └─────────────┴──────────────────┘
                                            │
                                            ▼
                                  warehouse.fact_sales
```

---

## How to Run

```bash
# 1. Start the database and run the full pipeline
bash scripts/run_pipeline.sh

# 2. Check platform health (row counts, DQ rate, partitions)
bash scripts/monitor.sh

# 3. Run analytics queries
docker exec -i sales_dw psql -U dataeng -d sales_analytics < queries/analytics.sql
```

---

## Project Structure

```
sales_analytics/
├── docker-compose.yml          # PostgreSQL on-premise simulation
├── migrations/
│   ├── V001__init_schemas.sql
│   ├── V002__raw_landing.sql
│   ├── V003__staging_tables.sql
│   ├── V004__warehouse_star_schema.sql
│   └── V005__indexes_and_constraints.sql
├── data/
│   ├── generate_data.py        # Generates CSV + JSON + XML test data
│   └── raw/
│       ├── sales.csv           # 5,000 rows (with injected dirty records)
│       ├── products.json       # 50 products
│       └── customers.xml       # 200 customers (XML / CRM format)
├── etl/
│   ├── config.py               # DB connection config
│   ├── extract.py              # Multi-format extraction (CSV, JSON, XML)
│   ├── transform.py            # Type casting, cleaning, date spine
│   ├── quality.py              # Data quality checks with DQReport
│   ├── load.py                 # Bulk COPY loader (raw → staging → warehouse)
│   └── pipeline.py             # Main orchestrator
├── queries/
│   └── analytics.sql           # CTEs, window functions, RFM, YoY, partitioning
└── scripts/
    ├── run_pipeline.sh         # End-to-end pipeline runner
    ├── backup.sh               # pg_dump with retention policy
    └── monitor.sh              # Operational health checks
```
