# Technical Overview — Sales Analytics Data Platform

## Context

This document walks through the key architectural and technical decisions behind this project. It is intended to complement the code and give context on *why* things were built the way they were, not just *what* was built.

The project simulates a real on-premise data platform for a company that needs to consolidate sales data from multiple source systems and make it available for analytical reporting. The constraints are typical of enterprise on-premise environments: no managed cloud services, physical servers running Linux, heterogeneous source formats, and a need for operational reliability.

---

## 1. Architecture — Why Three Layers?

The platform follows a **Raw → Staging → Warehouse** layered architecture. This is a deliberate choice, not a default one.

```
Source Files          Raw Layer          Staging Layer        Warehouse Layer
(CSV / JSON / XML) →  (schema: raw)  →  (schema: staging) →  (schema: warehouse)
```

**Raw layer** exists for one reason: auditability. Every record that enters the system is stored exactly as received, with no transformation. If something breaks downstream — bad data, a bug in a transformation, a schema change in the source — we can always replay from raw. On on-premise systems where you don't have object storage with infinite retention, this layer is your safety net.

**Staging layer** is where we apply typing, cleaning, and data quality checks. Critically, records that fail quality checks are *not* removed — they are flagged with `_dq_passed = FALSE` and a human-readable `_dq_notes` message. This means:
- We have a full audit trail of every bad record and why it was rejected
- Analysts can query quarantined records to investigate source data issues
- Nothing is silently dropped

**Warehouse layer** contains only DQ-passed, fully typed, analytics-ready data. It is the only layer that business users and BI tools should query.

This pattern avoids the most common failure mode I've seen in simpler pipelines: transforming data in-flight and losing the ability to debug when something goes wrong.

---

## 2. Data Modeling — Why Kimball Star Schema?

The warehouse follows the **Kimball dimensional modeling** approach (Star Schema). The alternative would be Inmon (3NF) or Data Vault, but for this use case, Kimball is the right choice.

Here is the reasoning:

| Criterion | Kimball (Star) | Inmon (3NF) | Data Vault |
|---|---|---|---|
| Query performance | High — shallow joins | Lower — many joins | Moderate |
| Model complexity | Low | High | Very high |
| Best for | Analytics / BI | OLTP-style queries | Audit-heavy, many sources |
| Learning curve | Low | Medium | High |

For a sales analytics platform where the primary use case is aggregations (revenue by period, by category, by customer segment), a Star Schema gives the best query performance with the least join depth. A BI tool or analyst writing SQL can understand the model intuitively: one fact table in the center, dimensions around it.

### The fact table design

`fact_sales` contains one row per order line. The key design decisions:

**Surrogate keys instead of natural keys.** `product_key` (INT) is used as the FK to `dim_product`, not `product_id` (VARCHAR). This is essential for SCD Type 2: if a product's category changes, we can add a new row in `dim_product` with a new surrogate key, while historical facts continue to point to the old version. The natural key alone cannot support this.

**Generated columns for derived metrics.** `gross_amount`, `net_amount`, and `gross_margin` are defined as `GENERATED ALWAYS AS` columns in PostgreSQL. This guarantees that these values are always mathematically consistent with the base columns — no application-level bug can produce an incorrect margin. The calculation is enforced at the database level.

```sql
net_amount   NUMERIC(14,2) GENERATED ALWAYS AS (quantity * unit_price * (1 - discount)) STORED,
gross_margin NUMERIC(14,2) GENERATED ALWAYS AS (quantity * (unit_price - COALESCE(unit_cost, 0))) STORED
```

**SCD Type 2 readiness.** Both `dim_product` and `dim_customer` include `effective_from`, `effective_to`, and `is_current` columns. The current implementation is SCD Type 1 (overwrite), but the schema is already designed to support Type 2 (full history) without migration.

---

## 3. Partitioning — Why and How

`fact_sales` is range-partitioned by `date_key` (year):

```sql
CREATE TABLE warehouse.fact_sales PARTITION BY RANGE (date_key);
-- fact_sales_2022, fact_sales_2023, fact_sales_2024, fact_sales_2025
```

The primary benefit is **partition pruning**: when a query filters by year, PostgreSQL's planner excludes all other partitions from the scan entirely. You can verify this with `EXPLAIN (ANALYZE, BUFFERS)`:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT SUM(net_amount) FROM warehouse.fact_sales
WHERE date_key BETWEEN 20240101 AND 20241231;
-- → Seq Scan on fact_sales_2024 (partitions 2022, 2023, 2025 not touched)
```

On a production table with hundreds of millions of rows, this is the difference between a 30-second query and a 2-second query.

The choice of `date_key` as an INT (YYYYMMDD format) rather than a DATE type is deliberate: integer comparisons are faster than date comparisons, and it doubles as a readable surrogate key for `dim_date`.

---

## 4. ETL Pipeline Design

The pipeline is split into four modules with clear responsibilities:

- `extract.py` — reads source files, returns raw DataFrames with no transformation
- `quality.py` — applies DQ rules, returns (passed_df, failed_df, report)
- `transform.py` — type casting, string normalization, date spine generation
- `load.py` — writes to PostgreSQL using bulk `COPY FROM`

### Why COPY FROM instead of INSERT?

PostgreSQL's `COPY FROM` is significantly faster than individual `INSERT` statements for bulk loading:

- It bypasses WAL logging overhead per-row
- It minimizes round-trips between Python and PostgreSQL
- On a server with spinning disks (common in on-premise environments), this matters

For 5,000 rows, the difference is small. For 500,000+ rows in a real pipeline, `COPY FROM` can be 10-50x faster than row-by-row inserts.

### Surrogate key resolution in SQL, not Python

The final step — loading `fact_sales` — resolves surrogate keys via a SQL `JOIN` rather than doing the lookup in Python:

```sql
INSERT INTO warehouse.fact_sales (...)
SELECT dd.date_key, dp.product_key, dc.customer_key, ...
FROM staging.stg_sales s
JOIN warehouse.dim_date     dd ON dd.full_date   = s.order_date
JOIN warehouse.dim_product  dp ON dp.product_id  = s.product_id AND dp.is_current
JOIN warehouse.dim_customer dc ON dc.customer_id = s.customer_id AND dc.is_current
WHERE s._dq_passed = TRUE;
```

Doing this in Python would require loading all dimension tables into memory and doing DataFrame merges. Letting the database handle it is more efficient and keeps the logic where it belongs — in the system designed for set-based operations.

---

## 5. Multi-Format Ingestion

The pipeline handles three source formats, reflecting realistic enterprise environments:

| File | Format | Source system |
|---|---|---|
| `sales.csv` | CSV | Transactional database export |
| `products.json` | JSON | Internal product catalog API |
| `customers.xml` | XML | CRM system export |

XML is handled with Python's `xml.etree.ElementTree`. The parsing logic in `extract.py` is explicit — each field is pulled by tag name — which makes it robust to extra tags or attribute changes in the XML structure.

All three extractors follow the same contract: they return a raw DataFrame with no type casting. Casting happens exclusively in `transform.py`. This separation means you can test extraction and transformation independently.

---

## 6. Data Quality

Four rules are applied in `quality.py`:

| Rule | Column | Condition | Typical cause |
|---|---|---|---|
| Valid date | `order_date` | Parseable as ISO date | Empty string, wrong format |
| Positive quantity | `quantity` | > 0 | Data entry error, system bug |
| Non-negative price | `unit_price` | ≥ 0 | Negative credit entries in source |
| Non-null FK | `customer_id`, `product_id` | Not null | Orphaned records in source OLTP |

The test dataset deliberately injects ~3% invalid rows to demonstrate detection. In a production run, the DQ report gives you a pass rate you can alert on — if it drops below a threshold, something changed in the source.

---

## 7. Schema Versioning

Migrations follow the Flyway naming convention: `VXXX__description.sql`. A tracking table records what has been applied:

```sql
SELECT * FROM public.schema_migrations ORDER BY version;
-- V001 | Initialize schemas
-- V002 | Create raw landing tables
-- V003 | Create staging tables
-- V004 | Create Kimball Star Schema with partitioning
-- V005 | Add performance indexes
```

Migrations are **append-only and irreversible**. If you need to change a table, you write `V006__alter_table.sql` — you never modify an existing migration. This is critical on on-premise systems where you may have multiple environments (dev/staging/prod) that need to stay in sync without automated rollout tooling.

---

## 8. Indexes

Indexes are defined in `V005` and cover the most common access patterns:

```sql
-- Most common analytical filter: by year/month
CREATE INDEX idx_dim_date_year_month ON warehouse.dim_date (year, month_num);

-- Fact table: date + product is the most common combination in GROUP BY queries
CREATE INDEX idx_fact_sales_date_product ON warehouse.fact_sales (date_key, product_key);

-- Partial index: only index current dimension records (avoids indexing historical SCD rows)
CREATE INDEX idx_dim_product_current ON warehouse.dim_product (is_current) WHERE is_current = TRUE;
```

The partial index on `is_current` is worth noting: in a mature SCD Type 2 dimension, most rows are historical (`is_current = FALSE`). Indexing only current rows keeps the index small and fast.

---

## 9. Operational Scripts

Three Bash scripts cover the operational lifecycle:

- `run_pipeline.sh` — starts the database, generates data, runs ETL. Designed to be called from cron or an orchestrator.
- `backup.sh` — runs `pg_dump` with a 7-day retention policy. Suitable for a daily cron job at off-peak hours.
- `monitor.sh` — checks row counts per layer, DQ pass rate, and partition distribution. Can be integrated with any alerting system that parses stdout.

---

## What I Would Add in Production

To be transparent about the scope of this project, here is what a full production implementation would add:

- **Apache Airflow** for pipeline orchestration (retry logic, dependency management, scheduling)
- **dbt** for the staging → warehouse transformation layer (SQL-first, testable, documented)
- **SCD Type 2** handling in the dimension loaders (currently Type 1)
- **Incremental loading** — the current pipeline reloads all data; production would load only new/changed records
- **Data lineage tooling** (OpenLineage or similar) to automate lineage tracking beyond the manual documentation here
- **Alerting** on DQ pass rate drops, pipeline failures, and partition row count anomalies
