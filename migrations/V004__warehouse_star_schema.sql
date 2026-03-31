-- ============================================================
-- V004 – Warehouse Star Schema (Kimball)
-- dim_date / dim_product / dim_customer / dim_region / fact_sales
-- fact_sales partitioned by year for query performance
-- ============================================================

-- ── Dimension: Date ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key        INT            PRIMARY KEY,   -- surrogate key YYYYMMDD
    full_date       DATE           NOT NULL UNIQUE,
    day_of_week     SMALLINT       NOT NULL,       -- 1=Mon ... 7=Sun
    day_name        VARCHAR(10)    NOT NULL,
    day_of_month    SMALLINT       NOT NULL,
    day_of_year     SMALLINT       NOT NULL,
    week_of_year    SMALLINT       NOT NULL,
    month_num       SMALLINT       NOT NULL,
    month_name      VARCHAR(10)    NOT NULL,
    quarter         SMALLINT       NOT NULL,
    year            SMALLINT       NOT NULL,
    is_weekend      BOOLEAN        NOT NULL,
    is_month_end    BOOLEAN        NOT NULL
);

-- ── Dimension: Product ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key     SERIAL         PRIMARY KEY,   -- surrogate key
    product_id      VARCHAR(20)    NOT NULL UNIQUE,
    product_name    VARCHAR(200)   NOT NULL,
    category        VARCHAR(100)   NOT NULL,
    sub_category    VARCHAR(100),
    unit_cost       NUMERIC(12,2),
    supplier_id     VARCHAR(20),
    effective_from  DATE           NOT NULL DEFAULT CURRENT_DATE,
    effective_to    DATE,
    is_current      BOOLEAN        NOT NULL DEFAULT TRUE
);

-- ── Dimension: Customer ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key    SERIAL         PRIMARY KEY,   -- surrogate key
    customer_id     VARCHAR(20)    NOT NULL UNIQUE,
    customer_name   VARCHAR(200)   NOT NULL,
    email           VARCHAR(200),
    city            VARCHAR(100),
    country         VARCHAR(100),
    segment         VARCHAR(50),                  -- Consumer / Corporate / SMB
    effective_from  DATE           NOT NULL DEFAULT CURRENT_DATE,
    effective_to    DATE,
    is_current      BOOLEAN        NOT NULL DEFAULT TRUE
);

-- ── Dimension: Region ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_region (
    region_key      SERIAL         PRIMARY KEY,
    region_name     VARCHAR(50)    NOT NULL UNIQUE,
    country_group   VARCHAR(50),
    timezone        VARCHAR(50)
);

-- ── Fact Table: Sales (partitioned by year) ─────────────────
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sale_id         BIGSERIAL,
    date_key        INT            NOT NULL REFERENCES warehouse.dim_date(date_key),
    product_key     INT            NOT NULL REFERENCES warehouse.dim_product(product_key),
    customer_key    INT            NOT NULL REFERENCES warehouse.dim_customer(customer_key),
    region_key      INT            REFERENCES warehouse.dim_region(region_key),
    order_id        VARCHAR(20)    NOT NULL,
    channel         VARCHAR(30),
    quantity        INT            NOT NULL,
    unit_price      NUMERIC(12,2)  NOT NULL,
    unit_cost       NUMERIC(12,2),
    discount        NUMERIC(5,4)   NOT NULL DEFAULT 0,
    gross_amount    NUMERIC(14,2)  GENERATED ALWAYS AS (quantity * unit_price) STORED,
    net_amount      NUMERIC(14,2)  GENERATED ALWAYS AS (quantity * unit_price * (1 - discount)) STORED,
    gross_margin    NUMERIC(14,2)  GENERATED ALWAYS AS (quantity * (unit_price - COALESCE(unit_cost, 0))) STORED,
    _loaded_at      TIMESTAMPTZ    DEFAULT NOW(),
    PRIMARY KEY (sale_id, date_key)
) PARTITION BY RANGE (date_key);

-- Partitions by year
CREATE TABLE IF NOT EXISTS warehouse.fact_sales_2022
    PARTITION OF warehouse.fact_sales
    FOR VALUES FROM (20220101) TO (20230101);

CREATE TABLE IF NOT EXISTS warehouse.fact_sales_2023
    PARTITION OF warehouse.fact_sales
    FOR VALUES FROM (20230101) TO (20240101);

CREATE TABLE IF NOT EXISTS warehouse.fact_sales_2024
    PARTITION OF warehouse.fact_sales
    FOR VALUES FROM (20240101) TO (20250101);

CREATE TABLE IF NOT EXISTS warehouse.fact_sales_2025
    PARTITION OF warehouse.fact_sales
    FOR VALUES FROM (20250101) TO (20260101);

INSERT INTO public.schema_migrations (version, description)
VALUES ('V004', 'Create Kimball Star Schema: dim_date, dim_product, dim_customer, dim_region, fact_sales partitioned by year');
