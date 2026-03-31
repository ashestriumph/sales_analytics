-- ============================================================
-- V003 – Staging tables (cleaned + typed + validated)
-- Applied after raw ingestion, before warehouse load
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.stg_sales (
    order_id        VARCHAR(20)    NOT NULL,
    order_date      DATE,                       -- nullable: failed DQ rows land here too
    customer_id     VARCHAR(20)    NOT NULL,
    product_id      VARCHAR(20)    NOT NULL,
    quantity        INT,                        -- nullable: CHECK enforced in warehouse only
    unit_price      NUMERIC(12,2),              -- nullable: dirty rows quarantined via _dq_passed
    discount        NUMERIC(5,4)   DEFAULT 0,
    region          VARCHAR(50),
    channel         VARCHAR(30),
    _dq_passed      BOOLEAN        DEFAULT TRUE,
    _dq_notes       TEXT,
    _loaded_at      TIMESTAMPTZ    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id      VARCHAR(20)    NOT NULL,
    product_name    VARCHAR(200)   NOT NULL,
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    unit_cost       NUMERIC(12,2),
    supplier_id     VARCHAR(20),
    _loaded_at      TIMESTAMPTZ    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id     VARCHAR(20)    NOT NULL,
    customer_name   VARCHAR(200)   NOT NULL,
    email           VARCHAR(200),
    phone           VARCHAR(50),
    city            VARCHAR(100),
    country         VARCHAR(100),
    segment         VARCHAR(50),
    _loaded_at      TIMESTAMPTZ    DEFAULT NOW()
);

INSERT INTO public.schema_migrations (version, description)
VALUES ('V003', 'Create staging tables with type casting and data quality flags');
