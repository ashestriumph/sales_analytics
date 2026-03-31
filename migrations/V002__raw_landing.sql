-- ============================================================
-- V002 – Raw landing tables (exact copy of source data)
-- No transformations – append-only, audit columns only
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.sales (
    _id             SERIAL,
    order_id        TEXT,
    order_date      TEXT,
    customer_id     TEXT,
    product_id      TEXT,
    quantity        TEXT,
    unit_price      TEXT,
    discount        TEXT,
    region          TEXT,
    channel         TEXT,
    _source_file    TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.products (
    _id             SERIAL,
    product_id      TEXT,
    product_name    TEXT,
    category        TEXT,
    sub_category    TEXT,
    unit_cost       TEXT,
    supplier_id     TEXT,
    _source_file    TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.customers (
    _id             SERIAL,
    customer_id     TEXT,
    customer_name   TEXT,
    email           TEXT,
    phone           TEXT,
    city            TEXT,
    country         TEXT,
    segment         TEXT,
    _source_file    TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO public.schema_migrations (version, description)
VALUES ('V002', 'Create raw landing tables: sales, products, customers');
