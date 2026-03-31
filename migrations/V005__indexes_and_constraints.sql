-- ============================================================
-- V005 – Indexes & performance optimization
-- Covers: lookup indexes on dims, composite indexes on fact
-- ============================================================

-- dim_product
CREATE INDEX IF NOT EXISTS idx_dim_product_category
    ON warehouse.dim_product (category);

CREATE INDEX IF NOT EXISTS idx_dim_product_current
    ON warehouse.dim_product (is_current) WHERE is_current = TRUE;

-- dim_customer
CREATE INDEX IF NOT EXISTS idx_dim_customer_segment
    ON warehouse.dim_customer (segment);

CREATE INDEX IF NOT EXISTS idx_dim_customer_country
    ON warehouse.dim_customer (country);

-- dim_date
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month
    ON warehouse.dim_date (year, month_num);

CREATE INDEX IF NOT EXISTS idx_dim_date_quarter
    ON warehouse.dim_date (year, quarter);

-- fact_sales (most critical)
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_product
    ON warehouse.fact_sales (date_key, product_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer
    ON warehouse.fact_sales (customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_channel
    ON warehouse.fact_sales (channel);

-- staging quality flag
CREATE INDEX IF NOT EXISTS idx_stg_sales_dq
    ON staging.stg_sales (_dq_passed);

INSERT INTO public.schema_migrations (version, description)
VALUES ('V005', 'Add performance indexes on warehouse dimensions and fact table');
