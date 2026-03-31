-- ============================================================
-- V001 – Initialize schemas (Raw / Staging / Warehouse)
-- Simulates on-premise layered data platform
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Schema migration tracking table
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version     VARCHAR(20)  NOT NULL PRIMARY KEY,
    description TEXT         NOT NULL,
    applied_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO public.schema_migrations (version, description)
VALUES ('V001', 'Initialize schemas: raw, staging, warehouse');
