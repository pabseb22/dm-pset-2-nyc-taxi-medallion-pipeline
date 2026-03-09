-- =========================================================
-- NYC Taxi Medallion Pipeline
-- Particionamiento Gold para PostgreSQL
-- Ejecutar antes del build Gold dentro del flujo de Mage.
-- =========================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS gold;

-- =========================================================
-- 1. FACT TABLE
-- RANGE por fecha de pickup
-- Se usa una columna generada pickup_date para mantener
-- compatibilidad con pickup_datetime.
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.fct_trips (
    trip_key BIGINT NOT NULL,
    pickup_date_key INTEGER NOT NULL,
    pickup_zone_id INTEGER NOT NULL,
    dropoff_zone_id INTEGER NOT NULL,
    service_type_key INTEGER,
    payment_type_key INTEGER,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP,
    pickup_date DATE GENERATED ALWAYS AS (pickup_datetime::date) STORED,
    trip_duration_minutes DOUBLE PRECISION,
    passenger_count NUMERIC,
    trip_distance NUMERIC,
    fare_amount NUMERIC,
    tip_amount NUMERIC,
    total_amount NUMERIC,
    source_month VARCHAR(7),
    ingest_ts TIMESTAMP,
    PRIMARY KEY (trip_key, pickup_date)
) PARTITION BY RANGE (pickup_date);

CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_01 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_02 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_03 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_04 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_05 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_06 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_07 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_08 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_09 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_10 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_11 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2024_12 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_01 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_02 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_03 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_04 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_05 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_06 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_07 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_08 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_09 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_10 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_11 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_2025_12 PARTITION OF gold.fct_trips
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS gold.fct_trips_default PARTITION OF gold.fct_trips
    DEFAULT;

CREATE INDEX IF NOT EXISTS idx_fct_trips_pickup_date_key ON gold.fct_trips (pickup_date_key);
CREATE INDEX IF NOT EXISTS idx_fct_trips_pickup_zone_id ON gold.fct_trips (pickup_zone_id);
CREATE INDEX IF NOT EXISTS idx_fct_trips_dropoff_zone_id ON gold.fct_trips (dropoff_zone_id);
CREATE INDEX IF NOT EXISTS idx_fct_trips_service_type_key ON gold.fct_trips (service_type_key);
CREATE INDEX IF NOT EXISTS idx_fct_trips_payment_type_key ON gold.fct_trips (payment_type_key);

-- =========================================================
-- 2. DIMENSION ZONE
-- HASH con 4 particiones
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.dim_zone (
    zone_id INTEGER NOT NULL,
    borough TEXT,
    zone TEXT,
    service_zone TEXT,
    PRIMARY KEY (zone_id)
) PARTITION BY HASH (zone_id);

CREATE TABLE IF NOT EXISTS gold.dim_zone_p0 PARTITION OF gold.dim_zone
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE IF NOT EXISTS gold.dim_zone_p1 PARTITION OF gold.dim_zone
    FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE IF NOT EXISTS gold.dim_zone_p2 PARTITION OF gold.dim_zone
    FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE IF NOT EXISTS gold.dim_zone_p3 PARTITION OF gold.dim_zone
    FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- =========================================================
-- 3. DIMENSION SERVICE TYPE
-- LIST por service_type
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.dim_service_type (
    service_type_key INTEGER NOT NULL,
    service_type TEXT NOT NULL,
    PRIMARY KEY (service_type_key, service_type)
) PARTITION BY LIST (service_type);

CREATE TABLE IF NOT EXISTS gold.dim_service_type_yellow PARTITION OF gold.dim_service_type
    FOR VALUES IN ('yellow');
CREATE TABLE IF NOT EXISTS gold.dim_service_type_green PARTITION OF gold.dim_service_type
    FOR VALUES IN ('green');

-- =========================================================
-- 4. DIMENSION PAYMENT TYPE
-- LIST por payment_type_desc agrupado en cash, card y other
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.dim_payment_type (
    payment_type_key INTEGER NOT NULL,
    payment_type_desc TEXT NOT NULL,
    PRIMARY KEY (payment_type_key, payment_type_desc)
) PARTITION BY LIST (payment_type_desc);

CREATE TABLE IF NOT EXISTS gold.dim_payment_type_cash PARTITION OF gold.dim_payment_type
    FOR VALUES IN ('Cash');
CREATE TABLE IF NOT EXISTS gold.dim_payment_type_card PARTITION OF gold.dim_payment_type
    FOR VALUES IN ('Credit card');
CREATE TABLE IF NOT EXISTS gold.dim_payment_type_other PARTITION OF gold.dim_payment_type
    FOR VALUES IN ('No charge', 'Dispute', 'Unknown', 'Voided trip', 'Other');

COMMIT;

-- =========================================================
-- Consultas de verificación para documentación y evidencia
-- =========================================================

-- \d+ gold.fct_trips
-- \d+ gold.dim_zone
-- \d+ gold.dim_service_type
-- \d+ gold.dim_payment_type

-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT COUNT(*)
-- FROM gold.fct_trips
-- WHERE pickup_date >= DATE '2024-01-01'
--   AND pickup_date < DATE '2024-02-01';

-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT *
-- FROM gold.dim_zone
-- WHERE zone_id = 132;
