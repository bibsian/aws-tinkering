-- =============================================================
-- Pattern B: Apache Iceberg table via AWS Glue + Redshift Spectrum
-- =============================================================
-- The same three columns are exposed as a single logical table.
-- Iceberg handles partition management automatically; no view
-- maintenance is required when new snapshot dates arrive.
-- =============================================================

-- Step 1: External schema pointing to the Glue Data Catalog
-- (Idempotent — safe to re-run)
CREATE EXTERNAL SCHEMA IF NOT EXISTS benchmark_iceberg
FROM DATA CATALOG
DATABASE '{glue_database}'
IAM_ROLE '{iam_role_arn}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;


-- Step 2: Create the Iceberg table
-- PARTITIONED BY partition_month mirrors the UNION ALL split in Pattern A,
-- letting us isolate the partition-management overhead as a variable.
--
-- IMPORTANT: Requires Redshift ra3.* or Serverless.
-- The table_format=ICEBERG property triggers Iceberg metadata generation.
--
-- Column type mapping (Parquet → Spectrum):
--   VARCHAR(20) → STRING   (Spectrum uses Hive-compatible types)
--   DATE        → DATE

CREATE EXTERNAL TABLE IF NOT EXISTS benchmark_iceberg.prop_snapshot_iceberg (
    prop_cd         STRING,
    snapshot_date   DATE,
    extract_date    DATE
)
PARTITIONED BY (partition_month VARCHAR(7))  -- e.g. '2024-01'
STORED AS PARQUET
LOCATION 's3://{s3_bucket}/{s3_prefix}/'
TABLE PROPERTIES ('table_type'='ICEBERG');

-- ── Partition note ─────────────────────────────────────────────
-- We partition by a VARCHAR partition_month ('YYYY-MM') rather than
-- snapshot_date directly. This matches how the union-view pattern
-- splits months into separate tables, making the comparison fair.
-- Iceberg stores one Parquet file per (partition_month, prop_cd chunk).

-- Step 3: Refresh after data load
-- Iceberg auto-discovers new snapshots, but MSCK REPAIR ensures
-- Spectrum reflects all files written by the Python data generator.
MSCK REPAIR TABLE benchmark_iceberg.prop_snapshot_iceberg;
