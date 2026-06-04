-- TODO STUB — Lakeflow Spark Declarative Pipeline (SQL) for the insurance medallion.
-- EXPECTATIONS enforce data quality inline. Deployed via
-- resources/insurance_pipeline.pipeline.yml (serverless).
-- (Lakeflow Spark Declarative Pipelines = formerly Delta Live Tables / DLT.)
-- Exam domains 2 & 3. Studybook: docs/studybook/M5_declarative_pipelines.md
--
-- NOTE: a pipeline uses EITHER this SQL file OR insurance_dlp.py — pick one to start.

-- ----------------------------- BRONZE (streaming tables) ----------------------------- --
CREATE OR REFRESH STREAMING TABLE bronze_customers
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingest_ts
FROM STREAM read_files('/Volumes/insurance/landing/raw/customers', format => 'csv', header => true);

-- TODO: bronze_telematics (json), bronze_policies (json, multiLine => true),
--       bronze_claims (json), bronze_payments (parquet).

-- ----------------------------- SILVER (with expectations) ---------------------------- --
CREATE OR REFRESH STREAMING TABLE silver_customers
( CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_state       EXPECT (length(state) = 2) )
AS SELECT
  /* TODO: trim/standardize, map state via ref, parse dates, dedupe */
  *
FROM STREAM(LIVE.bronze_customers);

-- TODO: silver_policies (+ explode coverages), silver_claims (normalize fraud_flag), etc.

-- ----------------------------- GOLD (materialized views) ----------------------------- --
CREATE OR REFRESH MATERIALIZED VIEW gold_agg_loss_ratio AS
-- TODO: incurred losses / earned premium by month × product_line × state.
SELECT CAST(NULL AS INT) AS month_key WHERE 1 = 0;
