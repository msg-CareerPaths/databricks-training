# 3. Bronze — Ingestion & Loading [4-6 hours]

_Milestone M2 · Exam domain 2: Data Ingestion and Loading_

**Goal:**
- Land each raw source into an append-only **bronze** Delta table with the right tool.
- Practise **Auto Loader** (+ schema evolution), **COPY INTO**, and **`read_files`** /
  `multiLine`, and know where **Lakeflow Connect** fits.

## Mandatory Materials:

**Videos:**
- Databricks Academy — *Data Ingestion with Lakeflow Connect*

**Reading:**
 - [Studybook M2 — Bronze Ingestion](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M2_bronze_ingestion.md)
 - [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html) · [COPY INTO](https://docs.databricks.com/en/ingestion/copy-into/index.html)
 - Worked example: [src/bronze/load_reference.py](https://github.com/msg-CareerPaths/databricks-training/blob/main/src/bronze/load_reference.py)

## Insurance Lakehouse:
 > 1. **Reference** — run the worked example `src/bronze/load_reference.py` to create the
 >    `bronze.ref_*` tables (batch `read_files`).
 > 2. **Customers** — complete `src/bronze/ingest_customers_autoloader.py`: Auto Loader
 >    (`cloudFiles`, CSV) with `schemaEvolutionMode = addNewColumns`, directory-listing mode.
 > 3. **Policies / Claims** — complete `src/bronze/ingest_policies_claims.py`: JSON via Auto
 >    Loader / `read_files`; `multiLine => true` for the nested policies.
 > 4. **Payments** — complete `src/bronze/ingest_payments_copyinto.sql`: **COPY INTO** the Parquet.
 > 5. **Telematics** — complete `src/bronze/ingest_telematics_stream.py`: Auto Loader streaming;
 >    after uploading delta batch 2, confirm `device_fw` appears automatically.
 >
 > **Acceptance:** one `bronze.<source>` table per source with `_source_file` + `_ingest_ts`;
 > re-running is idempotent; schema evolution picks up `device_fw` with no manual edit; you can
 > explain when to use COPY INTO vs Auto Loader vs `read_files` vs Lakeflow Connect.

## Further Resources:
- [`read_files`](https://docs.databricks.com/en/sql/language-manual/functions/read_files.html) · [Medallion architecture](https://docs.databricks.com/en/lakehouse/medallion.html)
