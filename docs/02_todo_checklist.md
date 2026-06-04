# 02 · TODO Checklist (M0–M10)

Your primary worklist. Work top to bottom. Each milestone: read the studybook chapter,
do the steps, then tick the **acceptance criteria**. `[D#]` = exam domain.

**Slice 1 ships M0–M2 in full detail. M3–M10 are outlined here and expanded (with stubs +
studybook chapters) in Slice 2.**

---

## ☐ M0 — Onboarding & platform foundations `[D1]`
**Goal:** a working Databricks Free Edition workspace + the mental model (Delta, Unity
Catalog, serverless). **Studybook:** `studybook/M0_platform_and_setup.md`.

Steps:
1. Sign up for **Free Edition**; sign in.
2. In **Catalog**, create catalog `insurance`. Note the UC hierarchy: *catalog → schema →
   table/volume*.
3. Start a **serverless** notebook and a **serverless SQL warehouse**.
4. Run `SELECT current_catalog(), current_user();` and `CREATE SCHEMA insurance.scratch;`.
5. Read the studybook chapter on Delta Lake (ACID, time travel) and compute/cost models.

**Acceptance**
- [ ] Catalog `insurance` exists; I can create/drop a schema in it.
- [ ] I ran a query on a serverless warehouse.
- [ ] I can explain in one sentence each: Delta Lake, Unity Catalog, serverless compute.

---

## ☐ M1 — Generate locally & land to a UC Volume `[D1→D2]`
**Goal:** produce the dataset locally and upload it to `insurance.landing.raw`.
**Studybook:** `studybook/M1_generate_and_land.md`.

Steps:
1. Create the venv and `pip install -r data_generator/requirements.txt`.
2. `python -m data_generator.generate --mode initial --target-mb 500 --out data/landing`.
3. Install the **Databricks CLI**; `databricks auth login`.
4. Create the schema + Volume; `databricks fs cp -r data/landing` into
   `/Volumes/insurance/landing/raw` (or run `scripts/upload_to_volume.sh`).
5. Verify with `databricks fs ls` and a notebook `LIST '/Volumes/insurance/landing/raw'`.

**Acceptance**
- [ ] `data/landing/` has all 7 source folders with multiple files each.
- [ ] The Volume `insurance.landing.raw` exists and contains the uploaded tree.
- [ ] I can `SELECT * FROM read_files('/Volumes/insurance/landing/raw/reference', format=>'csv') LIMIT 10`.

---

## ☐ M2 — Bronze: ingestion & loading `[D2]`
**Goal:** land each raw source into an append-only **bronze** Delta table with the
appropriate ingestion technique. **Studybook:** `studybook/M2_bronze_ingestion.md`.
**Worked example:** `src/bronze/load_reference.py` (reference CSVs — complete).

Steps (fill the stubs in `src/bronze/`):
1. **Reference** — run the worked example to create `bronze.ref_*` tables.
2. **Customers** (`ingest_customers_autoloader.py`) — Auto Loader (`cloudFiles`, CSV,
   `schemaEvolutionMode`), directory-listing mode.
3. **Policies/Claims** (`ingest_policies_claims.py`) — Auto Loader / `read_files` for JSON;
   `multiLine=true` for the nested policies arrays.
4. **Payments** (`ingest_payments_copyinto.sql`) — **COPY INTO** from the parquet folder.
5. **Telematics** (`ingest_telematics_stream.py`) — Auto Loader **streaming** read; add
   ingestion metadata; handle the `device_fw` schema-drift file with schema evolution.

**Acceptance**
- [ ] One `bronze.<source>` table per source, each with `_source_file` + `_ingest_ts`.
- [ ] Re-running ingestion is **idempotent** (Auto Loader/COPY INTO skip seen files).
- [ ] After loading the delta-batch-2 files, the telematics bronze table gains
      `device_fw` **without a manual schema edit** (schema evolution).
- [ ] I can explain when to use COPY INTO vs Auto Loader vs `read_files` vs Lakeflow Connect.

---

## ☐ M3 — Silver: clean & conform `[D3]`
Clean every defect in `03_data_dictionary.md`: dedup (exact + fuzzy), nulls→quarantine,
trim/casing, map categoricals via reference, fix out-of-range, parse dates, drop orphan
FKs, explode `coverages[]`, normalize `fraud_flag`, watermark telematics; build SCD2
`dim_customer` (pattern: `src/silver/clean_agents_scd2.py`). *Detailed in Slice 2.*

## ☐ M4 — Gold: dimensional model `[D3]`
Build `dim_date` (worked: `src/gold/dim_date.sql`), `dim_customer`, `dim_policy`,
`dim_agent`, `fact_claims`, `fact_premium`, `fact_payments`, and the aggregates for the 8
requirements. *Detailed in Slice 2.*

## ☐ M5 — Lakeflow Spark Declarative Pipelines `[D2,D3]`
Re-express bronze→silver→gold as a declarative pipeline with `EXPECTATIONS`; streaming
tables vs materialized views. *Detailed in Slice 2.*

## ☐ M6 — Orchestration with Lakeflow Jobs `[D4]`
Multi-task DAG (land→pipeline→DQ-test→publish), retries, conditional `run_if`, trigger
types. *Detailed in Slice 2.*

## ☐ M7 — CI/CD: Git Folders + Automation Bundles `[D5]`
Git Folders branch/commit/PR; `databricks.yml` with dev/test/prod; `databricks bundle
validate|deploy|run`. *Detailed in Slice 2.*

## ☐ M8 — Troubleshooting, Monitoring & Optimization `[D6]`
Spark UI (skew/shuffle/spill), Liquid Clustering, predictive optimization, run-history
trends, alerts on DQ metrics. *Detailed in Slice 2.*

## ☐ M9 — Governance & Security `[D7]`
Managed vs external tables; GRANT/REVOKE/DENY; column masking; row-level security; ABAC;
lineage; audit. *Detailed in Slice 2.*

## ☐ M10 — Dashboards & exam readiness `[D3,D4]`
Answer the 8 questions on a serverless SQL warehouse; final practice exam. *Detailed in
Slice 2.*
