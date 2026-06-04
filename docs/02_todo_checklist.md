# 02 · TODO Checklist (M0–M10)

Your primary worklist. Work top to bottom. Each milestone: read the studybook chapter,
do the steps, then tick the **acceptance criteria**. `[D#]` = exam domain.

Each milestone links its **studybook chapter** (`docs/studybook/`) and its **`src/` stubs**.
The worked examples (`load_reference.py`, `clean_agents_scd2.py`, `dim_date.sql`) show the
pattern; the stubs are where you apply it.

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
**Goal:** turn raw bronze into clean, conformed silver; every seeded defect handled.
**Studybook:** `studybook/M3_silver_transform.md`. **Worked:** `silver/clean_agents_scd2.py`.
**Stubs:** `silver/clean_customers.py`, `conform_policies.py`, `clean_claims.py`, `validate_expectations.py`.

Steps:
1. `clean_customers` — dedup (exact + fuzzy), trim/casing, map `state` via ref, parse dates, quarantine.
2. `conform_policies` — fix `annual_premium` (string/negative), map `status`, **explode** `coverages` → `silver.policy_coverages`, drop orphan customers.
3. `clean_claims` — normalize `fraud_flag`→boolean, map `claim_status`, flag `loss>sum_insured`, drop orphan policies.
4. `validate_expectations` — implement the pass/quarantine split + DQ counts → `ops.dq_scorecard`.
5. Telematics — **watermark** + dedupe late/out-of-order events into `silver.telematics`.

**Acceptance**
- [ ] No duplicate business keys; categories canonical; `fraud_flag` is boolean.
- [ ] Orphan rows quarantined (not dropped silently); DQ counts recorded.

## ☐ M4 — Gold: dimensional model `[D3]`
**Goal:** star schema + aggregates that answer the 8 requirements. **Studybook:** `M4`.
**Worked:** `gold/dim_date.sql`. **Stubs:** the rest of `src/gold/*.sql`.

Steps: build dims (`dim_customer`, `dim_policy`, `dim_agent` [current SCD2], `dim_date` ✓);
facts (`fact_claims`, `fact_premium`, `fact_payments`); aggregates (`agg_loss_ratio`,
`agg_claims_monthly`, `agg_agent_performance`, `agg_telematics_risk`, `agg_customer_value`);
choose **table vs view vs materialized view vs streaming table** appropriately.

**Acceptance**
- [ ] Every fact joins its dimensions; each of the 8 requirements resolves to a gold table.
- [ ] Loss ratio and fraud rate are sane (fraud ≈ 4%).

## ☐ M5 — Lakeflow Spark Declarative Pipelines `[D2,D3]`
**Goal:** re-express the medallion as ONE declarative pipeline with expectations.
**Studybook:** `M5`. **Stubs:** `src/pipelines/insurance_dlp.{sql,py}`, `resources/insurance_pipeline.pipeline.yml`.

Steps: bronze **streaming tables** (`cloudFiles`); silver streaming tables + **EXPECTATIONS**
(`expect` / `expect_or_drop` / `expect_or_fail`); gold **materialized views**; deploy via the bundle.

**Acceptance**
- [ ] Pipeline runs serverless; expectations drop/track bad rows (visible in the event log).
- [ ] Gold MVs populate from silver.

## ☐ M6 — Orchestration with Lakeflow Jobs `[D4]`
**Goal:** orchestrate the end-to-end flow as a scheduled DAG. **Studybook:** `M6`.
**Stub:** `resources/insurance_ingest.job.yml`.

Steps: tasks `land_delta → run_pipeline → dq_tests → check_dq (condition) → publish_dashboard /
notify_dq_failure`; add **retries**; pick a **trigger** (file-arrival vs cron vs table-update).

**Acceptance**
- [ ] DAG runs; the conditional branch routes on DQ pass/fail; failures email you; retries set.

## ☐ M7 — CI/CD: Git Folders + Automation Bundles `[D5]`
**Goal:** deploy the job + pipeline reproducibly across dev/test/prod. **Studybook:** `M7`.
**Files:** `databricks.yml`, `resources/*.yml`. **Cookbook:** `05_databricks_cli_cookbook.md`.

Steps: set your workspace `host`; `databricks bundle validate -t dev` → `deploy -t dev` → `run`;
practice the **Git Folders** branch/commit/PR flow; promote to `prod`.

**Acceptance**
- [ ] `bundle validate` passes; dev deploy creates the job+pipeline under your user prefix.
- [ ] You can promote to another target with one command.

## ☐ M8 — Troubleshooting, Monitoring & Optimization `[D6]`
**Goal:** read performance/reliability signals and optimize. **Studybook:** `M8`.

Steps: inspect a job's **run history**; open the **Spark UI** on the telematics⨝policies join,
spot **skew/spill**; **broadcast** the small ref dims; tune `spark.sql.shuffle.partitions`; apply
**Liquid Clustering** (`CLUSTER BY`) + `OPTIMIZE`; enable **predictive optimization**; reason about OOM/library/startup.

**Acceptance**
- [ ] You can explain a slow stage from Spark UI metrics and name the fix (AQE skew join / salt / broadcast).
- [ ] A gold table uses `CLUSTER BY`.

## ☐ M9 — Governance & Security `[D7]`
**Goal:** govern the lakehouse. **Studybook:** `M9`.

Steps: `GRANT USE CATALOG/SCHEMA` + `SELECT` on gold to an analyst group; **mask**
`customers.email`/`phone`; **row-filter** agents by region; try an **ABAC** policy; note
**managed vs external**; review **lineage** + **audit**.

**Acceptance**
- [ ] Analyst group reads gold but not PII; the row filter restricts by region.
- [ ] You can explain managed vs external and what ABAC centralizes.

## ☐ M10 — Dashboards & exam readiness `[D3,D4]`
**Goal:** publish the 6 dashboards + confirm exam readiness. **Studybook:** `M10`.
**Spec:** `dashboards/README.md`.

Steps: build the 6 dashboards on a **serverless SQL warehouse** from the gold tables; add
date/product filters; (optional) refresh via the job's **dashboard task**; take the **10-question
practice exam** in the M10 chapter.

**Acceptance**
- [ ] All 8 business questions are answered by a dashboard tile.
- [ ] You pass the practice exam confidently across all 7 domains.
