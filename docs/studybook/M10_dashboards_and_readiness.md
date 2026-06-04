# M10 · AI/BI Dashboards & Exam Readiness

> **Exam domains 3 & 4.** This final chapter publishes the gold layer to the business as
> **AI/BI Dashboards** running on a **serverless SQL warehouse**, then schedules the
> refresh as a **dashboard task in a Lakeflow Job**. Domain 3 is *building gold objects for
> BI*; domain 4 is *orchestrating the dashboard refresh*. The chapter closes with a
> 10-question, all-domains practice exam and a before-the-exam checklist.

## 1 · Concept / theory

### AI/BI Dashboards
A **Databricks AI/BI Dashboard** (the current name; formerly *Lakeview*) is a UC-governed
asset with three building blocks:

- **Datasets** — the SQL queries behind the dashboard. Each dataset is a named query
  against your **gold** tables (e.g. `SELECT … FROM insurance.gold.agg_loss_ratio`). A
  dataset is computed once and **shared by every visualization** that references it, so you
  push the heavy aggregation into gold (M4) and keep dataset SQL thin.
- **Visualizations (tiles)** — bar / line / counter / table / map / pie etc., each bound to
  a dataset and to fields (dimensions on one axis, measures on the other). Tiles live on a
  drag-and-drop **canvas**.
- **Filters (widgets)** — cross-tile controls (date range, product line, state, segment).
  One filter widget can drive many tiles because they share datasets, so a single
  `product_line` filter updates loss ratio, fraud, and premium tiles at once.

A dashboard has a **Draft** and a **Published** version; you share the **published** one.
Sharing is governed by Unity Catalog plus the dashboard's own permissions, and **viewers
query through the dashboard's credentials**, so a viewer needs no direct grant on the gold
tables (see M9 and the *embedded credentials* pitfall below).

There is also a **Genie / AI** side (natural-language Q&A over the same datasets); the
exam only expects you to know it exists, not to build one.

### Serverless SQL warehouse — the compute behind dashboards
Dashboards (and the SQL editor and Genie) run on a **SQL warehouse**, not on a notebook
cluster. A **serverless SQL warehouse**:

- **starts in seconds** and **autoscales** to many concurrent analysts — the right compute
  for bursty, ad-hoc BI (this is exactly the guide's sample Q4 answer);
- is the **only** warehouse type on **Free Edition** (serverless-only);
- bills for uptime, so it **auto-stops** after an idle period — leave auto-stop on to
  protect the FE budget.

Choosing compute is a domain-1 reflex: serverless SQL warehouse for BI/dashboards, **job
compute** for scheduled ETL, all-purpose only for interactive dev.

### Scheduling a refresh — the Lakeflow Jobs *dashboard task*
A published dashboard can refresh on its **own schedule** (set on the dashboard), **but**
the robust pattern — and the one the exam tests — is to refresh it **as a step in a
Lakeflow Job** so it runs *after* the data that feeds it.

- A **Lakeflow Job** is a DAG of **tasks**. One task **type** is **Dashboard** (alongside
  Notebook, SQL, Pipeline, etc.). The dashboard task **refreshes a chosen AI/BI dashboard**
  on a SQL warehouse you pick.
- Put the dashboard task **downstream** of the pipeline/publish task (a `depends_on` edge),
  so dashboards only refresh once gold is rebuilt — no stale-data races.
- The **job** carries the **schedule/trigger** (cron, or file-arrival), **retries**, and
  conditional `run_if`. Built-in alerts/notifications fire on success or failure.

So the end-to-end chain is: **land → Lakeflow Declarative Pipeline (bronze→silver→gold) →
DQ test → dashboard task** — all one scheduled Lakeflow Job (this is the M6 DAG with a
final BI step).

### Our 6 dashboards (8 requirements → 6 dashboards)
Built on the gold tables from M4; each tile names its **gold source table + chart type**.

| Dashboard | Reqs | Key tiles (gold source → chart) |
|---|---|---|
| **Executive Loss & Premium** | 1, 4 | monthly loss ratio (`agg_loss_ratio` → **line**); loss ratio by state (`agg_loss_ratio` → **map**/bar); written vs collected premium (`fact_premium`, `fact_payments` → **combo bar**); overdue exposure % (`fact_payments` → **counter**) |
| **Claims & Fraud** | 2, 3 | claim frequency trend (`agg_claims_monthly` → **line**); severity by peril (`agg_claims_monthly`+`ref_peril_codes` → **bar**); fraud rate over time (`fact_claims` → **line**); flagged-dollar exposure (`fact_claims` → **counter**) |
| **Distribution / Agents** | 5 | policies sold by agent (`agg_agent_performance`+`dim_agent` → **bar**); retention % leaderboard (`agg_agent_performance` → **table**); loss ratio per agent (`agg_agent_performance` → **bar**) |
| **Telematics Risk** | 6 | claim rate by harsh-score quintile (`agg_telematics_risk` → **bar**); risk-score distribution (`agg_telematics_risk` → **histogram**); score vs loss scatter (`agg_telematics_risk` → **scatter**) |
| **Customer & Retention** | 7 | active customers by segment (`dim_customer` → **pie**); lifetime premium by tenure band (`agg_customer_value` → **bar**); churn-flag counter (`agg_customer_value` → **counter**) |
| **Data Quality** | 8 | rows passed vs quarantined per layer (`ops.dq_scorecard` → **stacked bar**); quarantine rate by rule (`ops.dq_scorecard` → **table**); source freshness (`ops.dq_scorecard` → **counter**) |

All six share **date / product_line / state** filter widgets where the grain allows.

## 2 · Worked code

Datasets are just SQL over gold — keep them thin and let M4's aggregates do the work.

**Dataset: monthly loss ratio (Executive tile, req 1).**
```sql
-- powers the "Monthly loss ratio" line tile (x = year_month, y = loss_ratio)
SELECT
  d.year_month,
  p.product_line,
  p.state,                                            -- risk state carried on the policy dim
  SUM(a.incurred_losses)                              AS incurred_losses,
  SUM(a.earned_premium)                               AS earned_premium,
  ROUND(SUM(a.incurred_losses) / NULLIF(SUM(a.earned_premium), 0), 4) AS loss_ratio
FROM insurance.gold.agg_loss_ratio a
JOIN insurance.gold.dim_date    d ON a.date_key   = d.date_key
JOIN insurance.gold.dim_policy  p ON a.policy_key = p.policy_key
GROUP BY d.year_month, p.product_line, p.state
ORDER BY d.year_month;          -- NULLIF guards divide-by-zero (a real exam trap)
```

**Dataset: monthly fraud rate (Claims & Fraud tile, req 3).**
```sql
-- fraud_flag is a CLEAN boolean by gold (silver normalized Y/N/1/0/YES → boolean)
SELECT
  d.year_month,
  COUNT(*)                                              AS claims,
  SUM(CASE WHEN f.fraud_flag THEN 1 ELSE 0 END)         AS fraud_claims,
  ROUND(AVG(CASE WHEN f.fraud_flag THEN 1.0 ELSE 0 END), 4) AS fraud_rate,
  SUM(CASE WHEN f.fraud_flag THEN f.loss_amount ELSE 0 END) AS flagged_dollars
FROM insurance.gold.fact_claims f
JOIN insurance.gold.dim_date d ON f.loss_date_key = d.date_key
GROUP BY d.year_month
ORDER BY d.year_month;          -- expect fraud_rate ≈ 0.04 (the seeded rate)
```

**Create a serverless SQL warehouse + dashboard refresh from the CLI** (the bundle/CLI
ties M7 to this milestone):
```bash
# a small serverless warehouse for BI, auto-stops after 10 min idle
databricks warehouses create --json '{
  "name": "bi-serverless", "warehouse_type": "PRO",
  "enable_serverless_compute": true, "cluster_size": "2X-Small",
  "auto_stop_mins": 10 }'

# refresh a published dashboard on demand (the Job dashboard-task does this on a schedule)
databricks lakeview-dashboards get  --dashboard-id <id>      # find it
# (Schedule the refresh by adding a Dashboard task to a Lakeflow Job — see below.)
```

**Add the dashboard refresh as a task in a Lakeflow Job** (`databricks.yml` bundle
fragment — downstream of the gold pipeline, domain 4 + 5):
```yaml
resources:
  jobs:
    insurance_medallion:
      name: insurance-medallion
      schedule: { quartz_cron_expression: "0 0 6 * * ?", timezone_id: "UTC" }  # daily 06:00
      tasks:
        - task_key: build_gold            # the Declarative Pipeline (M5)
          pipeline_task: { pipeline_id: ${resources.pipelines.medallion.id} }
        - task_key: refresh_dashboards
          depends_on: [{ task_key: build_gold }]      # only after gold is rebuilt
          dashboard_task:
            dashboard_id: ${var.exec_dashboard_id}
            warehouse_id: ${var.bi_warehouse_id}      # the serverless SQL warehouse
```

## 3 · Best practices & pitfalls
- **Aggregate in gold, not in the dataset.** A dashboard dataset should select from a
  pre-aggregated gold table (`agg_*`); don't re-join silver in the tile. Datasets are
  shared by every tile, so one heavy query taxes the whole dashboard.
- **Guard divide-by-zero** in ratio tiles with `NULLIF(denominator, 0)` — loss ratio and
  fraud rate both divide, and a zero denominator throws or shows ∞.
- **Refresh order matters.** Make the **dashboard task `depends_on` the pipeline/publish
  task**; a dashboard on its own timer can refresh mid-rebuild and show torn data.
- **Leave auto-stop on** for the serverless SQL warehouse — an always-on warehouse silently
  drains the Free Edition budget. Right-size to **2X-Small** for this dataset.
- **Publish, then share.** Viewers see the *published* version and query via the
  dashboard's embedded credentials, so they need **no direct grant** on gold — but that
  also means a viewer can see *aggregated* data they couldn't query directly. Aggregate or
  apply masking/RLS in gold (M9) for anything sensitive (PII like `email`, `phone`).
- **One filter, many tiles.** Shared datasets let a single `product_line`/`state`/date
  widget drive the whole canvas — design tiles on a common grain (`year_month`,
  `product_line`, `state`) so filters apply everywhere.
- **Don't confuse the task types.** A **Dashboard** task refreshes an AI/BI dashboard; a
  **SQL** task runs a query/alert; a **Pipeline** task runs a Declarative Pipeline. The
  exam will offer all three as distractors.

## 4 · Exam focus
**Domain 3 objective:** create **gold** objects (tables/views) in Unity Catalog for BI and
analytics teams — the `agg_*`/`fact_*`/`dim_*` your datasets read.
**Domain 4 objective:** build a **Lakeflow Job** with multiple **task types** — here the
**Dashboard task** that refreshes an AI/BI dashboard on a SQL warehouse, ordered after the
data step via `depends_on`.

**Practice questions**
1. *You need fast-starting, autoscaling compute for many analysts hitting curated gold
   tables in dashboards all day. Which compute?* **A.** A **serverless SQL warehouse**
   (seconds to start, scales with concurrency; the only type on Free Edition). A big
   all-purpose cluster wastes money; a single-node dev cluster can't serve many users.
2. *A dashboard must refresh only after the nightly gold rebuild finishes. Best approach?*
   **A.** Add a **Dashboard task** to the **Lakeflow Job** with `depends_on` the
   pipeline/publish task — not a standalone dashboard timer (which can refresh mid-rebuild).
3. *Several tiles show the same aggregation sliced differently and you want one place to
   define it.* **A.** Define one **dataset** (shared query) and point multiple
   visualizations + a shared filter at it — don't write per-tile SQL.
4. *Where should the loss-ratio aggregation live?* **A.** In a **gold** table
   (`agg_loss_ratio`) built in M4; the dataset just `SELECT`s it (thin dataset, fast tile).

## 5 · References
- **AI/BI Dashboards** — datasets, visualizations, filters; draft vs published; sharing &
  embedded credentials
- **Serverless SQL warehouses** — sizing, auto-stop, concurrency, Free Edition
- **Lakeflow Jobs** — task types incl. the **Dashboard task**, `depends_on`, schedules/
  triggers, retries, notifications
- **Unity Catalog** governance for BI assets; masking/RLS for PII in gold (M9)
- The Medallion architecture — gold objects for BI (M4)

---

## Final exam readiness

You've built the whole lakehouse. This is a **mixed 45→10-scale practice exam** spanning
**all 7 domains** at associate difficulty. Answer first, *then* read the key.

### 10-question practice exam (all domains)

**Q1 (D1 · Platform).** A team wants rapid pipeline iteration, reliable rollback after a
bad load, an audit trail, and **one** governed copy of data for BI *and* AI. Best strategy?
- A. Store CSVs in a bucket and copy them manually per environment
- B. Keep everything in Spark DataFrames in memory and re-run on failure
- C. **Delta Lake** (ACID + time travel) governed by **Unity Catalog**
- D. A separate data warehouse for BI and a separate data lake for AI

**Q2 (D2 · Ingestion).** CSV files land in a Volume throughout the day and *occasionally
gain a new column*. You need exactly-once, incremental ingestion that tolerates the new
column. Which tool + option?
- A. `read_files(...)` in a scheduled query
- B. **Auto Loader** with `cloudFiles.schemaEvolutionMode = addNewColumns` + per-source `schemaLocation`
- C. `COPY INTO` with `FORMAT_OPTIONS('mergeSchema'='true')` only
- D. A nightly `CREATE OR REPLACE TABLE … SELECT * FROM read_files(...)`

**Q3 (D2 · Ingestion).** You periodically bulk-load **Parquet** with a *stable* schema from
object storage into an existing Delta table, and want re-runs to **skip already-loaded
files**. Best tool?
- A. Auto Loader file-notification mode
- B. `read_files` with `multiLine => true`
- C. **`COPY INTO`** (idempotent, file-level batch load)
- D. `INSERT INTO … SELECT * FROM parquet.\`/path\``

**Q4 (D3 · Transformation).** The raw `fraud_flag` is a mix of `Y/N`, `1/0`, `YES/NO`,
`true/false`. The gold fraud-rate tile must compute a clean rate. Where/how do you fix it?
- A. In the dashboard dataset with a long `CASE` per query
- B. In **silver**, normalize to a real **boolean**, then aggregate in gold
- C. Leave it as a string and `COUNT(*) WHERE fraud_flag = 'Y'`
- D. Drop every row whose `fraud_flag` isn't already boolean

**Q5 (D3 · Modeling).** You must keep **full history** of agent attribute changes (a new row
when `status`/`branch` changes) so historical loss ratios attribute to the agent version in
force then. Which pattern + operation?
- A. Overwrite the dimension each load (SCD Type 1)
- B. **SCD Type 2** maintained with **`MERGE INTO`** (close old row, insert new, effective dates)
- C. Append every delta with no keys
- D. A view that always shows the latest row only

**Q6 (D4 · Lakeflow Jobs).** A multi-task Job must run **land → pipeline → DQ test →
publish dashboard**, and the *publish* step should run **only if the DQ test passes**. What
do you configure?
- A. Four unrelated jobs on the same cron
- B. One Job DAG with `depends_on` edges and a conditional **`run_if`** on the publish task
- C. A single notebook that does all four sequentially with `try/except`
- D. Put the dashboard on its own timer, unrelated to the job

**Q7 (D4 · Lakeflow Jobs).** Which task type **refreshes an AI/BI dashboard** as a step in a
Lakeflow Job?
- A. A **SQL** task (alert)
- B. A **Notebook** task
- C. A **Pipeline** task
- D. A **Dashboard** task (on a chosen SQL warehouse)

**Q8 (D5 · CI/CD).** Your team wants the same medallion deployed to **dev/test/prod** from
version-controlled config, promoted via the CLI. Which Databricks feature?
- A. Export/import notebooks by hand per environment
- B. **Declarative Automation Bundles** (`databricks.yml` with targets) + `databricks bundle deploy`
- C. Copy-paste jobs in the Workspace UI for each environment
- D. A shell script that `curl`s the REST API with hard-coded IDs

**Q9 (D6 · Optimization).** A gold aggregation is slow; the Spark UI shows a few tasks
running far longer than the rest with **disk spill** during a wide join. Most likely cause
and a fitting remedy?
- A. Too few columns — add more columns
- B. **Data skew**; mitigate with better partitioning / **Liquid Clustering** on the join key (and let predictive optimization compact)
- C. Time travel is enabled — disable it
- D. The table is Delta — convert it to CSV

**Q10 (D7 · Governance).** Analysts may see **aggregated** customer metrics but must **not**
read raw `email`/`phone`. They query a serverless SQL warehouse and a dashboard. What
enforces this in Unity Catalog?
- A. Tell them not to select those columns
- B. A **column mask** (and/or RLS) on the PII columns + `GRANT SELECT` only on the gold aggregates
- C. `DENY ALL` on the whole catalog
- D. Put the data in DBFS root so UC doesn't apply

### Answer key (with 1-line rationales)
1. **C** — ACID + time travel give rollback/iteration; UC gives governance, lineage, and one governed copy for BI **and** AI.
2. **B** — Auto Loader is exactly-once + incremental; `addNewColumns` evolves the schema; each source needs its own `schemaLocation`.
3. **C** — `COPY INTO` is the idempotent, file-level **batch** loader for a stable schema and tracks loaded files so re-runs skip them.
4. **B** — Clean once in **silver** to a real boolean (defects belong in silver), then gold/dataset just aggregate; expect ≈4%.
5. **B** — Keeping history = **SCD Type 2**, implemented with **`MERGE INTO`** (close the current row, insert the new one with effective dates).
6. **B** — One Job DAG expresses order via `depends_on`; the conditional **`run_if`** gates the publish task on the DQ result.
7. **D** — The **Dashboard task** refreshes an AI/BI dashboard on a chosen SQL warehouse; SQL/Notebook/Pipeline tasks do other work.
8. **B** — **Declarative Automation Bundles** version `databricks.yml` with dev/test/prod **targets** and deploy via `databricks bundle deploy`.
9. **B** — Long stragglers + spill on a wide join signal **skew**; cluster/partition on the join key (Liquid Clustering) and compact (predictive optimization).
10. **B** — A **column mask** (plus RLS if needed) hides PII while grants on the aggregate tables allow the metrics; UC enforces it for warehouse **and** dashboard.

### Before the exam — checklist
- [ ] **45 questions / 90 min** → ~2 min each; flag-and-return rather than stall. No aids.
- [ ] Use the **current names**: Lakeflow **Jobs**, Lakeflow Spark **Declarative Pipelines**
      (ex-DLT), **Declarative Automation Bundles** (ex-DABs), **Git Folders** (ex-Repos),
      **AI/BI Dashboards** (ex-Lakeview), **Lakeflow Connect**.
- [ ] **Prioritize ingestion**: Auto Loader (incremental + evolving) vs `COPY INTO` (stable
      batch) vs `read_files` (ad-hoc) vs Lakeflow Connect (managed DB/SaaS).
- [ ] **Medallion**: bronze = raw + metadata (no cleaning); silver = clean/conform +
      quarantine; gold = star schema + `agg_*` for BI.
- [ ] **MERGE/SCD2**, exactly-once checkpoints, **watermarks** for late/out-of-order events.
- [ ] **Lakeflow Jobs**: task types (incl. **Dashboard**), `depends_on`, conditional
      `run_if`, retries, trigger types (cron/file-arrival), notifications.
- [ ] **Compute**: serverless SQL warehouse for BI, job compute for ETL, all-purpose for dev
      — and Free Edition is **serverless-only**.
- [ ] **Delta ops**: `OPTIMIZE`, **Liquid Clustering**, `VACUUM` vs time travel, predictive
      optimization; read the **Spark UI** for skew/shuffle/spill.
- [ ] **Governance**: UC three-level namespace, managed vs external, `GRANT/REVOKE/DENY`,
      **column masking**, **RLS**, **ABAC**, lineage, audit.
- [ ] Re-skim **M0–M10** chapters and re-check the **live exam guide** the night before.

Good luck — you built the whole thing, so you already know it.
