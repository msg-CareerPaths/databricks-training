# M5 · Lakeflow Spark Declarative Pipelines

> **Exam domains 2 (Data Ingestion) and 3 (Data Transformation and Modeling).** There is
> **no standalone "pipelines" domain** — a declarative pipeline is a *tool you use inside*
> D2/D3. This chapter re-expresses the whole medallion (bronze → silver → gold) as one
> **Lakeflow Spark Declarative Pipeline** (*formerly Delta Live Tables / DLT*) and teaches
> the two things the exam keeps asking: **streaming tables vs materialized views** and
> **expectations** (data-quality constraints). On **Free Edition** pipelines run
> **serverless**.

## 1 · Concept / theory

### What a declarative pipeline is
You **declare the datasets you want** (each as a `CREATE … STREAMING TABLE` /
`MATERIALIZED VIEW` or a `@dlt.table`-decorated function) and the engine figures out the
**how**: dependency order, incremental vs full refresh, checkpoints, retries. You write
*what each table is*; you do **not** write `writeStream`, `trigger`, or orchestration glue
(that was M2's manual style; M6 orchestrates the pipeline as a job).

### The pipeline is a DAG of datasets
The engine reads every dataset definition, sees that `silver_customers` reads from
`bronze_customers` (via `STREAM(...)` / `dlt.read_stream`) and that `gold_agg_loss_ratio`
reads from the silver facts, and **infers the DAG** from those references. It then runs the
nodes in topological order and only recomputes what changed. References inside the pipeline
use the **bare dataset name** (`LIVE.` prefix is legacy) — the engine resolves them.

### The event log
Every pipeline writes a structured **event log** (queryable via `event_log(...)`): one row
per event — dataset materialized, **expectation pass/fail counts**, row counts, data
quality, lineage, durations, errors. This is your audit + monitoring surface (it's how M8
trends DQ and how we feed `ops.dq_scorecard`). The UI's DAG view and the per-dataset
data-quality panel are just renderings of the event log.

### Serverless execution on Free Edition
A pipeline runs in **triggered** mode (run to completion, then stop — budget-friendly,
what we use) or **continuous** mode (stay up, process as data arrives). On **Free Edition
everything is serverless**: you don't pick cluster sizes; the platform provisions compute
for the run. **Unity Catalog** is the publishing target — set the pipeline's **default
catalog + schema** (`insurance` / `bronze` here) and datasets land as governed UC tables.

### Streaming tables vs materialized views — the core distinction
| | **Streaming table** | **Materialized view (MV)** |
|---|---|---|
| Processes | **incrementally**, append-only, exactly-once | **recomputes** the full result on refresh |
| Source must be | a **stream** (`STREAM(...)` / `read_stream`) — append-only | a batch query (any tables/MVs) |
| Keeps state | yes — **checkpoint**, won't re-read old files | no — declarative, always up to date |
| Use for | **ingest + row-level transform**: bronze, silver clean | **aggregates / joins** that must recompute: gold |
| Our layer | **bronze.\*** (Auto Loader), **silver.\*** clean | **gold.agg\_\*** rollups |

Rule of thumb: **append-only, incremental → streaming table; derived/aggregated, always-fresh
→ materialized view.** Reading a streaming table with `STREAM(...)` keeps the *downstream*
incremental too — that's how bronze→silver stays cheap.

### Expectations — declarative data quality
Expectations are **named boolean constraints** evaluated as rows flow through a dataset.
Three actions (this is the exam's favourite table):

| Clause (SQL) | Decorator (Python) | On violation | Pipeline |
|---|---|---|---|
| `EXPECT (...)` | `@dlt.expect` | **keep** the row, **count** the failure in the event log | keeps running (warn/track) |
| `EXPECT (...) ON VIOLATION DROP ROW` | `@dlt.expect_or_drop` | **drop** the bad row, keep counting | keeps running |
| `EXPECT (...) ON VIOLATION FAIL UPDATE` | `@dlt.expect_or_fail` | **fail the whole update** | stops immediately |

This is exactly how we enforce the data-dictionary DQ rules **inside** the pipeline instead
of hand-rolling a quarantine: non-null business keys, `premium > 0`, `loss_amount <=
sum_insured`, valid `status`. Use **`expect`** to *track* a defect you tolerate, **`drop`**
to fence bad rows out of silver, **`fail`** for invariants that must never break.

## 2 · Worked code

The same pipeline, written both ways. One source file may be **all SQL**, **all Python**,
or a mix — the engine stitches them into one DAG. Names follow `src/common/config.py`
(`insurance.bronze/silver/gold`); in a per-user dev target the pipeline's default schema
carries the prefix, so definitions stay unqualified.

### 2a · SQL

```sql
-- BRONZE: streaming table = incremental Auto Loader ingest (append-only)
CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT 'Raw customers, exactly-once via Auto Loader'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingest_ts
   FROM STREAM read_files('/Volumes/insurance/landing/raw/customers',
                          format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze_telematics
AS SELECT *, current_timestamp() AS _ingest_ts
   FROM STREAM read_files('/Volumes/insurance/landing/raw/telematics', format => 'json');

-- SILVER: still a streaming table (incremental), now with EXPECTATIONS enforcing DQ rules
CREATE OR REFRESH STREAMING TABLE silver_customers (
  CONSTRAINT valid_id    EXPECT (customer_id IS NOT NULL)      ON VIOLATION DROP ROW,
  CONSTRAINT valid_state EXPECT (length(state) = 2),                       -- warn + track
  CONSTRAINT valid_seg   EXPECT (segment IN ('PERSONAL','COMMERCIAL')) ON VIOLATION FAIL UPDATE
)
AS SELECT customer_id, initcap(trim(city)) AS city, upper(state) AS state, segment
   FROM STREAM(bronze_customers);          -- STREAM() keeps the read incremental

-- GOLD: materialized view = recomputed aggregate (Requirement #1, loss ratio)
CREATE OR REFRESH MATERIALIZED VIEW gold_agg_loss_ratio
COMMENT 'Monthly loss ratio by product line & state'
AS SELECT date_trunc('MONTH', c.loss_date) AS month, p.product_line, cu.state,
          sum(c.loss_amount) / nullif(sum(p.annual_premium/12), 0) AS loss_ratio
   FROM silver_claims c
   JOIN silver_policies  p  USING (policy_id)
   JOIN silver_customers cu USING (customer_id)
   GROUP BY 1, 2, 3;
```

### 2b · Python (decorator API)

```python
import dlt                                   # current pipelines module (decorator API)
from pyspark.sql.functions import col, current_timestamp, initcap, trim, upper

# BRONZE: streaming table — append-only Auto Loader ingest
@dlt.table(name="bronze_telematics", comment="Raw per-trip IoT events")
def bronze_telematics():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")   # device_fw drift
            .load("/Volumes/insurance/landing/raw/telematics")
            .withColumn("_ingest_ts", current_timestamp()))

# SILVER: streaming table + expectations. Decorators stack; multiple-rule form also exists.
@dlt.table(name="silver_customers")
@dlt.expect_or_drop("valid_id", "customer_id IS NOT NULL")               # drop bad rows
@dlt.expect("valid_state", "length(state) = 2")                          # warn + track
@dlt.expect_or_fail("valid_seg", "segment IN ('PERSONAL','COMMERCIAL')") # fail update
def silver_customers():
    return (dlt.read_stream("bronze_customers")        # incremental read of a streaming tbl
            .select("customer_id", initcap(trim(col("city"))).alias("city"),
                    upper("state").alias("state"), "segment"))

# GOLD: materialized view — recomputed aggregate
@dlt.table(name="gold_agg_telematics_risk", comment="Harsh-event score per policy")
def gold_agg_telematics_risk():
    t = dlt.read("silver_telematics")                  # batch read → full recompute
    return (t.groupBy("policy_id")
            .agg((col("harsh_brake") + col("harsh_accel")).alias("risk_score")))
```

> **Two Python forms exist** and both are current: `@dlt.expect("name", "cond")` with one
> rule per decorator (above), or the **dictionary** form
> `@dlt.expect_all_or_drop({"valid_id": "customer_id IS NOT NULL", "premium_pos":
> "annual_premium > 0"})` to apply many rules at once. `dlt.read_stream`/`dlt.read`
> reference other pipeline datasets (vs `spark.readStream`/`read_files` for external
> sources).

### How it maps to our medallion
- **bronze.customers / bronze.telematics** → `STREAMING TABLE` (Auto Loader, incremental).
- **silver clean** → `STREAMING TABLE` reading bronze with `STREAM()` / `read_stream`,
  carrying the **expectations** that replace the manual quarantine (non-null keys,
  `annual_premium > 0`, `loss_amount <= sum_insured`, valid `status`/`segment`).
- **gold aggregates** (`agg_loss_ratio`, `agg_telematics_risk`, …) → `MATERIALIZED VIEW`
  (recomputed rollups for BI).

### Deploying it (the bundle resource)
The pipeline isn't a notebook you click — it's deployed by a bundle resource
**`resources/insurance_pipeline.pipeline.yml`**. That file declares the pipeline name, its
**source libraries** (the SQL/Python files above), `serverless: true`, the **default
catalog `insurance` + target schema**, and the dev/prod target. `databricks bundle deploy`
(M7) creates the pipeline; M6 runs it from a Lakeflow Job.

## 3 · Best practices & pitfalls

- **Pick the right object type.** Append-only ingest/transform → **streaming table**;
  recomputed aggregate/join → **materialized view**. Putting a `GROUP BY` in a streaming
  table, or trying to feed an MV from a `STREAM()`, is the classic mistake.
- A streaming table's **source must be append-only**. If silver needs upserts/SCD2 (our
  `dim_agent`), use **`APPLY CHANGES INTO` / `create_auto_cdc_flow`** (CDC), *not* a plain
  streaming table — a streaming table can't consume updates/deletes.
- **`STREAM()` / `read_stream` keeps downstream incremental.** Reading a streaming table as
  a *batch* (`dlt.read` / plain `FROM tbl`) forces a full recompute — fine for an MV, wrong
  for an incremental silver hop.
- **Choose the expectation action deliberately:** `expect` to *measure* a tolerated defect,
  `expect_or_drop` to *exclude* bad rows, `expect_or_fail` only for true invariants — a
  `fail` on a noisy rule will halt every run.
- **Don't hand-roll checkpoints.** The pipeline manages checkpoints/state per dataset; you
  don't set `checkpointLocation`. Renaming a streaming table loses its state (re-ingests).
- **Reference datasets by bare name**, not the full `catalog.schema.table` and not the
  legacy `LIVE.` prefix — that's what lets the engine build the DAG and isolate dev targets.
- **Read DQ from the event log**, not by eyeballing the UI: `expectation` rows give pass/drop
  counts per constraint — that's the feed for `ops.dq_scorecard` and M8's alerts.
- **Use triggered + serverless on Free Edition.** Continuous mode keeps compute up and burns
  the budget; triggered runs to completion and stops.

## 4 · Exam focus

**Objectives:** define a declarative pipeline as a **DAG of datasets**; distinguish
**streaming tables vs materialized views** and pick correctly per layer; write **expectations**
(`expect` / `expect_or_drop` / `expect_or_fail`) in **SQL and Python**; read pipeline health
from the **event log**; know it runs **serverless** (triggered vs continuous) and publishes to
**Unity Catalog**. Remember pipelines live **inside D2 (ingest) and D3 (transform/model)** —
there is no separate pipelines domain.

**Practice questions**

1. *A silver dataset must incrementally read an append-only bronze table and drop rows whose
   business key is null, without stopping the pipeline. Which object + clause?*
   **A.** A **streaming table** reading `STREAM(bronze_…)` with
   `CONSTRAINT … EXPECT (key IS NOT NULL) ON VIOLATION DROP ROW`
   (= `@dlt.expect_or_drop`). A materialized view would recompute (not incremental); plain
   `EXPECT` would keep the bad rows; `FAIL UPDATE` would halt the run.

2. *Requirement #1 needs monthly loss ratio by product line & state, always reflecting the
   latest silver data, computed by joining and aggregating cleaned facts. Streaming table or
   materialized view?* **A.** **Materialized view** — it's a recomputed aggregate/join, so it
   must refresh fully; a streaming table can't host a `GROUP BY` over an append stream.

3. *You must enforce that `segment` is only `PERSONAL`/`COMMERCIAL` and abort the entire
   update if it ever isn't, while just **tracking** (not dropping) bad `state` lengths. Which
   two expectation actions?* **A.** `EXPECT (segment IN ('PERSONAL','COMMERCIAL')) ON
   VIOLATION FAIL UPDATE` (= `@dlt.expect_or_fail`) for the invariant, and a plain `EXPECT
   (length(state)=2)` (= `@dlt.expect`) to warn/track in the **event log** without removing
   rows.

## 5 · References

- **Lakeflow Spark Declarative Pipelines** (formerly Delta Live Tables / DLT) — concepts,
  triggered vs continuous, serverless, publishing to Unity Catalog
- **Streaming tables** vs **materialized views** — when to use each
- **Pipeline expectations** — `expect`, `expect_or_drop`, `expect_or_fail`; SQL
  `CONSTRAINT … EXPECT … ON VIOLATION …`; Python decorator + `expect_all_*` forms
- **`CREATE STREAMING TABLE` / `CREATE MATERIALIZED VIEW`** SQL syntax; `STREAM()` and
  `read_files` as pipeline sources
- **`APPLY CHANGES INTO` / `create_auto_cdc_flow`** — CDC/SCD into streaming tables
- **Pipeline event log** — querying `event_log(...)` for data quality, lineage, runtime
- **Deploying a pipeline with a bundle** — `resources/*.pipeline.yml` resource (M7)
