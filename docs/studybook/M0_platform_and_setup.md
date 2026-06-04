# M0 · Databricks Intelligence Platform & Setup

> **Exam domain 1 — Databricks Intelligence Platform.** Objectives: (a) understand the
> core components — architecture, **Delta Lake**, **Unity Catalog**; (b) understand the
> compute services — characteristics, limitations, cost models — and pick the right one.

## 1 · Concept / theory

### The lakehouse, in one picture
Databricks separates **storage** (cheap cloud object storage holding open **Delta**/
Parquet files) from **compute** (ephemeral, on-demand). One copy of the data serves both
BI and AI. Unity Catalog governs *everything* on top.

### Delta Lake
A table format = Parquet data files + a **transaction log** (`_delta_log/`). The log is
what gives you:
- **ACID transactions** — concurrent reads/writes stay consistent; each commit is atomic.
- **Time travel** — query old versions (`VERSION AS OF` / `TIMESTAMP AS OF`); the log
  records every version. Great for audits and "oops" rollback.
- **Schema enforcement & evolution** — bad-schema writes are rejected unless you opt into
  evolution.
- **Performance ops** — `OPTIMIZE` (compaction), **Liquid Clustering**, `VACUUM`
  (remove old files), predictive optimization (covered in M8).

This is why the answer to "rapid iteration + reliable rollback + audit + one source of
truth for AI & BI" is **Delta Lake + Unity Catalog** (see the guide's sample Q2).

### Unity Catalog (UC)
The governance layer. **Three-level namespace**: `catalog.schema.object`.
- **Metastore** (one per region) → **Catalogs** → **Schemas** → **Tables / Views /
  Volumes / Models / Functions**.
- **Managed vs external tables** — managed: UC owns the data lifecycle (drop = data gone),
  stored in the metastore's managed location; external: you point at a path UC governs but
  doesn't own. (Detail in M9.)
- **Volumes** — UC-governed storage for **non-tabular files** (CSV/JSON/Parquet/images).
  This project lands its raw files in the Volume `insurance.landing.raw` (M1).
- Built-in **lineage**, **access control**, and **audit** (M9).

### Compute services & cost
- **Serverless** — Databricks manages the compute; fast start, autoscaling, pay for use.
  **Free Edition is serverless-only.**
- **SQL warehouse** — compute for SQL/BI and dashboards. A **serverless SQL warehouse**
  starts in seconds and supports many concurrent analysts — the right choice for ad-hoc
  BI (the guide's sample Q4 points at the concurrency/fast-start option).
- **Job compute vs all-purpose** — job compute is cheaper, created per job run for
  scheduled ETL; all-purpose is for interactive development (more expensive, keep it off
  when idle).
- **Cost model** — you're billed for compute uptime (DBUs). The exam tests *picking the
  cheapest compute that meets the requirement*; for us, serverless keeps it simple.

## 2 · Worked code

**SQL — set up the namespace and use time travel:**
```sql
CREATE CATALOG IF NOT EXISTS insurance;
CREATE SCHEMA  IF NOT EXISTS insurance.scratch;
USE CATALOG insurance; USE SCHEMA scratch;

CREATE OR REPLACE TABLE demo AS SELECT 1 AS id, 'a' AS v;
INSERT INTO demo VALUES (2, 'b');

DESCRIBE HISTORY demo;                 -- see every version + operation
SELECT * FROM demo VERSION AS OF 0;    -- time travel to the first commit
SELECT current_catalog(), current_user();
```

**PySpark — write Delta and read an older version:**
```python
df = spark.range(5).withColumn("v", (col("id") * 10))
df.write.format("delta").mode("overwrite").saveAsTable("insurance.scratch.demo2")
spark.sql("DELETE FROM insurance.scratch.demo2 WHERE id < 2")
# read the pre-delete version
spark.read.option("versionAsOf", 0).table("insurance.scratch.demo2").show()
```

## 3 · Best practices & pitfalls
- On Free Edition, prefer **managed** Delta tables (UC handles storage); you don't manage
  clusters at all.
- **Stop** SQL warehouses / interactive compute when idle — the daily budget is finite.
- Time travel + `VACUUM` interact: `VACUUM` past the retention window deletes the files
  older versions need. Don't `VACUUM` aggressively if you rely on time travel.
- Don't confuse **DBFS** (legacy) with **UC Volumes** (governed) — this project uses
  Volumes throughout.

## 4 · Exam focus
**Objectives tested:** core components (Delta, UC, architecture); compute characteristics/
limits/cost and choosing the right option.

**Practice questions**
1. *A team needs rapid pipeline iteration, reliable rollback after a bad ingest, audit
   trails, and one source of truth for BI **and** AI. Best strategy?*
   **A.** Delta Lake ACID + time travel, governed by Unity Catalog. ✅ — ACID+time travel
   give rollback/iteration; UC gives governance/lineage and one governed copy. (CSV+manual
   copies, object storage only, or in-memory DataFrames provide none of ACID/audit/lineage.)

2. *Analysts run many ad-hoc SQL queries all day on curated Delta tables; you want fast
   start, concurrency, and cost control. Which compute?*
   **A.** A **serverless SQL warehouse** (fast start + concurrency, scales to demand). A
   large all-purpose cluster wastes money; a single-node dev cluster can't serve many users.

3. *Where do raw CSV/JSON files live so they're governed by Unity Catalog?*
   **A.** In a **UC Volume** (not DBFS, not an ungoverned bucket).

## 5 · References
- Databricks Data Intelligence Platform — architecture overview
- Delta Lake: transaction log, time travel, `DESCRIBE HISTORY`
- Unity Catalog: object model, managed vs external, **Volumes**
- Serverless compute & SQL warehouses; Free Edition limitations

*(Look these up on docs.databricks.com — the exam tracks the current docs.)*
