# M8 · Troubleshooting, Monitoring & Optimization

> **Exam domain 6 — Troubleshooting, Monitoring & Optimization.** Objectives: spot
> **trends in job performance** via the **Lakeflow Jobs run history** (vs historical
> baselines); use the **Lakeflow Jobs UI** to monitor pipeline health (statuses, DAG, run
> times, failure rates); find bottlenecks — **skew, shuffle, spill** — from **stage-level
> Spark UI** metrics; understand **Liquid Clustering** + **predictive optimization**;
> diagnose **cluster-startup failures, library conflicts, and OOM**.

## 1 · Concept / theory

### Two altitudes of monitoring
- **Orchestration altitude — Lakeflow Jobs UI.** Did the *job* succeed, how long did each
  task take, what's the failure rate? The **run history** lists every run of a job with
  status, duration, and trigger; sort/scan it to see a task that used to take 4 min now
  taking 12 — a **trend vs the baseline** (the exam's "is this run slower than usual?"
  question). The **DAG** view shows task dependencies and which task failed/skipped.
- **Execution altitude — Spark UI.** *Why* was a task slow? The Spark UI breaks a job into
  **stages** (work between shuffles) and **tasks** (one per partition). Bottlenecks live
  here: skew, shuffle, spill.

### The Spark execution model (still on the exam, even on serverless)
A Spark **job** → **stages** (split at every shuffle/wide dependency) → **tasks** (one per
partition, run on executor cores). Free Edition is **serverless** so you don't size a
cluster — but the exam still tests the model, so learn it:
- **Driver** — plans the job, collects results. OOMs if you `collect()` a huge DataFrame.
- **Executors** — run tasks; each has memory + cores. OOM here = a partition too big to fit.
- **Partitions** — the unit of parallelism. Too few → idle cores; too many → scheduling
  overhead and tiny files.
- **Narrow vs wide** — `select`/`filter`/`withColumn` are **narrow** (no data movement);
  `join`/`groupBy`/`distinct`/`orderBy` are **wide** → a **shuffle** (data repartitioned
  across the network by key). Shuffles are where it gets slow.

### The three classic bottlenecks (read them off the Spark UI)
1. **Data skew** — one key has far more rows than others, so one task does most of the work
   while the rest finish and idle. **Tell:** in the stage's task table, **max** "Shuffle
   Read" (or task duration) is **≫ median** (a long-tail histogram). In our data,
   `telematics` joined to `policies` on `policy_id` skews — a handful of high-mileage
   policies own a disproportionate share of trip events.
2. **Shuffle** — the join/aggregation itself moving data across the network. Watch
   **"Shuffle Read/Write"** sizes; too few `shuffle.partitions` makes each one huge.
3. **Disk spill** — a partition doesn't fit in executor memory, so Spark writes it to disk
   and reads it back (slow). **Tell:** non-zero **"Spill (Memory)"** / **"Spill (Disk)"**
   columns. Spill usually means partitions are too big (skew, or too few partitions).

### AQE — the engine fixes a lot for you
**Adaptive Query Execution** (on by default) re-plans at runtime using actual shuffle
statistics. It does three big things the exam expects you to name:
- **Coalesce shuffle partitions** — merge tiny post-shuffle partitions (you don't have to
  hand-tune `shuffle.partitions` as much as in old Spark).
- **Skew-join handling** (`spark.sql.adaptive.skewJoin.enabled`) — detects a skewed
  partition and **splits it** into sub-partitions so the work spreads across tasks. This is
  the **first fix** for a max-≫-median shuffle-read stage.
- **Dynamically switch join strategy** — e.g. demote a sort-merge join to a broadcast join
  when a side turns out small.

### Salting — the manual skew fix when AQE isn't enough
If skew-join handling still leaves one giant task (a *single* key is monstrous), **salt**
the key: add a random suffix `0..N` to the hot key on the big side, replicate the small
side across all N salt values, join on `(key, salt)`. This spreads one mega-partition into
N — at the cost of an N× fan-out on the replicated side. It's the textbook answer to
"max shuffle read ≫ median and AQE didn't fully fix it."

### Broadcast joins — kill the shuffle entirely for small dims
If one join side is small, **broadcast** it: ship the whole small table to every executor
so the big side joins locally — **no shuffle on the big table**. Controlled by
`spark.sql.autoBroadcastJoinThreshold` (default ~10 MB; `-1` disables). Our **reference
dims** (`ref_us_states`, `ref_peril_codes`, … 6 tiny CSVs) and **`gold.dim_date`** (~3.6k
rows) are perfect broadcast candidates — every fact join to them should be a broadcast, not
a sort-merge shuffle.

### Liquid Clustering vs partitioning
- **Hive partitioning** (`PARTITIONED BY`) physically splits data into per-value directories.
  It helps *only* the partition column, and **over-partitioning** (high-cardinality key like
  `policy_id`) creates millions of tiny files — a classic anti-pattern.
- **Liquid Clustering** (`CLUSTER BY`) — the modern replacement. You declare clustering
  keys; Delta keeps data clustered by them (data-skipping on those columns) **without rigid
  directories**, and you can **change the keys later** without rewriting the table.
  `CLUSTER BY AUTO` lets Databricks pick keys from query patterns. Prefer it over
  partitioning for almost everything here (e.g. cluster `fact_claims` by `claim_date`/
  `policy_id` for the date-range and per-policy queries).
- **`OPTIMIZE`** compacts many small files into right-sized ones (and re-clusters for
  Liquid Clustering). Run it after big appends. (`ZORDER BY` is the *older* skipping
  technique — Liquid Clustering supersedes it; don't combine them.)

### Predictive optimization
Databricks **automatically** runs maintenance (**`OPTIMIZE`**, **`VACUUM`**, stats, and
clustering) on UC managed tables — **no schedule to write, no cron job**. It learns from
your query/write patterns when compaction pays off. On managed tables this is the modern
answer to "how do I keep files compacted and old versions cleaned" — *you enable predictive
optimization* instead of hand-scheduling maintenance jobs.

### Diagnosing failures
- **Cluster-startup failure** — compute never launches (quota/capacity, bad init script,
  unavailable instance type). On FE this is largely abstracted, but the exam tests it: read
  the **event log / driver logs**; an **init-script error** is a common cause.
- **Library conflicts** — two libraries (or a workspace lib vs a notebook-scoped one) pin
  incompatible versions → `ImportError`/`NoSuchMethodError`. Fix with **notebook-scoped
  libraries** (`%pip install`) to isolate, and pin compatible versions.
- **OOM (out of memory)** — `java.lang.OutOfMemoryError`/executor lost. **Driver OOM**:
  you `collect()`/`toPandas()`'d too much, or broadcast something too big — don't collect
  big data; lower the broadcast threshold. **Executor OOM**: a partition too large — usually
  **skew or too few partitions**; fix the skew (AQE/salt) or raise `shuffle.partitions`.

## 2 · Worked code

**Reading the Spark UI (the reasoning).** Open a notebook cell's **Spark Jobs ▸ View ▸
Stages**. For the `telematics ⨝ policies` join stage, look at the **task table** and the
**Summary Metrics** percentiles:

```
Stage 7 (sort-merge join, telematics ⨝ policies on policy_id)
Summary Metrics      Min     25th    Median   75th    Max
Duration             0.4 s   0.5 s    0.6 s   0.7 s   38 s     <- Max ≫ Median  → SKEW
Shuffle Read         11 MB   12 MB    12 MB   13 MB   2.9 GB   <- one task ate the hot key
Spill (Disk)         0       0        0       0       1.7 GB   <- that task spilled
```
Diagnosis: **one task** reads ~2.9 GB while the median reads ~12 MB and **spills** — a
single hot `policy_id`. **Fix order: (1) confirm AQE skew join is on; (2) if still skewed,
salt the key.**

**Tuning configs (set in the notebook/job):**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")              # AQE (default on)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")     # split skewed partitions
spark.conf.set("spark.sql.shuffle.partitions", "auto")            # AQE coalesces; or a number
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # broadcast dims < 50MB
# Knobs the exam names (mostly cluster-mode; serverless abstracts memory sizing):
#   spark.default.parallelism     -> default partitions for RDD/wide ops (≈ total cores)
#   spark.executor.memory / spark.driver.memory -> heap per executor / driver (OOM lever)
```

**Force-broadcast the small dim (PySpark) — no shuffle on the big fact:**
```python
from pyspark.sql.functions import broadcast
claims = spark.table("insurance.silver.claims")
perils = spark.table("insurance.bronze.ref_peril_codes")          # tiny
enriched = claims.join(broadcast(perils), "peril_code", "left")   # ship perils to executors
```

**Salt a skewed join (PySpark) — when AQE skew-join isn't enough:**
```python
from pyspark.sql import functions as F
N = 16
tel = (spark.table("insurance.silver.telematics")
         .withColumn("salt", (F.rand() * N).cast("int")))         # 0..N-1 on the BIG side
pol = (spark.table("insurance.silver.policies")
         .withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(N - 1)))))  # replicate small side
joined = tel.join(pol, ["policy_id", "salt"]).drop("salt")        # one mega-partition → N tasks
```

**Liquid Clustering + OPTIMIZE — SQL:**
```sql
-- declare clustering keys at create time (no rigid partition directories):
CREATE OR REPLACE TABLE insurance.gold.fact_claims (
  claim_key BIGINT, policy_id STRING, claim_date DATE, loss_amount DOUBLE /* ... */
) CLUSTER BY (claim_date, policy_id);

ALTER TABLE insurance.gold.fact_claims CLUSTER BY (claim_date);    -- change keys later, no rewrite
OPTIMIZE insurance.gold.fact_claims;                               -- compact + re-cluster
```

**Liquid Clustering — PySpark (DataFrame writer):**
```python
(spark.table("insurance.silver.claims")
   .write.format("delta").mode("overwrite")
   .clusterBy("claim_date", "policy_id")                          # Liquid Clustering keys
   .saveAsTable("insurance.gold.fact_claims"))
spark.sql("OPTIMIZE insurance.gold.fact_claims")
```

**Predictive optimization — enable, then stop scheduling maintenance:**
```sql
ALTER CATALOG insurance ENABLE PREDICTIVE OPTIMIZATION;   -- auto OPTIMIZE/VACUUM on managed tables
-- inspect what it did:
SELECT * FROM system.storage.predictive_optimization_operations_history
WHERE catalog_name = 'insurance' ORDER BY start_time DESC;
```

## 3 · Best practices & pitfalls
- **Diagnose before you tune.** Read the Spark UI *first*: max-≫-median ⇒ skew; non-zero
  spill ⇒ partitions too big; huge shuffle read/write ⇒ too few partitions or a missing
  broadcast. Don't blindly bump configs.
- **Skew fix order:** AQE skew-join (on by default) → then **salt** only if one key is still
  a monster. Salting adds fan-out cost, so don't reach for it first.
- **Broadcast the small dims** (`ref_*`, `dim_date`) and **never broadcast a big table** —
  broadcasting something over the threshold is a fast route to **driver OOM**. Raise
  `autoBroadcastJoinThreshold` deliberately, not wildly.
- **Don't `collect()`/`toPandas()` large results** — that's the #1 self-inflicted driver OOM.
- **Liquid Clustering over partitioning.** Don't `PARTITIONED BY` a high-cardinality key
  (`policy_id`, `claim_id`) — it shatters the table into tiny files. Use `CLUSTER BY`.
- **`OPTIMIZE` after big appends**; better, **enable predictive optimization** and stop
  hand-writing OPTIMIZE/VACUUM jobs on managed tables.
- **`VACUUM` vs time travel** — vacuuming below the retention window deletes files older
  versions need (recall M0). Predictive optimization respects retention.
- **Monitor trends, not just the last run** — the Lakeflow Jobs **run history** is the
  baseline; a single 12-min run only matters relative to the usual 4 min.
- **Library conflicts:** prefer **notebook-scoped `%pip`** to isolate versions over piling
  workspace-wide libraries that collide.

## 4 · Exam focus
**Objectives:** read **run-history** trends vs baselines; use the **Lakeflow Jobs UI**
(statuses/DAG/run times/failure rate) for pipeline health; interpret **stage-level Spark UI**
metrics to find **skew / shuffle / spill**; **AQE** skew-join + **salting**; **broadcast**
small dims (`autoBroadcastJoinThreshold`); the knobs `spark.sql.shuffle.partitions`,
`spark.default.parallelism`, `spark.executor/driver.memory`; **Liquid Clustering**
(`CLUSTER BY`) vs partitioning + `OPTIMIZE`; **predictive optimization** (auto OPTIMIZE/
VACUUM); diagnosing **startup failures / library conflicts / OOM**.

**Practice questions**
1. *In the Spark UI, one stage of a `telematics ⨝ policies` join has a task whose
   **max Shuffle Read (2.9 GB) is far above the median (12 MB)**, and that task spills to
   disk. What is happening and the first fix?* **A.** **Data skew** — a hot `policy_id` lands
   one giant partition on one task. **First fix: confirm AQE skew-join handling is enabled**
   (`spark.sql.adaptive.skewJoin.enabled`), which splits the skewed partition; if a single
   key is still too large, **salt** the join key. (Raising executor memory or
   `shuffle.partitions` alone doesn't address a single hot key.)
2. *A fact-to-`dim_date` join runs a full sort-merge **shuffle** even though `dim_date` is
   ~3.6k rows. Cheapest fix?* **A.** **Broadcast** the small dimension — `broadcast(dim)` or
   raise `spark.sql.autoBroadcastJoinThreshold` so it auto-broadcasts — eliminating the
   shuffle on the large fact. (Don't broadcast the fact; don't add partitions.)
3. *You want a managed gold table to stay compacted and have old files cleaned **without
   scheduling an OPTIMIZE/VACUUM job**. What do you use?* **A.** **Predictive optimization**
   — Databricks auto-runs `OPTIMIZE`/`VACUUM`/clustering on UC managed tables. (Hand-cron'd
   maintenance jobs are the thing it replaces; `ZORDER` is older skipping, not scheduling.)

## 5 · References
- **Spark UI** — jobs/stages/tasks, Summary Metrics percentiles, Shuffle Read/Write, **spill**
- **Adaptive Query Execution** — coalesce partitions, `skewJoin`, dynamic join switch
- **Skew & salting**; **broadcast joins** & `spark.sql.autoBroadcastJoinThreshold`
- Tuning: `spark.sql.shuffle.partitions`, `spark.default.parallelism`, `spark.executor/driver.memory`
- **Liquid Clustering** (`CLUSTER BY`, `CLUSTER BY AUTO`) vs `PARTITIONED BY`; **`OPTIMIZE`**
- **Predictive optimization** for UC managed tables; `VACUUM` vs time-travel retention
- **Lakeflow Jobs** run history & monitoring (statuses, DAG, durations, failure rate)
- Troubleshooting: cluster-startup failures (event/driver logs, init scripts), **OOM**,
  **notebook-scoped libraries** for conflicts
