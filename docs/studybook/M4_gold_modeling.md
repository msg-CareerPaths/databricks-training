# M4 · Gold — Dimensional Modeling for BI

> **Exam domain 3 — Data Transformation and Modeling.** Core objective: understand the
> difference between, and how to **build Gold-layer objects — tables, views, materialized
> views, and streaming tables — for BI/analytics teams in Unity Catalog**. Also: star-schema
> design (conformed **dimensions** + **facts** at a clear grain), the **aggregates** that
> answer the 8 business questions, aggregate functions (`count`, `approx_count_distinct`,
> `avg`, `summary()`), and **data-quality validation** on gold.

## 1 · Concept / theory

### Gold = serving layer, modeled for the question
Silver is clean and conformed; **gold is shaped for consumption**. The dominant pattern is the
**star schema**: narrow, query-friendly **facts** (one row per business event, mostly numeric
measures + foreign keys) surrounded by **dimensions** (the descriptive "by what" you slice on).
BI tools join one fact to many dimensions — simple, fast, intuitive.

### Conformed dimensions + facts at a declared grain
A **conformed** dimension is shared across facts (the same `dim_date`, `dim_policy` serve claims,
premium, and payments), so metrics line up across subject areas. The single most important design
decision is the fact's **grain** — *what one row means*. Declare it, then every measure must be
true at that grain.

| Gold object | Grain (one row =) | Built from |
|---|---|---|
| `dim_date` *(done — `src/gold/dim_date.sql`)* | one calendar day | generated |
| `dim_customer` | current customer (SCD2 current row) | `silver.dim_customer` |
| `dim_policy` | one policy | `silver.policies` |
| `dim_agent` | **current** agent (SCD2 `is_current` view) | `silver.dim_agent` |
| `fact_claims` | one claim | `silver.claims` |
| `fact_premium` | one policy × in-force month (earned premium) | `silver.policies` × `dim_date` |
| `fact_payments` | one billing installment | `silver.payments` |

Dimensions carry a **surrogate key** (`*_key`, a stable integer/hash) plus the natural business
key; facts store the surrogate FKs. `dim_date.date_key` (e.g. `20260504`) is the model already.

### The exam's heart: table vs view vs materialized view vs streaming table
All four are UC objects you can put in gold; the exam wants you to pick correctly on
**freshness, cost, and incremental** trade-offs.

| Object | What it is | Compute when | Freshness | Use in gold for |
|---|---|---|---|---|
| **Table** (managed Delta) | Materialized rows you write/`MERGE` | at write time | as of last write | facts/dims you load on a schedule; full control |
| **View** | Saved query; **no storage** | every read (recomputes) | always live | thin renames/joins, security/row filters, cheap small results |
| **Materialized view** | Query whose result **is stored** and kept fresh | refresh (incremental when possible) | as of last refresh | expensive **aggregates** read often (dashboards) |
| **Streaming table** | Append/CDC target fed by a **streaming** query | continuously / on trigger | near-real-time | continuously arriving facts (e.g. telematics) |

Rules of thumb: **view** when the query is cheap and you want zero staleness/zero storage;
**materialized view** when the query is expensive and read far more than the data changes —
Databricks does the **incremental** refresh for you; **streaming table** when the source is an
unbounded stream and you need exactly-once incremental ingest; plain **table** when you want to
own the write (custom `MERGE`/SCD2, back-dated corrections). Materialized views and streaming
tables are the native gold/silver objects of **Lakeflow Spark Declarative Pipelines** (M5).
*Free Edition:* serverless powers all four; MV/streaming-table **refresh** runs as a pipeline.

### Aggregates that answer the 8 requirements
Each business question (`docs/01_requirements.md`) maps to one gold aggregate, all
star-joins over the facts/dims above:

| Aggregate | Grain | Answers |
|---|---|---|
| `agg_loss_ratio` | month × product_line × state | #1 loss ratio |
| `agg_claims_monthly` | month × peril | #2 frequency & severity, #3 fraud rate |
| `agg_agent_performance` | agent | #5 policies sold, retention, loss ratio |
| `agg_telematics_risk` | policy | #6 harsh-driving score vs claims |
| `agg_customer_value` | customer | #7 tenure, active policies, lifetime premium, churn |

(#4 written-vs-collected reads `fact_premium`/`fact_payments` directly; #8 is the `ops.dq_scorecard`.)

### Aggregate functions you must know
`count(*)` / `count(col)` (nulls skipped), `sum`, `avg`/`mean`, `min`/`max`; **`approx_count_distinct(col)`**
— a HyperLogLog estimate, far cheaper than exact `count(distinct)` on high-cardinality columns
(policies, customers) and the exam's go-to for "approximate unique"; and **`summary()`** —
`df.summary()` returns count/mean/stddev/min/quartiles/max per column for a fast sanity profile.

## 2 · Worked code

**`dim_agent` — SCD2 *current* view (a thin VIEW: live, zero storage):**
```sql
CREATE OR REPLACE VIEW insurance.gold.dim_agent AS
SELECT xxhash64(agent_id) AS agent_key, agent_id,           -- surrogate + business key
       first_name, last_name, branch, region, state, status, commission_rate
FROM insurance.silver.dim_agent
WHERE is_current = true;                                     -- collapse SCD2 history to "now"
```

**`agg_loss_ratio` — star-join MATERIALIZED VIEW (expensive, read on every dashboard load):**
```sql
CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_loss_ratio AS
WITH losses AS (   -- incurred losses by month/line/state
  SELECT d.year_month, p.product_line, p.state, SUM(f.loss_amount) AS incurred_losses
  FROM insurance.gold.fact_claims  f
  JOIN insurance.gold.dim_policy   p ON f.policy_key = p.policy_key
  JOIN insurance.gold.dim_date     d ON f.loss_date_key = d.date_key
  GROUP BY 1, 2, 3),
earned AS (        -- earned premium = annual_premium/12 per in-force month
  SELECT d.year_month, p.product_line, p.state, SUM(fp.earned_premium) AS earned_premium
  FROM insurance.gold.fact_premium fp
  JOIN insurance.gold.dim_policy   p ON fp.policy_key = p.policy_key
  JOIN insurance.gold.dim_date     d ON fp.month_key  = d.date_key
  GROUP BY 1, 2, 3)
SELECT e.year_month, e.product_line, e.state,
       e.earned_premium, COALESCE(l.incurred_losses, 0) AS incurred_losses,
       COALESCE(l.incurred_losses, 0) / NULLIF(e.earned_premium, 0) AS loss_ratio
FROM earned e LEFT JOIN losses l USING (year_month, product_line, state);
```

**`agg_claims_monthly` — frequency/severity/fraud, with `approx_count_distinct` (SQL):**
```sql
CREATE OR REPLACE MATERIALIZED VIEW insurance.gold.agg_claims_monthly AS
SELECT d.year_month, rp.peril_name,
       COUNT(*)                              AS claim_count,
       AVG(f.loss_amount)                    AS severity,          -- avg loss per claim
       approx_count_distinct(f.policy_key)   AS policies_with_claims,
       AVG(CASE WHEN f.fraud_flag THEN 1 ELSE 0 END) AS fraud_rate -- ~0.04 after silver
FROM insurance.gold.fact_claims  f
JOIN insurance.gold.dim_date     d  ON f.loss_date_key = d.date_key
JOIN insurance.silver.ref_peril_codes rp ON f.peril_code = rp.peril_code
GROUP BY d.year_month, rp.peril_name;
```

**PySpark equivalent — `agg_customer_value` (one current row per customer):**
```python
from pyspark.sql import functions as F
from src.common import config

cust = spark.read.table(config.table("gold", "dim_customer"))
pol  = spark.read.table(config.table("gold", "dim_policy"))
prem = spark.read.table(config.table("gold", "fact_premium"))

policy_roll = (pol.groupBy("customer_key")
    .agg(F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_policies"),
         F.countDistinct("policy_key").alias("total_policies")))
premium_roll = (prem.groupBy("customer_key")
    .agg(F.sum("earned_premium").alias("lifetime_premium")))

agg = (cust.join(policy_roll, "customer_key", "left")
           .join(premium_roll, "customer_key", "left")
           .withColumn("tenure_months",
                       F.months_between(F.current_date(), F.col("customer_since")).cast("int"))
           .withColumn("churn_flag", F.col("active_policies").isNull() | (F.col("active_policies") == 0))
           .na.fill({"active_policies": 0, "total_policies": 0, "lifetime_premium": 0.0}))

(agg.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(config.table("gold", "agg_customer_value")))
```

**`fact_premium` is the one fact that *fans out* silver:** a policy in force Jan–Jun yields six
rows (`annual_premium/12` each). Generate the month spine from `dim_date`, then join:
```sql
CREATE OR REPLACE TABLE insurance.gold.fact_premium AS
SELECT xxhash64(p.policy_id) AS policy_key, p.customer_key, d.date_key AS month_key,
       p.annual_premium / 12.0 AS earned_premium
FROM insurance.silver.policies p
JOIN insurance.gold.dim_date d
  ON d.date = d.first_of_month                                  -- one row per month
 AND d.date BETWEEN p.effective_date AND p.expiration_date;     -- months the policy is in force
```

**DQ validation on gold (sanity ranges + referential completeness):**
```sql
-- 1) Loss ratio sane for >95% of cells (requirement #1 acceptance)
SELECT AVG(CASE WHEN loss_ratio BETWEEN 0 AND 5 THEN 1.0 ELSE 0 END) AS pct_sane
FROM insurance.gold.agg_loss_ratio;
-- 2) Referential completeness: every fact FK resolves to a dimension (expect 0)
SELECT count(*) AS orphan_facts
FROM insurance.gold.fact_claims f
LEFT ANTI JOIN insurance.gold.dim_policy p ON f.policy_key = p.policy_key;
-- 3) Fast profile of the measures
-- SELECT * FROM (SELECT loss_amount, paid_amount FROM insurance.gold.fact_claims); -- then df.summary() in PySpark
```

## 3 · Best practices & pitfalls

- **Declare the grain first**, then validate `count(*)` == expected (e.g. `fact_payments` rows ==
  installments). A silent join fan-out is the classic way to double-count `sum(amount_due)`.
- **Pick the object by economics:** read-heavy expensive aggregate → **materialized view** (incremental
  refresh); cheap rename/filter → **view**; unbounded source → **streaming table**; custom write/SCD2 →
  **table**. Don't materialize what a view answers instantly, and don't recompute via a view what a
  dashboard hits thousands of times.
- A **materialized view is only as fresh as its last refresh** — schedule/trigger it; understand it can
  fall back to a **full** recompute if the query isn't incrementalizable (non-deterministic functions,
  some joins). A **view never goes stale** but pays full compute every read.
- Build dimensions on the **SCD2 *current* view** (`is_current = true`) so each active row maps to exactly
  one dimension version (requirement #5/#7 acceptance). Joining the full history multiplies facts.
- Use **`approx_count_distinct`** for big unique counts (active policies, customers); reserve exact
  `count(distinct)` for small/audited numbers — the exam tests that you know the cost trade-off.
- Guard divisions: `losses / NULLIF(premium, 0)` — never let a zero-premium cell blow up the loss ratio.
- Validate gold with **range checks + LEFT ANTI JOIN** (orphans) + **`summary()`**; gold feeds executives,
  so a sanity layer here is cheap insurance.
- `MERGE` upserts/SCD2 live in **silver**; gold facts/aggregates are normally **full rebuilds** or MV
  **refreshes** — don't re-implement CDC in gold.

## 4 · Exam focus

**Objectives:** build **tables, views, materialized views, and streaming tables** in UC for BI; choose the
right one by freshness/cost/incremental; design star schemas (conformed dims + facts at a grain); compute
**aggregates** (`count`, `approx_count_distinct`, `avg`/`mean`, `sum`, `summary()`); validate DQ on gold.

**Practice questions**
1. *A dashboard runs an expensive 4-table join + GROUP BY hundreds of times a day; the underlying facts
   update only nightly. You want fast reads and Databricks-managed incremental refresh. Which gold object?*
   **A. Materialized view.** Its result is **stored** and **incrementally refreshed**; a plain view would
   recompute the costly join on every read, and a table would force you to hand-code the refresh/MERGE.
2. *You need a thin gold object exposing only the **current** SCD2 agent row with a couple of renamed
   columns, always live and using no extra storage. Which object?* **A. A view** (`WHERE is_current = true`).
   It recomputes on read (cheap here), never goes stale, and stores nothing — unlike a materialized view
   or table, which persist rows.
3. *You must report the number of **distinct policies** with a claim across ~180k claims, where a close
   estimate is acceptable and speed matters. Which function?* **A. `approx_count_distinct(policy_key)`** —
   a cheap HyperLogLog estimate; exact `count(distinct)` is far costlier on high-cardinality columns and
   isn't required when an approximation suffices.

## 5 · References

- Gold objects in Unity Catalog: **CREATE TABLE / VIEW / MATERIALIZED VIEW / STREAMING TABLE**
- **Materialized views** — incremental refresh, refresh scheduling, incrementalization limits
- **Streaming tables** and **Lakeflow Spark Declarative Pipelines** (the MV/streaming-table runtime, M5)
- **Dimensional modeling** on the lakehouse: star schema, conformed dimensions, fact grain, surrogate keys
- Aggregate functions: `count`, **`approx_count_distinct`**, `avg`/`mean`, `sum`; `DataFrame.summary()`
- The Medallion architecture (gold = curated serving layer)
