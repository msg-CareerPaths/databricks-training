# M3 · Silver — Data Transformation and Modeling

> **Exam domain 3 — Data Transformation and Modeling.** This chapter covers the
> transforms the exam names explicitly: **read bronze and clean nulls + standardize
> types** into new silver tables; **joins** (inner, left, broadcast, multi-key, cross,
> union/union all); **column & row manipulation** (add, drop, split, rename, filter,
> explode arrays); **deduplication** and **aggregates** (count, approx_count_distinct,
> mean, summary); and **data-quality validation** so silver/gold stay reliable.

## 1 · Concept / theory

### Silver = clean + conform (the layer where defects die)
Bronze mirrors the source faithfully; **silver makes it trustworthy**. Every defect in
`docs/03_data_dictionary.md` is fixed here: trim/casing, type casts, invalid
categoricals mapped via reference tables, multi-format dates parsed, out-of-range
numerics and orphan FKs handled, booleans normalized, arrays exploded, duplicates
removed. Two outputs per entity: the **clean** table (`insurance.silver.<entity>`) and a
**quarantine** table for rows that fail validation — never silently drop bad data.

### Cleaning primitives
- **Text:** `trim`, `upper`/`lower`, `initcap`, `regexp_replace` (strip `$` `,` from a
  numeric-as-string premium).
- **Types:** `cast(...)`; dates with `to_date`/`to_timestamp`. Parse **mixed formats**
  with `coalesce(to_date(c,'fmt1'), to_date(c,'fmt2'), ...)` — the first that parses
  wins, the rest return `NULL`. Flag **future** dates with a comparison to
  `current_date()`.
- **Categoricals:** validate against a **reference (dim) table**. Because the dims are
  tiny (`ref_us_states` ≈ 50 rows), use a **broadcast join** — Spark ships the small side
  to every executor, skipping the shuffle.

### Joins (know all six)
| Join | Keeps | Use here |
|---|---|---|
| **inner** | matching rows both sides | policy ⨝ customer (valid FKs only) |
| **left** | all left + matched right | claim **left** ref_claim_status (keep unmapped to quarantine) |
| **broadcast** | a hint, not a type | tiny dim → `F.broadcast(dim)` / `/*+ BROADCAST */` |
| **multi-key** | join on ≥2 columns | coverage ⨝ ref on `(coverage_code, product_line)` |
| **cross** | Cartesian product | calendar/scenario fan-out (rare; explicit `crossJoin`) |
| **union / unionAll** | stack rows | combine Auto + Property claims. **`union` and `unionAll` are identical in Spark — both keep duplicates** (no implicit DISTINCT) |

**Orphan FKs** are removed with a **left-anti join** (`how="left_anti"`): keep left rows
with *no* match on the right → that's your orphan/quarantine set; the inner join is the
clean set.

### Structure manipulation
`withColumn` (add/derive), `drop`, `withColumnRenamed`, `split` (→ array, index it),
`filter`/`where` (rows), and **`explode`** to flatten an `array<struct>` into one row per
element (`policies.coverages[]` → a coverage line table). `explode_outer` keeps rows with
empty/NULL arrays.

### Deduplication
- **Exact dupes:** `dropDuplicates()` / SQL `DISTINCT`.
- **Fuzzy dupes** (same business key, casing/space noise): **normalize text first**, then
  keep one row per key — the canonical pattern is `row_number()` over a window
  partitioned by the key, ordered by recency, keep `_rn = 1`. `dropDuplicates(["key"])`
  picks an *arbitrary* row, so the window approach is preferred when "latest wins".

### Aggregates & profiling
`count`, `countDistinct`, **`approx_count_distinct`** (HyperLogLog — fast cardinality on
big columns), `avg`/`mean`, `sum`, `min`/`max`, and **`df.summary()`** /
`df.describe()` for a quick stats profile used in DQ checks.

### Late / out-of-order events (streaming dedup)
Telematics arrives late and out of order. A **watermark** (`withWatermark("event_ts",
"2 hours")`) bounds how long state is kept; combine with **`dropDuplicates(["event_id"])`**
to drop replays within that window. Watermarking also lets stateful aggregations drop
state for closed windows.

### SCD2 dimensions
`dim_customer` keeps **history**: `effective_from`/`effective_to`/`is_current`, one new
version when a tracked attribute changes — built with the exact MERGE pattern in
`src/silver/clean_agents_scd2.py` (the `mergeKey = NULL` trick). Reuse it.

## 2 · Worked code

**Clean customers — trim/case, mixed dates, map state via broadcast, fuzzy-dedup (PySpark):**
```python
from pyspark.sql import Window
from pyspark.sql import functions as F
from src.common import config

cust = spark.read.table(config.table("bronze", "customers"))
states = spark.read.table(config.table("bronze", "ref_us_states"))  # small dim

clean = (cust.where(F.col("customer_id").isNotNull())
    .withColumn("city", F.initcap(F.trim("city")))
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("state_raw", F.upper(F.trim("state")))
    # mixed/future dates: first format that parses wins; NULL if unparseable
    .withColumn("date_of_birth", F.coalesce(
        F.to_date("date_of_birth", "yyyy-MM-dd"),
        F.to_date("date_of_birth", "MM/dd/yyyy"),
        F.to_date("date_of_birth", "dd-MMM-yyyy")))
    .withColumn("dob_future_flag", F.col("date_of_birth") > F.current_date()))

# broadcast join the tiny state dim; unmapped state_code -> NULL (caught in DQ)
clean = (clean.join(F.broadcast(states),
                    clean.state_raw == states.state_code, "left")
              .withColumn("state", F.col("state_code"))
              .drop("state_raw", "state_code", "state_name"))

# fuzzy dedupe: normalized text already applied -> keep latest per business key
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last())
clean = clean.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
clean.write.mode("overwrite").saveAsTable(config.table("silver", "customers"))
```

**Clean policies — strip `$`/`,` premium, range-check, explode coverages (PySpark):**
```python
pol = spark.read.table(config.table("bronze", "policies"))
pol = (pol
    # numeric-as-string "$1,234.50" -> double, then negatives are invalid
    .withColumn("annual_premium",
                F.regexp_replace(F.col("annual_premium").cast("string"), r"[$,]", "").cast("double"))
    .withColumn("status", F.upper(F.trim("status")))
    .withColumn("effective_date", F.coalesce(
        F.to_date("effective_date", "yyyy-MM-dd"), F.to_date("effective_date", "MM/dd/yyyy")))
    .withColumn("premium_valid", F.col("annual_premium") >= 0))

# explode the nested array<struct> into one row per coverage line
cov = (pol.select("policy_id", "product_line", F.explode("coverages").alias("c"))
          .select("policy_id", "product_line",
                  "c.coverage_code", "c.limit", "c.deductible", "c.peril_code"))
cov.write.mode("overwrite").saveAsTable(config.table("silver", "policy_coverages"))
```

**Drop orphan FKs via anti-join → quarantine (PySpark):**
```python
cust_keys = spark.read.table(config.table("silver", "customers")).select("customer_id")
valid   = pol.join(cust_keys, "customer_id", "inner")       # FK resolves
orphans = pol.join(cust_keys, "customer_id", "left_anti")   # FK missing -> quarantine
valid.where("premium_valid").write.mode("overwrite").saveAsTable(config.table("silver", "policies"))
orphans.withColumn("_dq_reason", F.lit("orphan_customer_id")) \
       .write.mode("append").saveAsTable(config.table("silver", "policies_quarantine"))
```

**Clean claims — normalize mixed boolean, map status (left join), loss range (SQL):**
```sql
CREATE OR REPLACE TABLE insurance.silver.claims AS
SELECT  c.claim_id, c.policy_id, c.customer_id,
        coalesce(to_date(c.claim_date,'yyyy-MM-dd'), to_date(c.claim_date,'MM/dd/yyyy')) AS claim_date,
        upper(trim(c.claim_status))                               AS status_raw,
        s.status_name,
        -- mixed Y/N/1/0/YES/true -> real boolean
        upper(trim(cast(c.fraud_flag AS string))) IN ('Y','1','YES','TRUE') AS fraud_flag,
        c.loss_amount, p.sum_insured,
        (c.loss_amount <= p.sum_insured)                          AS loss_in_range
FROM        insurance.bronze.claims        AS c
LEFT JOIN /*+ BROADCAST(s) */ insurance.bronze.ref_claim_status AS s
       ON upper(trim(c.claim_status)) = s.status_code            -- small dim -> broadcast
JOIN        insurance.silver.policies      AS p ON c.policy_id = p.policy_id;  -- inner: drops orphan claims
```

**Aggregates / profiling (SQL + PySpark):**
```sql
SELECT product_line,
       count(*)                         AS claims,
       approx_count_distinct(policy_id) AS policies_est,   -- fast HLL cardinality
       avg(loss_amount)                 AS mean_loss
FROM insurance.silver.claims GROUP BY product_line;
```
```python
spark.read.table(config.table("silver", "claims")).summary("count","mean","min","max").show()
```

**Combine Auto + Property claims (union all keeps every row):**
```python
auto = spark.read.table(config.table("silver","claims")).where("product_line='AUTO'")
prop = spark.read.table(config.table("silver","claims")).where("product_line='PROPERTY'")
all_claims = auto.unionByName(prop)   # unionByName aligns columns by name, not position
```

**Telematics — watermark + dedupe late/out-of-order events (PySpark, streaming):**
```python
(spark.readStream.table(config.table("bronze", "telematics"))
   .where((F.col("speed_kmh") >= 0) & (F.col("speed_kmh") <= 300))  # drop out-of-range
   .withWatermark("event_ts", "2 hours")
   .dropDuplicates(["event_id"])                                    # drop replays in window
 .writeStream.option("checkpointLocation", config.checkpoint_path("silver_telematics"))
   .trigger(availableNow=True)
   .toTable(config.table("silver", "telematics")))
```

**SCD2 `dim_customer`:** reuse `src/silver/clean_agents_scd2.py` verbatim — swap the
source to `bronze.customers`, set `TRACKED` to the customer attributes
(`first_name,last_name,email,city,state,segment`), key on `customer_id`, drive ordering
by `updated_at`. The `mergeKey = NULL` MERGE expires the old version and inserts the new
one in a single statement.

## 3 · Best practices & pitfalls
- **`union` ≠ deduplicate.** In Spark `union` and `unionAll` are the *same* (both keep
  dupes). Add an explicit `.distinct()` if you need set semantics. Prefer **`unionByName`**
  so columns align by name, not position.
- **Broadcast only the small side.** `F.broadcast()` / `/*+ BROADCAST */` on a tiny dim
  avoids a shuffle; broadcasting a large table OOMs the executors.
- **Quarantine, don't drop.** Anti-join orphans and range failures into a `_quarantine`
  table with a `_dq_reason`; the totals feed `ops.dq_scorecard` (passed + quarantined =
  ingested).
- **`dropDuplicates(["key"])` is non-deterministic** — it keeps an arbitrary row. When
  "latest wins", use `row_number()` over a window ordered by `updated_at`.
- **Date parsing fails silently.** A wrong format yields `NULL`, not an error — always
  `coalesce` the candidate formats and then assert "parsed rate" in DQ.
- **Streaming dedup needs a watermark** to bound state; without it `dropDuplicates`
  retains keys forever and state grows unboundedly.
- **`explode` drops empty/NULL arrays** — use `explode_outer` if a policy with no
  coverages must survive.
- Cast the numeric-as-string **before** the range check, or `"$-50"` slips through as a
  string compare.

## 4 · Exam focus
**Objectives:** read bronze with PySpark/SQL, clean nulls and standardize types into new
silver tables; **joins** (inner, left, broadcast, multi-key, cross, union/union all);
column & row manipulation (**add, drop, split, rename, filter, explode**); **dedup** and
**aggregates** (count, approx_count_distinct, mean, summary); **data-quality
checks/validation** for reliable silver and gold.

**Practice questions**
1. *You must remove `claims` rows whose `policy_id` has no matching policy. Which join
   gives you exactly the rows to quarantine?* **A. Left-anti join** (`how="left_anti"`)
   on `policy_id` — it returns left rows with no right match; the inner join is the clean
   set.
2. *A tiny `ref_us_states` dim (≈50 rows) is joined to 250k customers to validate `state`.
   What avoids a shuffle?* **A. A broadcast join** — `F.broadcast(states)` / `/*+
   BROADCAST */` ships the small side to every executor.
3. *Customer rows are duplicated by the same `customer_id` with casing/whitespace
   differences, and you must keep the most recently updated row. Best approach?*
   **A. Normalize the text, then `row_number()` over a window partitioned by
   `customer_id` ordered by `updated_at` desc, keep `_rn = 1`.** (`dropDuplicates(["customer_id"])`
   keeps an arbitrary row, not the latest.)

## 5 · References
- **DataFrame transformations** — `select`, `withColumn`, `drop`, `filter`/`where`, `cast`
- **Joins** in Spark SQL/DataFrame, the **broadcast** join hint, and `union`/`unionByName`
- **`explode` / `explode_outer`** and working with nested/`array<struct>` data
- **Built-in functions** — string (`trim`, `initcap`, `regexp_replace`), date
  (`to_date`, `to_timestamp`), aggregate (`approx_count_distinct`, `summary`)
- **Deduplicating data** and **drop duplicates in streaming** with **watermarks**
- **Slowly Changing Dimensions (Type 2)** with Delta **MERGE**
- The Medallion (bronze/silver/gold) architecture
