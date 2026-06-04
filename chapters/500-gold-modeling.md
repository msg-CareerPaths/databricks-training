# 5. Gold — Dimensional Modeling [5-7 hours]

_Milestone M4 · Exam domain 3: Data Transformation and Modeling_

**Goal:**
- Build a **star schema** (facts + conformed dimensions) and the aggregates that answer the 8
  business questions.
- Know when to use a **table vs view vs materialized view vs streaming table**.

## Data Model
![Gold Star Schema](https://raw.githubusercontent.com/msg-CareerPaths/databricks-training/main/diagrams/gold-star-schema.svg "Gold Star Schema")

## Mandatory Materials:

**Videos:**
- Databricks Academy — *Build Data Pipelines with Lakeflow Spark Declarative Pipelines* (modeling)

**Reading:**
 - [Studybook M4 — Gold Modeling](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M4_gold_modeling.md)
 - [Business requirements](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/01_requirements.md)
 - Worked example: [src/gold/dim_date.sql](https://github.com/msg-CareerPaths/databricks-training/blob/main/src/gold/dim_date.sql)

## Insurance Lakehouse:
 > 1. Dimensions: `dim_date` (worked) + `dim_customer`, `dim_policy`, `dim_agent` (current SCD2).
 > 2. Facts: `fact_claims`, `fact_premium` (policy × month earned premium), `fact_payments`.
 > 3. Aggregates (one per requirement): `agg_loss_ratio`, `agg_claims_monthly`,
 >    `agg_agent_performance`, `agg_telematics_risk`, `agg_customer_value` — build these as
 >    **materialized views**.
 > 4. Use `count`, `approx_count_distinct`, `avg`, and `summary()` for profiling/DQ.
 >
 > **Acceptance:** every fact joins its dimensions; each of the 8 requirements resolves to a gold
 > table; loss ratio and fraud rate are sane (fraud ≈ 4%).

## Further Resources:
- [Materialized views](https://docs.databricks.com/en/views/materialized.html) · [Streaming tables](https://docs.databricks.com/en/tables/streaming.html)
