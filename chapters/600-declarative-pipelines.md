# 6. Lakeflow Declarative Pipelines [4-6 hours]

_Milestone M5 · Exam domains 2 & 3 (tool: Lakeflow Spark Declarative Pipelines)_

**Goal:**
- Re-express the whole medallion as **one** Lakeflow Spark Declarative Pipeline (formerly DLT).
- Use **streaming tables** vs **materialized views** correctly, and enforce DQ with
  **EXPECTATIONS**.

## Mandatory Materials:

**Videos:**
- Databricks Academy — *Build Data Pipelines with Lakeflow Spark Declarative Pipelines*

**Reading:**
 - [Studybook M5 — Declarative Pipelines](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M5_declarative_pipelines.md)
 - [Lakeflow Spark Declarative Pipelines docs](https://docs.databricks.com/aws/en/ldp/)
 - Stubs: [src/pipelines/insurance_dlp.sql](https://github.com/msg-CareerPaths/databricks-training/blob/main/src/pipelines/insurance_dlp.sql) · [.py](https://github.com/msg-CareerPaths/databricks-training/blob/main/src/pipelines/insurance_dlp.py)

## Insurance Lakehouse:
 > 1. Complete `src/pipelines/insurance_dlp.sql` **or** `insurance_dlp.py` (pick one language).
 > 2. Bronze as **streaming tables** (`cloudFiles`); silver as streaming tables with
 >    **EXPECTATIONS** (`expect`, `expect_or_drop`, `expect_or_fail`); gold as **materialized
 >    views**.
 > 3. Deploy the pipeline via the bundle resource
 >    [resources/insurance_pipeline.pipeline.yml](https://github.com/msg-CareerPaths/databricks-training/blob/main/resources/insurance_pipeline.pipeline.yml).
 > 4. Inspect the **event log** to see expectation pass/drop metrics.
 >
 > **Acceptance:** the pipeline runs serverless; expectations drop/track bad rows (visible in the
 > event log); gold materialized views populate from silver.

## Further Resources:
- [Expectations](https://docs.databricks.com/aws/en/ldp/expectations.html) · streaming tables vs materialized views
