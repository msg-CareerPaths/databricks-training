# 1. Platform Foundations [2 hours]

_Milestone M0 · Exam domain 1: Databricks Intelligence Platform_

**Goal:**
- Get a working Databricks Free Edition workspace and the mental model behind it.
- Understand **Delta Lake** (ACID, time travel), **Unity Catalog** (catalog → schema → table/
  volume), and **serverless** compute + cost models.

## Mandatory Materials:

**Videos:**
- Databricks Academy — *Data Engineering with Databricks* (intro modules)

**Reading:**
 - [Studybook M0 — Platform & Setup](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M0_platform_and_setup.md) — theory + worked SQL/PySpark (start here)
 - [Delta Lake docs](https://docs.databricks.com/en/delta/index.html) · [Unity Catalog docs](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
 - [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)

## Insurance Lakehouse:
 > 1. Sign up for **Databricks Free Edition** and sign in.
 > 2. In **Catalog**, create a catalog named `insurance`. Note the three-level namespace
 >    (`catalog.schema.object`).
 > 3. Start a **serverless** notebook and a **serverless SQL warehouse**.
 > 4. Run `SELECT current_catalog(), current_user();` and `CREATE SCHEMA insurance.scratch;`.
 > 5. Create a tiny Delta table, `INSERT`, then `DESCRIBE HISTORY` it and query `VERSION AS OF 0`
 >    to see **time travel**.
 >
 > **Acceptance:** catalog `insurance` exists and you can create/drop a schema in it; you ran a
 > query on a serverless warehouse; you can explain Delta Lake, Unity Catalog, and serverless in
 > one sentence each.

## Further Resources:
- [Serverless compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
- [Exam blueprint map](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/04_exam_blueprint_map.md)
