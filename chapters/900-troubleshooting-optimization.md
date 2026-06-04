# 9. Troubleshooting, Monitoring & Optimization [3-4 hours]

_Milestone M8 · Exam domain 6: Troubleshooting, Monitoring & Optimization_

**Goal:**
- Read performance + reliability signals (run history, **Spark UI**) and optimize: skew/spill,
  **broadcast** joins, tuning params, **Liquid Clustering**, **predictive optimization**.

## Mandatory Materials:

**Videos:**
- Databricks Academy — *Data Engineering with Databricks* (performance module)

**Reading:**
 - [Studybook M8 — Troubleshooting & Optimization](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M8_troubleshooting_optimization.md)
 - [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html) · [Predictive optimization](https://docs.databricks.com/en/optimizations/predictive-optimization.html)

## Insurance Lakehouse:
 > 1. Run a job and inspect its **run history** vs prior runs.
 > 2. Open the **Spark UI** on the telematics ⨝ policies join; find the skewed stage (max shuffle
 >    read ≫ median) and the spill metrics.
 > 3. Apply a fix: confirm **AQE skew-join**, **salt** the key, or **broadcast** the small dims;
 >    tune `spark.sql.shuffle.partitions`.
 > 4. Add **Liquid Clustering** (`CLUSTER BY`) + `OPTIMIZE` to a hot gold table; enable
 >    **predictive optimization**.
 >
 > **Acceptance:** you can explain a slow stage from Spark UI metrics and name the right fix
 > (AQE skew join / salt / broadcast); a gold table uses `CLUSTER BY`.

## Further Resources:
- [Spark UI guide](https://docs.databricks.com/en/optimizations/spark-ui-guide/index.html) · [AQE](https://docs.databricks.com/en/optimizations/aqe.html)
