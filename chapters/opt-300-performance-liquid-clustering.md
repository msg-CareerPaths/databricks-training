# OPT-3. Performance & Liquid Clustering [2-3 hours]

_Optional · Exam domain 6_

**Goal:** go beyond the basics — compare **Liquid Clustering** vs partitioning vs Z-ORDER, when
`OPTIMIZE` / `VACUUM` matter, and how **predictive optimization** automates them.

## Mandatory Materials:
**Reading:**
 - [Studybook M8 — Troubleshooting & Optimization](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M8_troubleshooting_optimization.md)
 - [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html) · [OPTIMIZE](https://docs.databricks.com/en/delta/optimize.html)

## Insurance Lakehouse:
 > Add `CLUSTER BY` to `fact_claims` (e.g. on `date_key`, `policy_id`), run `OPTIMIZE`, and
 > compare a date-range query's Spark UI metrics before vs after. Write down the file-pruning
 > difference you observe.
