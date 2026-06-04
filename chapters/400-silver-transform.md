# 4. Silver — Transform & Conform [6-8 hours]

_Milestone M3 · Exam domain 3: Data Transformation and Modeling_

**Goal:**
- Turn raw bronze into clean, conformed **silver** tables — every seeded defect handled.
- Practise joins (inner/left/broadcast/multi-key/cross/union), dedup, explode, type/date
  cleaning, **MERGE / SCD2**, and the **quarantine** pattern.

## Mandatory Materials:

**Videos:**
- Databricks Academy — *Data Engineering with Databricks* (transformations module)

**Reading:**
 - [Studybook M3 — Silver Transform](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M3_silver_transform.md)
 - [Data dictionary (defect catalogue)](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/03_data_dictionary.md)
 - Worked example: [src/silver/clean_agents_scd2.py](https://github.com/msg-CareerPaths/databricks-training/blob/main/src/silver/clean_agents_scd2.py)

## Insurance Lakehouse:
 > 1. `src/silver/clean_customers.py` — dedup (exact + fuzzy), trim/casing, map `state` via the
 >    reference table (**broadcast join**), parse mixed dates, quarantine failures.
 > 2. `src/silver/conform_policies.py` — fix `annual_premium` (string/negative), map `status`,
 >    **explode** `coverages[]` into `silver.policy_coverages`, drop orphan customers (anti-join).
 > 3. `src/silver/clean_claims.py` — normalize the mixed `fraud_flag` → boolean, map
 >    `claim_status`, flag `loss_amount > sum_insured`, drop orphan policies.
 > 4. `src/silver/validate_expectations.py` — implement the pass/quarantine split + DQ counts.
 > 5. Telematics — **watermark** + dedupe late/out-of-order events into `silver.telematics`.
 > 6. Build a Type-2 `dim_customer` by reusing the worked `clean_agents_scd2.py` pattern.
 >
 > **Acceptance:** no duplicate business keys; categories canonical; `fraud_flag` is boolean;
 > orphan rows quarantined (not silently dropped); DQ counts recorded for the scorecard.

## Further Resources:
- [Deduplication & watermarks](https://docs.databricks.com/en/structured-streaming/delta-lake.html) · [MERGE / SCD2](https://docs.databricks.com/en/delta/merge.html)
