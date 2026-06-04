# 10. Governance & Security [3-4 hours]

_Milestone M9 · Exam domain 7: Governance and Security_

**Goal:**
- Govern the lakehouse: managed vs external tables, **GRANT/REVOKE/DENY**, **column masking**,
  **row-level security**, and **Unity Catalog ABAC** — applied to the PII in `customers`.

## Mandatory Materials:

**Videos:**
- Databricks Academy — *Get Started with Data Governance on Databricks*

**Reading:**
 - [Studybook M9 — Governance & Security](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M9_governance_security.md)
 - [Unity Catalog privileges](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html)
 - [Row filters & column masks](https://docs.databricks.com/en/tables/row-and-column-filters.html)

## Insurance Lakehouse:
 > 1. `GRANT USE CATALOG` / `USE SCHEMA` + `SELECT` on `insurance.gold` to an analyst group.
 > 2. Add a **column mask** that hides `customers.email` / `phone` from non-privileged groups.
 > 3. Add a **row filter** so an agent group only sees rows for their own `region`.
 > 4. Try an **ABAC** policy (tag the PII columns; one policy applies masking centrally).
 > 5. Note **managed vs external** tables; review **lineage** + **audit** (system tables).
 >
 > **Acceptance:** the analyst group can read gold but not PII; the row filter restricts by
 > region; you can explain managed vs external and what ABAC centralizes.

## Further Resources:
- [ABAC policies](https://docs.databricks.com/en/data-governance/unity-catalog/abac/index.html) · [System tables / audit](https://docs.databricks.com/en/admin/system-tables/index.html)
