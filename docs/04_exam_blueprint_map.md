# 04 · Exam Blueprint Map

Every objective from the official **Databricks Certified Data Engineer Associate** exam
guide (version **May 4, 2026**) → where you practice it in this project. Use it to confirm
coverage and to find the studybook chapter / stub for any weak area.

> The official guide lists **7 domains** and **does not publish weights**. The "unofficial
> emphasis" in `00_project_guide.md` is an objective-count heuristic only.

**Exam facts:** 45 scored questions · 90 minutes · USD 200 · 2-year validity. Re-check the
live guide before sitting the exam.

---

## Domain 1 — Databricks Intelligence Platform
| Objective | Milestone | Studybook | Code |
|---|---|---|---|
| Core components: architecture, Delta Lake, Unity Catalog | M0 | `M0_platform_and_setup.md` | — |
| Compute services: characteristics, limitations, cost models; pick the right one | M0 | `M0_platform_and_setup.md` | — |

## Domain 2 — Data Ingestion and Loading
| Objective | Milestone | Studybook | Code |
|---|---|---|---|
| Batch / streaming / incremental patterns; local files & Lakeflow Connect connectors | M1–M2 | `M2_bronze_ingestion.md` | `src/bronze/*` |
| COPY INTO from cloud object storage into UC tables | M2 | `M2` | `bronze/ingest_payments_copyinto.sql` |
| Auto Loader + schema enforcement/evolution (directory listing or file notification) | M2 | `M2` | `bronze/ingest_customers_autoloader.py`, `ingest_telematics_stream.py` |
| Configure Lakeflow Connect to ingest from enterprise sources | M2 (concept) | `M2` | — (FE-limited; conceptual) |
| JDBC/ODBC or REST landing, orchestrated with Lakeflow Jobs | M2/M6 | `M2`, `M6` | — |
| Prioritize between Auto Loader / COPY INTO / connectors | M2 | `M2` | — |
| Ingest semi-structured / nested JSON into UC Delta | M2 | `M2` | `bronze/ingest_policies_claims.py` |

## Domain 3 — Data Transformation and Modeling
| Objective | Milestone | Studybook | Code |
|---|---|---|---|
| Clean bronze→silver (nulls, types) with PySpark/SQL | M3 | `M3_silver_transform.md` | `src/silver/*` |
| Joins: inner/left/broadcast/multi-key/cross/union/union all | M3 | `M3` | `silver/*` |
| Manipulate columns/rows/structure; **explode** arrays | M3 | `M3` | `silver/conform_policies.py` |
| Dedup + aggregates (count, approx_count_distinct, mean, summary) | M3–M4 | `M3`, `M4` | `silver/*`, `gold/agg_*.sql` |
| Build Gold objects: MV, view, streaming table, table | M4 | `M4_gold_modeling.md` | `gold/*.sql` |
| Apply DQ checks/validation for silver & gold | M3–M4 | `M3`, `M4` | `silver/validate_expectations.py`, `gold/dq_scorecard.sql` |
| *(tool)* Build the medallion as a Declarative Pipeline w/ expectations | M5 | `M5_declarative_pipelines.md` | `src/pipelines/*` |

## Domain 4 — Working with Lakeflow Jobs
| Objective | Milestone | Studybook | Code |
|---|---|---|---|
| Control flow: retries, conditional (branch/loop) | M6 | `M6_lakeflow_jobs.md` | `resources/insurance_ingest.job.yml` |
| Task types (notebook/SQL/dashboard/pipeline) + DAG dependencies | M6 | `M6` | `resources/insurance_ingest.job.yml` |
| Schedules & trigger types (scheduled, file arrival, table update) | M6 | `M6` | `resources/insurance_ingest.job.yml` |
| Time-based vs data-driven triggers | M6 | `M6` | — |

## Domain 5 — Implementing CI/CD
| Objective | Milestone | Studybook | Code |
|---|---|---|---|
| Databricks Git Folders: branches, commit/push, PRs | M7 | `M7_cicd_bundles.md` | — |
| Automation Bundle variables/overrides across dev/test/prod | M7 | `M7` | `databricks.yml` |
| Deploy Declarative Automation Bundles (jobs + pipelines) | M7 | `M7` | `databricks.yml`, `resources/*` |
| Databricks CLI to validate/deploy/manage bundles | M7 | `M7` | `docs/05_databricks_cli_cookbook.md` |

## Domain 6 — Troubleshooting, Monitoring & Optimization
| Objective | Milestone | Studybook | Code |
|---|---|---|---|
| Lakeflow Jobs run-history trend analysis | M8 | `M8_troubleshooting_optimization.md` | — |
| Monitor pipeline health (Jobs UI / DAG / failure rates) | M8 | `M8` | — |
| Spark UI: data skew, shuffling, disk spilling | M8 | `M8` | — |
| Liquid Clustering & predictive optimization | M8 | `M8` | — |
| Diagnose cluster startup / library conflict / OOM | M8 | `M8` | — |

## Domain 7 — Governance and Security
| Objective | Milestone | Studybook | Code |
|---|---|---|---|
| Managed vs external tables (create/modify/delete/convert) | M9 | `M9_governance_security.md` | — |
| Access control: GRANT/REVOKE/DENY across the hierarchy | M9 | `M9` | — |
| Column masking & row-level security | M9 | `M9` | — |
| Unity Catalog ABAC policies | M9 | `M9` | — |

---

### Self-assessment
For each domain, can you (a) explain the concept, (b) write the SQL/PySpark, and (c) answer
the studybook practice questions? The 10-question mixed practice exam in
`studybook/M10_dashboards_and_readiness.md` spans all 7 domains — treat a confident pass
there as your readiness gate.
