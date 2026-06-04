# Studybook — Index

The studybook is your **theory + code companion**. There is **one chapter per milestone**
(M0–M10), so it maps 1:1 to `docs/02_todo_checklist.md`. The checklist says *what to do*;
the studybook teaches *why and how*, at exam depth, using this project's insurance tables.

## The loop
For each milestone:
1. **Read** the studybook chapter.
2. **Do** the checklist milestone (finish the `src/` stub).
3. **Check** with the chapter's practice questions and tick the acceptance criteria.

## Chapter template (every chapter)
1. **Concept / theory** — the Databricks/Spark/Delta theory behind the milestone.
2. **Worked code** — runnable **SQL *and* PySpark** on the insurance tables.
3. **Best practices & pitfalls.**
4. **Exam focus** — the exact objectives this milestone tests + practice questions with
   answer rationales (modeled on the 5 retired samples in the official guide).
5. **References** — official Databricks docs.

## Chapters & exam domains
| Chapter | Milestone | Exam domain |
|---|---|---|
| `M0_platform_and_setup.md` | M0 | 1 · Databricks Intelligence Platform |
| `M1_generate_and_land.md` | M1 | 1→2 · Volumes, CLI, landing |
| `M2_bronze_ingestion.md` | M2 | 2 · Data Ingestion and Loading |
| `M3_silver_transform.md` *(Slice 2)* | M3 | 3 · Data Transformation and Modeling |
| `M4_gold_modeling.md` *(Slice 2)* | M4 | 3 |
| `M5_declarative_pipelines.md` *(Slice 2)* | M5 | 2, 3 |
| `M6_lakeflow_jobs.md` *(Slice 2)* | M6 | 4 · Working with Lakeflow Jobs |
| `M7_cicd_bundles.md` *(Slice 2)* | M7 | 5 · Implementing CI/CD |
| `M8_troubleshooting_optimization.md` *(Slice 2)* | M8 | 6 · Troubleshooting/Monitoring/Optimization |
| `M9_governance_security.md` *(Slice 2)* | M9 | 7 · Governance and Security |
| `M10_dashboards_and_readiness.md` *(Slice 2)* | M10 | 3, 4 |

> Reminder: the official guide publishes **no domain weights**; any percentages elsewhere
> in these docs are an unofficial study-time aid (see `00_project_guide.md`). The full
> objective→milestone map is `docs/04_exam_blueprint_map.md` (Slice 2).
