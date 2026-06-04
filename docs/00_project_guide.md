# 00 · Project Guide — Databricks DE Associate Career Path

Welcome! This project turns the **Databricks Certified Data Engineer Associate** exam
syllabus into something you *build*. You start from this repo (a data generator + worked
examples + TODO stubs) and, milestone by milestone, construct a full **medallion
lakehouse** over a synthetic **insurance** dataset — then publish dashboards. Every
milestone maps to an exam domain, and a **studybook** teaches the theory behind it.

## How to use this repo

1. **Read** the studybook chapter for the milestone (`docs/studybook/M<n>_*.md`).
2. **Do** the milestone in `docs/02_todo_checklist.md` (fill in the `src/` stubs).
3. **Check** yourself with the chapter's practice questions, and tick the acceptance
   criteria. Then move to the next milestone.

```
docs/
  00_project_guide.md      <- you are here (setup → build → deploy)
  01_requirements.md       <- the 8 business questions your gold layer must answer
  02_todo_checklist.md     <- M0–M10 worklist (your primary checklist)
  03_data_dictionary.md    <- source schemas + the seeded data-quality defects
  04_exam_blueprint_map.md <- every exam objective → milestone/stub/doc
  05_databricks_cli_cookbook.md <- CLI recipes (auth, volumes, bundle, jobs)
  studybook/               <- one theory+code chapter per milestone
data_generator/            <- run locally to produce the dataset
src/                       <- worked examples (complete) + TODO stubs you finish
```

---

## The exam (authoritative — May 4 2026 guide)

> Source of truth: the official *Databricks Certified Data Engineer Associate* exam
> guide (version **May 4, 2026**). Always re-check the live guide before your exam.

| Fact | Value |
|---|---|
| Scored questions | **45** multiple-choice |
| Time limit | **90 minutes** |
| Registration fee | **USD 200** (+ local tax) |
| Delivery | online or test center · no test aids |
| Prerequisite | none (course + ~6 months hands-on **recommended**) |
| Validity | 2 years (recertify on the current exam) |

The guide lists **7 domains** of objectives. **It does not publish percentage weights**
— the percentages below are an **UNOFFICIAL** study-time aid we derived from the number
of objectives per domain. **Do not treat them as official.**

| # | Domain | Unofficial emphasis* | Built in |
|---|---|---|---|
| 1 | Databricks Intelligence Platform | ~6% | M0 |
| 2 | Data Ingestion and Loading | ~21% | M1–M2, M5 |
| 3 | Data Transformation and Modeling | ~21% | M3–M4, M5 |
| 4 | Working with Lakeflow Jobs | ~12% | M6 |
| 5 | Implementing CI/CD | ~12% | M7 |
| 6 | Troubleshooting, Monitoring & Optimization | ~15% | M8 |
| 7 | Governance and Security | ~12% | M9 |

*\*Unofficial — objective-count share, **not** an official weighting. The guide assigns
no weights.* Full objective-by-objective mapping: `docs/04_exam_blueprint_map.md`.

### Naming you must know (the guide uses the *new* names)
- **Lakeflow Spark Declarative Pipelines** = formerly *Delta Live Tables (DLT)*.
- **Lakeflow Jobs** = formerly *Workflows / Jobs*.
- **Declarative Automation Bundles** (config `databricks.yml`, CLI `databricks bundle`)
  = formerly *Databricks Asset Bundles (DABs)*.
- **Databricks Git Folders** = formerly *Repos*.
- **Lakeflow Connect** = managed/standard ingestion connectors.

---

## M0 · Set up your personal Databricks (Free Edition)

**Databricks Free Edition** is a no-cost, serverless workspace with Unity Catalog enabled
— perfect for this project. Sign up at **databricks.com/learn/free-edition** with an email.

What you get / its limits (these matter for the exam *and* this project):
- **Serverless compute only** — no custom clusters or GPUs. Great, because the exam's
  serverless/compute questions are exactly this model.
- **Unity Catalog is required** for governed tables and Volumes.
- **Restricted outbound internet** — you **cannot** pip-install big libraries or download
  datasets from inside the workspace. That is why this project generates data **locally**
  and uploads it (next section).
- **A daily/monthly compute budget** — if you exhaust it, compute pauses until it resets;
  your data and notebooks are safe. Keep runs small; stop streams when done.

First steps in the workspace: open **Catalog** (you'll see Unity Catalog), create a
catalog named `insurance`, and confirm you can start a **serverless** SQL warehouse and a
serverless notebook. (Details + screenshots-by-words are in `studybook/M0_*`.)

---

## M1 · Local toolchain, the CLI, and landing the data

### 1) Generate the dataset locally
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r data_generator/requirements.txt
python -m data_generator.generate --mode initial --target-mb 500 --out data/landing
```
This writes ~500 MB across the seven source folders (see `03_data_dictionary.md`). The
output stays **gitignored**. Generate a delta later with
`--mode delta --batch 1` (and `--batch 2` introduces the schema-drift column).

### 2) Install & authenticate the Databricks CLI
```bash
# macOS (Homebrew)
brew tap databricks/tap && brew install databricks
databricks -v

# log in to your workspace (OAuth in the browser); creates a CLI profile
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks auth profiles          # verify
```

### 3) Create the catalog/schema/Volume and upload
```bash
databricks catalogs create insurance || true
databricks schemas create landing insurance
databricks volumes create insurance landing raw MANAGED

# upload the whole landing tree into the Volume
databricks fs cp -r data/landing dbfs:/Volumes/insurance/landing/raw
databricks fs ls dbfs:/Volumes/insurance/landing/raw
```
A helper script (`scripts/upload_to_volume.sh`) wraps these. The full CLI reference is in
`docs/05_databricks_cli_cookbook.md`.

> **Why upload instead of generating in the cloud?** Free Edition's restricted internet
> blocks installing Faker/NumPy and pulling data inside the workspace. Generating locally
> and uploading via the CLI is both the practical path **and** a realistic ingestion
> pattern (land files in object storage → ingest with Auto Loader / COPY INTO).

---

## M2–M10 · Build the lakehouse (overview)

You now have raw files in a Volume. The rest of the path (each milestone has a studybook
chapter + checklist entry):

- **M2 — Bronze:** ingest each source with the right tool — Auto Loader (+schema
  evolution) for customers/telematics, `read_files`/`multiLine` for policies, COPY INTO
  for payments. *(Domain 2)*
- **M3 — Silver:** clean every seeded defect (dedup, nulls→quarantine, type/category
  fixes, MERGE/SCD2, orphan-FK handling, watermarking). *(Domain 3)*
- **M4 — Gold:** star schema (facts + dims) + the aggregates the dashboards need.
  *(Domain 3)*
- **M5 — Declarative Pipelines:** re-express the medallion as a Lakeflow Spark Declarative
  Pipeline with `EXPECTATIONS`. *(Domains 2–3)*
- **M6 — Lakeflow Jobs:** orchestrate land→pipeline→DQ-test→publish as a DAG with retries,
  conditional tasks, and triggers. *(Domain 4)*
- **M7 — CI/CD:** Git Folders workflow + a Declarative Automation Bundle (`databricks.yml`)
  with dev/test/prod targets, deployed via the CLI. *(Domain 5)*
- **M8 — Troubleshooting/Monitoring/Optimization:** Spark UI (skew/spill), Liquid
  Clustering, predictive optimization, run-history, alerts. *(Domain 6)*
- **M9 — Governance:** managed vs external tables, GRANT/REVOKE/DENY, masking, RLS, ABAC,
  lineage, audit. *(Domain 7)*
- **M10 — Dashboards:** answer the 8 business questions on a serverless SQL warehouse,
  plus the exam-readiness review. *(Domains 3–4)*

---

## Studybook & checklist loop

Open `docs/studybook/00_index.md` for the chapter list. For each milestone: **read the
chapter → do the checklist item → answer the practice questions**. The worked examples in
`src/` (`bronze/load_reference.py`, `silver/clean_agents_scd2.py`, `gold/dim_date.sql`)
show the pattern; the TODO stubs are where you apply it.

## Exam-readiness checklist (high level)
- [ ] I can explain Delta Lake (ACID, time travel) and Unity Catalog's object hierarchy.
- [ ] I can choose between Auto Loader, COPY INTO, `read_files`, and Lakeflow Connect.
- [ ] I can clean data and build SCD2 dimensions with MERGE.
- [ ] I can build streaming tables vs materialized views and add expectations.
- [ ] I can orchestrate with Lakeflow Jobs (retries, conditional tasks, triggers).
- [ ] I can deploy with a Declarative Automation Bundle via the CLI and use Git Folders.
- [ ] I can read the Spark UI for skew/shuffle/spill and know Liquid Clustering.
- [ ] I can apply GRANT/REVOKE/DENY, masking, RLS, and ABAC in Unity Catalog.
