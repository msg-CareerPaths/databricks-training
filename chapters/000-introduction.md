# 0. Databricks Data Engineer Associate — Introduction

---

# [github.com/msg-CareerPaths/databricks-training](https://github.com/msg-CareerPaths/databricks-training)

```bash
git clone https://github.com/msg-CareerPaths/databricks-training.git
```

---

## Working Mode

This training takes you from zero to a working **Databricks lakehouse** and prepares you for
the **Databricks Certified Data Engineer Associate** exam. Each chapter explores a set of
concepts (backed by a studybook chapter with theory + code) and then asks you to extend one
running project — the **Insurance Lakehouse** — using what you learned.

- Work in **your own repository** (fork/clone this one) and give your mentors access (put your
  name in the repo description if your username is cryptic).
- Create a **`develop`** branch from `main` **before you start**. Commit per chapter with
  descriptive messages.
- To request a **code review**, open a **Pull Request** from `develop` to `main` and tell your
  mentor (standup or PM). Merge after approval, then continue on a fresh branch.
- Connect your repo to the workspace with **Databricks Git Folders** so notebooks, SQL, and the
  bundle live in Git.

## Timeline

Guidance only — understand the concepts before moving on.

- **Day 1**: Chapter 0, 1 (Platform), 2 (Generate & Land)
- **Day 2**: Chapter 3 (Bronze)
- **Day 3**: Chapter 4 (Silver)
- **Day 4**: Chapter 5 (Gold)
- **Day 5**: Chapter 6 (Pipelines) — **open a Pull Request**
- **Day 6**: Chapter 7 (Jobs)
- **Day 7**: Chapter 8 (CI/CD)
- **Day 8**: Chapter 9 (Optimization), Chapter 10 (Governance)
- **Day 9**: Chapter 11 (Dashboards & readiness), Optional chapters / fix review remarks

## Environment Setup

- A **Databricks Free Edition** workspace — sign up at
  [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition). It is
  serverless with Unity Catalog enabled (and has restricted internet + a compute budget — that's
  why we generate data locally and upload it).
- The **Databricks CLI** — install and run `databricks auth login` (see Chapter 2).
- **Python 3.9+** with a virtual environment for the data generator:
  `pip install -r data_generator/requirements.txt`.
- An **IDE** (VS Code or PyCharm), optionally with the Databricks extension.

## Insurance Lakehouse

You are the data engineer for a fictional **Property & Casualty insurer** writing **Auto** and
**Property** policies. The business wants a governed lakehouse and dashboards that answer
questions like monthly **loss ratio**, **claims frequency & severity**, **fraud rate**, billing
leakage, agent performance, telematics risk, customer retention, and a **data-quality
scorecard**. The raw data arrives from **seven sources** (CSV, JSON, JSONL, Parquet) and is
**intentionally dirty** — you must clean it as you build bronze → silver → gold.

![Medallion Lineage](https://raw.githubusercontent.com/msg-CareerPaths/databricks-training/main/diagrams/medallion-lineage.svg "Medallion Lineage")

The full business requirements are in
[docs/01_requirements.md](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/01_requirements.md);
the source schemas and seeded defects are in
[docs/03_data_dictionary.md](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/03_data_dictionary.md).

## Notes

- The exam version targeted here is **May 4, 2026** (45 questions · 90 min · 7 domains). Always
  re-check the live exam guide before sitting it.
- If a link is broken, tell your mentor. The deep theory for every chapter lives in the
  [studybook](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/00_index.md).
