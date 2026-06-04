# 8. CI/CD — Git Folders & Automation Bundles [3-4 hours]

_Milestone M7 · Exam domain 5: Implementing CI/CD_

**Goal:**
- Deploy the job + pipeline reproducibly across **dev / test / prod** with a **Declarative
  Automation Bundle** and the **Databricks CLI**, and practise the **Git Folders** workflow.

## Mandatory Materials:

**Videos:**
- Databricks Academy — *DevOps Essentials for Data Engineering*

**Reading:**
 - [Studybook M7 — CI/CD & Bundles](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M7_cicd_bundles.md)
 - [Declarative Automation Bundles docs](https://docs.databricks.com/aws/en/dev-tools/bundles/)
 - Files: [databricks.yml](https://github.com/msg-CareerPaths/databricks-training/blob/main/databricks.yml) · [CLI cookbook](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/05_databricks_cli_cookbook.md)

## Insurance Lakehouse:
 > 1. Set your workspace `host` in `databricks.yml` (and review the `dev`/`test`/`prod` targets).
 > 2. `databricks bundle validate -t dev` → `databricks bundle deploy -t dev` →
 >    `databricks bundle run insurance_ingest -t dev`.
 > 3. Practise the **Git Folders** flow: create a branch, commit, open a PR, merge.
 > 4. Promote to another target and observe the per-user schema prefix in `dev`.
 >
 > **Acceptance:** `bundle validate` passes; the dev deploy creates the job + pipeline under your
 > user prefix; you can promote to another target with one command; you can explain how variables/
 > overrides differ per target.

## Further Resources:
- [Bundle resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources) · [Databricks Git Folders](https://docs.databricks.com/en/repos/index.html)
