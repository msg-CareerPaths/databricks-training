# OPT-4. CI with GitHub Actions [2 hours]

_Optional · Exam domain 5_

**Goal:** automate the bundle workflow — run `databricks bundle validate` (and the generator
tests) on every pull request, and deploy to `prod` on merge to `main`.

## Mandatory Materials:
**Reading:**
 - [Studybook M7 — CI/CD & Bundles](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M7_cicd_bundles.md)
 - [CI/CD with Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/ci-cd.html)

## Insurance Lakehouse:
 > Add a `.github/workflows/ci.yml` that, on PR: sets up Python, runs `pytest tests/`, and runs
 > `databricks bundle validate -t dev` (using `DATABRICKS_HOST` / `DATABRICKS_TOKEN` secrets). On
 > push to `main`, add a job that runs `databricks bundle deploy -t prod`. Explain why secrets go
 > in CI, not in `databricks.yml`.
