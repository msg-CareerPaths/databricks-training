# 05 · Databricks CLI Cookbook

Recipes for the **Databricks CLI** (v0.2+) used across this project. The CLI is also exam
material (domain 5 — CI/CD). Install, authenticate, then use the snippets below.

## Install & authenticate
```bash
# macOS
brew tap databricks/tap && brew install databricks
databricks -v                                   # confirm v0.2+

# OAuth login (browser) — creates/updates a profile in ~/.databrickscfg
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com
databricks auth profiles                        # list profiles
databricks current-user me                       # who am I?
```
Use `--profile <name>` (or `DATABRICKS_CONFIG_PROFILE`) to target a specific workspace.

## Unity Catalog: catalog / schema / volume
```bash
databricks catalogs create insurance
databricks schemas  create landing insurance
databricks volumes  create insurance landing raw MANAGED
databricks volumes  list   insurance landing
```

## Files & Volumes (upload the dataset)
```bash
databricks fs cp -r --overwrite data/landing dbfs:/Volumes/insurance/landing/raw
databricks fs ls dbfs:/Volumes/insurance/landing/raw
databricks fs ls dbfs:/Volumes/insurance/landing/raw/telematics
# helper that wraps catalog+schema+volume+upload:
scripts/upload_to_volume.sh insurance data/landing
```

## Declarative Automation Bundle (deploy the job + pipeline)
```bash
databricks bundle validate -t dev               # check databricks.yml + resources/*
databricks bundle deploy   -t dev               # create/update the job + pipeline
databricks bundle run insurance_ingest -t dev   # run the job
databricks bundle run insurance_pipeline -t dev # run the pipeline
databricks bundle summary  -t dev               # what got deployed
databricks bundle destroy  -t dev               # tear down
```

## Pipelines (Lakeflow Spark Declarative Pipelines)
```bash
databricks pipelines list-pipelines
databricks pipelines start-update <pipeline_id> --full-refresh
databricks pipelines get <pipeline_id>
```

## Jobs (Lakeflow Jobs)
```bash
databricks jobs list
databricks jobs run-now <job_id>
databricks jobs list-runs --job-id <job_id> --limit 5     # run-history trend (domain 6)
```

## SQL warehouses & queries
```bash
databricks warehouses list
databricks warehouses start <warehouse_id>
```

## Git Folders (CI/CD, domain 5)
The Git workflow happens mostly in the workspace UI (create a **Git Folder**, branch,
commit, PR). The CLI complements it via bundle deploys from CI. Typical promotion:
```bash
git checkout -b feature/my-change        # work locally or in a Git Folder
databricks bundle deploy -t dev          # test in dev
# open a PR; on merge, CI runs:
databricks bundle deploy -t prod
```

## Tips
- Prefer **OAuth** (`auth login`) over personal access tokens.
- On **Free Edition**, everything is **serverless** — you won't create clusters via the CLI.
- `databricks bundle validate` is your fast feedback loop; run it before every deploy.
- Set `DATABRICKS_HOST`/`DATABRICKS_TOKEN` env vars in CI instead of interactive login.
