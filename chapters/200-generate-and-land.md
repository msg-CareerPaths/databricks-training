# 2. Generate & Land the Data [2-3 hours]

_Milestone M1 · Exam domains 1 → 2: Volumes, CLI, the landing zone_

**Goal:**
- Produce the synthetic insurance dataset locally and upload it to a **Unity Catalog Volume**.
- Learn **UC Volumes** and the **Databricks CLI** (auth, profiles, `fs`, `volumes`).

## Mandatory Materials:

**Videos:**
- Databricks Academy — *Data Ingestion with Lakeflow Connect* (intro: landing data)

**Reading:**
 - [Studybook M1 — Generate & Land](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M1_generate_and_land.md)
 - [Data generator README](https://github.com/msg-CareerPaths/databricks-training/blob/main/data_generator/README.md)
 - [CLI cookbook](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/05_databricks_cli_cookbook.md) · [Unity Catalog Volumes](https://docs.databricks.com/en/volumes/index.html)

## Insurance Lakehouse:
 > 1. Create a Python venv and `pip install -r data_generator/requirements.txt`.
 > 2. Generate the data:
 >    `python -m data_generator.generate --mode initial --target-mb 500 --out data/landing`.
 > 3. Install the **Databricks CLI**; run `databricks auth login --host <workspace-url>`.
 > 4. Create the schema + Volume and upload (or run `scripts/upload_to_volume.sh`):
 >    `databricks volumes create insurance landing raw MANAGED` then
 >    `databricks fs cp -r data/landing dbfs:/Volumes/insurance/landing/raw`.
 > 5. Generate a delta later: `--mode delta --batch 1` (and `--batch 2` adds a schema-drift
 >    column). Verify with `databricks fs ls`.
 >
 > **Acceptance:** `data/landing/` has all 7 source folders (multiple files each); the Volume
 > `insurance.landing.raw` holds the uploaded tree; you can
 > `SELECT * FROM read_files('/Volumes/insurance/landing/raw/reference', format => 'csv') LIMIT 10`.

### Folder structure
```
data/landing/
  customers/*.csv   policies/*.json   claims/*.json   payments/*.parquet
  agents/*.csv      telematics/*.jsonl   reference/*.csv   _manifest.json
```

## Further Resources:
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) · run `scripts/run_local_generate.sh`
