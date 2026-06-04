# M1 · Generate Locally & Land to a UC Volume

> Bridges **domain 1 → 2**. You'll meet **Unity Catalog Volumes**, the **Databricks CLI**,
> and the landing-zone pattern that every ingestion technique in M2 builds on. Exam-wise
> this seeds domain 2's *"import data from sources such as local files"* objective.

## 1 · Concept / theory

### Why generate locally and upload?
Databricks **Free Edition** has **restricted outbound internet** — you can't `pip install`
Faker/NumPy or download datasets inside the workspace. So we generate on your laptop and
**upload the files to a Volume**. This is also a realistic enterprise pattern: data lands
as files in governed storage, then pipelines ingest it.

### Unity Catalog Volumes
A **Volume** is a UC object (sibling of tables) for **non-tabular files**. Path form:
`/Volumes/<catalog>/<schema>/<volume>/...`. Volumes are governed (GRANTs, lineage, audit)
just like tables. This project uses one Volume, `insurance.landing.raw`, with a folder per
source — the same layout the generator writes locally, so upload is a straight copy.

### The Databricks CLI
The CLI (v0.2+) drives the workspace from your terminal and is the tool the exam's CI/CD
domain expects you to know.
- **Auth:** `databricks auth login --host <workspace-url>` does **OAuth** in the browser
  and stores a **profile** (`~/.databrickscfg`). Prefer OAuth over personal access tokens.
- **Filesystem/Volumes:** `databricks fs cp/ls/...` works against `dbfs:/Volumes/...`.
- **UC objects:** `databricks catalogs|schemas|volumes create ...`.
- Later (M7) the same CLI runs `databricks bundle deploy`.

## 2 · Worked code

**Generate (local shell):**
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r data_generator/requirements.txt
python -m data_generator.generate --mode initial --target-mb 500 --out data/landing
# a delta later (batch 2 adds the telematics.device_fw schema-drift column):
python -m data_generator.generate --mode delta --batch 1 --out data/landing
```

**Authenticate + create the Volume + upload (CLI):**
```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks schemas create landing insurance
databricks volumes create insurance landing raw MANAGED
databricks fs cp -r data/landing dbfs:/Volumes/insurance/landing/raw
databricks fs ls  dbfs:/Volumes/insurance/landing/raw
```
(or just run `scripts/upload_to_volume.sh`.)

**Peek at the landed files from a notebook (SQL `read_files`):**
```sql
SELECT * FROM read_files('/Volumes/insurance/landing/raw/reference',
                         format => 'csv', header => true) LIMIT 10;
LIST '/Volumes/insurance/landing/raw/telematics';
```

## 3 · Best practices & pitfalls
- **Folder-per-source** in the Volume — each gets its own Auto Loader stream + checkpoint
  in M2. Don't dump everything in one folder.
- Uploads are **idempotent by name** — re-`cp` overwrites; new files (deltas) are added.
- Keep generated data **out of git** (`data/` is gitignored). Only the small `reference/`
  CSVs are committed (in `data_generator/reference/`).
- Use **OAuth** (`auth login`), not long-lived tokens, for your personal workspace.
- The Volume path is `dbfs:/Volumes/...` for the `fs` command but `/Volumes/...` inside
  Spark.

## 4 · Exam focus
**Objective:** import data from sources including **local files** into UC-governed tables;
know Volumes and the CLI. (CI/CD domain reuses the CLI in M7.)

**Practice questions**
1. *On Free Edition you must get a 500 MB local dataset into a governed location. Best
   approach?* **A.** Create a **UC Volume** and upload with `databricks fs cp -r`. (DBFS
   root is legacy/ungoverned; you can't download it inside FE due to restricted internet.)
2. *Which auth method should a learner use for their personal workspace CLI?* **A.** OAuth
   via `databricks auth login` (stores a profile) — preferred over a PAT.
3. *What is a Volume for?* **A.** Governing **non-tabular files** in Unity Catalog (the
   landing zone), addressable at `/Volumes/<catalog>/<schema>/<volume>`.

## 5 · References
- Unity Catalog **Volumes**; `read_files` table-valued function
- Databricks **CLI**: `auth login`, `fs`, `volumes`, profiles (`~/.databrickscfg`)
- Free Edition limitations (restricted internet, serverless-only)
