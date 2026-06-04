# M2 · Bronze — Data Ingestion and Loading

> **Exam domain 2 — Data Ingestion and Loading** (one of the two heaviest domains). This
> chapter covers the techniques the exam names explicitly: **Auto Loader** (schema
> inference + evolution; directory listing vs file notification), **COPY INTO**,
> **`read_files`**, **Lakeflow Connect**, and how to **prioritize** between them.

## 1 · Concept / theory

### Bronze layer = raw, append-only, + metadata
Bronze tables mirror the source faithfully (no cleaning — that's silver) plus ingestion
metadata (`_source_file`, `_ingest_ts`). Goal: **idempotent, incremental** capture so
re-running never double-loads.

### Auto Loader (`cloudFiles`)
Incrementally and **exactly-once** ingests files as they arrive in a folder.
- **Schema inference** — infers columns; persisted at a **`schemaLocation`** (checkpoint).
- **Schema evolution** — `cloudFiles.schemaEvolutionMode`: `addNewColumns` (default — add
  new columns and restart the stream), `rescue`, `failOnNewColumns`, `none`. New fields
  (our `telematics.device_fw`) land automatically; unexpected data goes to the
  **`_rescued_data`** column.
- **File-discovery modes:** **directory listing** (lists the folder — the default, works
  on serverless/**Free Edition**) vs **file notification** (cloud queue/event — higher
  scale, needs a non-serverless access mode → *not available on FE*). The exam asks you to
  distinguish them; you'll **use directory listing**.
- **Checkpointing** gives exactly-once + restartability. One checkpoint **per source**.
- Run continuously, or batch-incremental with **`trigger(availableNow=True)`** (process all
  new files once, then stop — ideal for scheduled jobs and the budget on FE).

### COPY INTO
Idempotent, **file-level** SQL load into an existing Delta table from object storage.
Tracks loaded files so re-runs skip them. Best for **periodic batch** loads of a known
format/schema (our **payments** Parquet). Simpler than Auto Loader but less suited to
high-frequency arrival or evolving schemas.

### `read_files` (table-valued function)
Ad-hoc/batch read of files in SQL or as a streaming source. Great for **exploration** and
**static/reference** data (`reference/` CSVs). For the nested **policies** JSON arrays use
`multiLine => true`.

### Lakeflow Connect
Managed (and standard) **connectors** that ingest from enterprise sources (databases,
SaaS) into UC tables with little code. Conceptually the "no-code/low-code" ingestion
option. *Managed connectors generally aren't available on Free Edition*, so here you learn
them conceptually and use Auto Loader/COPY INTO/`read_files` hands-on.

### Choosing the right tool (the "prioritize" objective)
| Situation | Use |
|---|---|
| Files arriving continuously/incrementally, schema may evolve | **Auto Loader** |
| Periodic batch load, stable schema, from object storage | **COPY INTO** |
| Ad-hoc/exploration, static reference, one-off | **`read_files`** |
| Enterprise DB/SaaS source, managed pipeline | **Lakeflow Connect** |

Decide on data **volume, arrival frequency, data types, and governance** needs.

## 2 · Worked code

**Auto Loader — customers CSV with schema evolution (PySpark, batch-incremental):**
```python
from src.common import config
src = config.volume_path("customers")
chk = config.checkpoint_path("bronze_customers")
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "csv")
   .option("header", "true")
   .option("cloudFiles.schemaLocation", chk)
   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
   .option("rescuedDataColumn", "_rescued_data")
   .load(src)
   .selectExpr("*", "_metadata.file_path AS _source_file", "current_timestamp() AS _ingest_ts")
 .writeStream.option("checkpointLocation", chk)
   .trigger(availableNow=True)             # process new files once, then stop
   .toTable(config.table("bronze", "customers")))
```

**COPY INTO — payments Parquet (SQL, idempotent):**
```sql
CREATE TABLE IF NOT EXISTS insurance.bronze.payments;
COPY INTO insurance.bronze.payments
FROM '/Volumes/insurance/landing/raw/payments'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true');     -- re-running skips already-loaded files
```

**`read_files` — nested policies JSON (SQL, multiLine):**
```sql
CREATE OR REPLACE TABLE insurance.bronze.policies AS
SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingest_ts
FROM read_files('/Volumes/insurance/landing/raw/policies',
                format => 'json', multiLine => true);
-- coverages stays an array<struct> here; you explode it in silver (M3).
```

**Telematics streaming + schema drift:** ingest `telematics/` exactly as the customers
example (format `json`). When you upload delta **batch 2**, the new `device_fw` column
appears; with `schemaEvolutionMode=addNewColumns` the stream picks it up automatically
(it restarts once to register the new column).

## 3 · Best practices & pitfalls
- **One `schemaLocation`/checkpoint per source** — sharing them corrupts state.
- Idempotency is free with Auto Loader/COPY INTO — **don't** hand-roll "load everything"
  reads in bronze.
- Keep **`_rescued_data`** — it catches malformed/extra fields instead of failing the load.
- On FE you must use **directory-listing** mode (file-notification needs an access mode FE
  doesn't offer). Know the difference for the exam anyway.
- `trigger(availableNow=True)` is the budget-friendly pattern: it behaves like batch but
  keeps Auto Loader's exactly-once bookkeeping.
- Bronze does **no cleaning** — resist the urge; defects are cleaned in silver (M3).

## 4 · Exam focus
**Objectives:** batch/streaming/incremental patterns; **COPY INTO** from cloud storage;
**Auto Loader** schema enforcement + evolution (directory listing or file notification);
Lakeflow Connect; JDBC/REST landing; **prioritizing** ingestion methods; semi-structured/
nested JSON into UC Delta tables.

**Practice questions**
1. *Files (CSV) arrive in a Volume throughout the day and occasionally gain a new column.
   You need exactly-once, incremental ingestion that tolerates the new column. Which tool
   + option?* **A.** **Auto Loader** with `cloudFiles.schemaEvolutionMode = addNewColumns`
   and a per-source `schemaLocation`. (COPY INTO is batch and doesn't auto-evolve as
   gracefully; `read_files` alone isn't incremental/exactly-once.)
2. *You periodically bulk-load Parquet with a stable schema from object storage into an
   existing Delta table and want re-runs to skip already-loaded files. Best tool?*
   **A.** **COPY INTO** (file-level idempotent batch load).
3. *On Free Edition (serverless), which Auto Loader file-discovery mode applies?*
   **A.** **Directory listing** — file-notification mode requires an access mode serverless/
   FE doesn't provide.
4. *Nested JSON arrays (policy `coverages[]`) need ingesting for later explosion. Which
   read?* **A.** `read_files(..., format => 'json', multiLine => true)` (or Auto Loader
   JSON), keeping the array intact in bronze.

## 5 · References
- **Auto Loader** (`cloudFiles`): schema inference/evolution, `schemaLocation`, rescued
  data, directory listing vs file notification, `trigger(availableNow)`
- **COPY INTO** syntax & idempotency
- **`read_files`** table-valued function (incl. `multiLine`)
- **Lakeflow Connect** standard & managed connectors
- The Medallion (bronze/silver/gold) architecture
