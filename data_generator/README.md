# Insurance Data Generator

Local Python generator that produces a realistic, **intentionally dirty** multi-source
insurance dataset for the Databricks DE Associate starter project. It runs **on your
machine** (not on Databricks Free Edition, which has restricted internet); you then
upload the output to a Unity Catalog Volume with the Databricks CLI.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r data_generator/requirements.txt

# initial ~500 MB load (telematics fills to the target)
python -m data_generator.generate --mode initial --target-mb 500 --out data/landing

# a delta (~50 MB). Batch 2 introduces the telematics.device_fw schema-drift column.
python -m data_generator.generate --mode delta --batch 1 --out data/landing
python -m data_generator.generate --mode delta --batch 2 --out data/landing
```

Output mirrors the UC Volume layout — one folder per source:

```
data/landing/
  customers/   *.csv      claims/      *.json     agents/      *.csv
  policies/    *.json      payments/    *.parquet  telematics/  *.jsonl
  reference/   *.csv      _manifest.json
```

Then upload (see `docs/05_databricks_cli_cookbook.md`):

```bash
databricks fs cp -r data/landing dbfs:/Volumes/insurance/landing/raw
```

## Sources & ingestion technique each one teaches

| Source | Format | Ingestion taught (bronze) |
|---|---|---|
| customers | CSV | Auto Loader + schema evolution |
| policies | JSON (nested `coverages[]`) | Auto Loader / `read_files` (`multiLine`) |
| claims | JSON (ndjson) | Auto Loader |
| payments | Parquet | **COPY INTO** |
| agents | CSV | batch / Auto Loader (SCD2 dim) |
| telematics | JSONL | **Auto Loader streaming** (late/out-of-order) |
| reference | CSV (committed) | batch `read_files` |

## Knobs

All sizing, business rates, and **data-quality defect rates** live in
[`config.py`](config.py). Generation is **deterministic** given `--seed` (default
`20260504`) + the CLI args, so two identical runs produce identical output.

The seeded defect catalogue (and which silver task cleans each one) is documented in
`docs/03_data_dictionary.md`.

## Tests

```bash
pip install pytest
pytest tests/
```
