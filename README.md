# Databricks Training — Data Engineer Associate Career Path

An interactive starter project to onboard new colleagues onto Databricks and prepare them
for the **Databricks Certified Data Engineer Associate** certification (exam version of
**May 4, 2026**: 45 questions · 90 minutes · 7 domains).

You start from this repo and, guided by the **project guide**, **TODO checklist**, and
**studybook**, build a full **medallion lakehouse** (bronze → silver → gold) over a
synthetic, intentionally-dirty **insurance** dataset — then publish dashboards that answer
real business questions. Every milestone maps to an exam domain.

## Start here
1. **`docs/00_project_guide.md`** — setup → build → deploy (the spine).
2. **`docs/02_todo_checklist.md`** — your milestone worklist (M0–M10).
3. **`docs/studybook/00_index.md`** — theory + code, one chapter per milestone.

## Quickstart (generate the dataset locally)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r data_generator/requirements.txt
python -m data_generator.generate --mode initial --target-mb 500 --out data/landing
pytest tests/        # optional: verify the generator
```
Then upload to a Unity Catalog Volume and start M0 (see the project guide).

## What's in the repo
```
data_generator/   synthetic insurance data generator (7 sources, seeded DQ defects)
src/              worked examples (complete) + TODO stubs, per medallion layer + pipelines
resources/        Lakeflow Job + Declarative Pipeline bundle resources
databricks.yml    Declarative Automation Bundle (dev / test / prod)
scripts/          generate + upload helpers
dashboards/       AI/BI dashboard spec
docs/             guide, requirements, data dictionary, checklist, CLI cookbook, studybook
tests/            pytest suite for the generator + DQ rules
```

## Status
- ✅ **Slice 1** — data generator (500 MB initial + 50 MB deltas, verified), reference data,
  tests, `src/common/config.py` + worked examples, core docs + studybook **M0–M2**.
- ✅ **Slice 2** — full TODO stubs (bronze/silver/gold/pipeline), Lakeflow Job + Declarative
  Automation Bundle (`databricks.yml` + `resources/`), studybook **M3–M10**, the exam-blueprint
  map, CLI cookbook, and dashboards spec — buildable end-to-end by a participant.
- ⏸️ **Phase 2** — integration with the company career-path web app (deferred;
  `docs/06_phase2_platform_integration.md`).

Domain dataset & scope: Auto + Property P&C · Platform: Databricks Free Edition
(serverless, Unity Catalog) · Languages: Spark SQL + PySpark.
