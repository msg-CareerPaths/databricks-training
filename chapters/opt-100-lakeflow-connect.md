# OPT-1. Lakeflow Connect (Managed Ingestion) [2 hours]

_Optional · Exam domain 2_

**Goal:** understand **Lakeflow Connect** standard + managed connectors — the low/no-code way to
ingest from enterprise sources (databases, SaaS) into Unity-Catalog tables — and when to choose
it over Auto Loader / COPY INTO. (Managed connectors generally aren't available on Free Edition,
so this is conceptual + exam prep.)

## Mandatory Materials:
**Reading:**
 - [Studybook M2 — Bronze Ingestion](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M2_bronze_ingestion.md) (the "Lakeflow Connect" + "choosing the right tool" sections)
 - [Lakeflow Connect docs](https://docs.databricks.com/en/ingestion/lakeflow-connect/index.html)

## Insurance Lakehouse:
 > Write a one-page decision note: for each of the 7 sources, which ingestion method fits and
 > why (volume, frequency, schema stability, governance) — and where a managed connector would
 > replace our Auto Loader / COPY INTO path if this ran on a full workspace.
