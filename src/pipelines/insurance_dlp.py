"""TODO STUB — Lakeflow Spark Declarative Pipeline (Python) for the insurance medallion.

Re-expresses bronze → silver → gold as ONE declarative pipeline with **expectations**.
Deployed by the bundle resource `resources/insurance_pipeline.pipeline.yml` (serverless).
(Lakeflow Spark Declarative Pipelines = formerly Delta Live Tables / DLT.)

Use streaming tables for incremental bronze/silver and materialized views for gold
aggregates. Exam domains 2 & 3. Studybook: `docs/studybook/M5_declarative_pipelines.md`.
"""
import dlt
from pyspark.sql import functions as F  # noqa: F401

LANDING = "/Volumes/insurance/landing/raw"


# ----------------------------- BRONZE (streaming ingest) ----------------------------- #
@dlt.table(name="bronze_customers", comment="Raw customers via Auto Loader")
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821 (spark is provided by the pipeline)
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(f"{LANDING}/customers")
    )


# TODO: add bronze_telematics (json), bronze_policies (json, multiLine), bronze_claims,
#       bronze_payments — each as a streaming table from cloudFiles.


# ----------------------------- SILVER (clean + expectations) ------------------------- #
@dlt.table(name="silver_customers")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("has_email", "email IS NOT NULL")
def silver_customers():
    # TODO: clean (trim/casing, validate state, parse dates) and dedupe.
    return dlt.read_stream("bronze_customers")


# TODO: silver_policies (+ explode coverages), silver_claims (normalize fraud_flag),
#       silver_agents (SCD2 — apply_changes), with expectations per the data dictionary.


# ----------------------------- GOLD (materialized views) ----------------------------- #
@dlt.table(name="gold_agg_loss_ratio", comment="Loss ratio by month/product_line/state")
def gold_agg_loss_ratio():
    # TODO: aggregate silver claims (incurred losses) / premium (earned) at the right grain.
    return dlt.read("silver_customers").limit(0)  # placeholder so the pipeline parses
