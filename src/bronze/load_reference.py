"""WORKED EXAMPLE — Bronze: batch-load the committed reference CSVs.

This is the pattern to copy for the other bronze stubs: read the raw files, attach
ingestion metadata (source file + load timestamp), and write an append-friendly
bronze Delta table in Unity Catalog. Reference data is small and static, so a plain
batch read is the right tool — no Auto Loader needed here (you will use Auto Loader
and COPY INTO for the high-volume sources in M2).

Exam domain 2 (Data Ingestion and Loading): batch ingestion, file metadata, writing
managed Delta tables to Unity Catalog.

Run it in a Databricks notebook (Free Edition serverless) after uploading the data:
    %pip install nothing — just import and call
    from src.bronze.load_reference import load_reference_tables
    load_reference_tables(spark)
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common import config

# bronze table name  ->  source CSV under landing/reference
REFERENCE_TABLES = {
    "ref_us_states": "us_states.csv",
    "ref_vehicle_makes": "vehicle_makes.csv",
    "ref_peril_codes": "peril_codes.csv",
    "ref_coverage_types": "coverage_types.csv",
    "ref_claim_status": "claim_status_ref.csv",
    "ref_postal_region": "postal_region.csv",
}


def load_reference_tables(spark=None) -> None:
    spark = spark or config.get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.schema('bronze')}")

    for table_name, fname in REFERENCE_TABLES.items():
        path = f"{config.volume_path('reference')}/{fname}"
        df = (
            spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .load(path)
            # _metadata is a hidden column available on any file-based read
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingest_ts", F.current_timestamp())
        )
        target = config.table("bronze", table_name)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target)
        )
        print(f"wrote {df.count():>6,} rows -> {target}")


if __name__ == "__main__":
    load_reference_tables()
