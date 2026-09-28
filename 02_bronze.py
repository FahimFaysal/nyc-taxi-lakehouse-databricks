# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02_bronze
# MAGIC ### §5 Task 2 — Bronze
# MAGIC Reads the raw files landed by `01_land` and saves them as Delta tables, unchanged, so we always
# MAGIC have a safety copy to re-run Silver from without re-downloading.
# MAGIC
# MAGIC Rule (§5): do not rename business columns, do not filter rows, do not fix types beyond what is
# MAGIC needed to read the file. Only two columns are added: `_source_file` and `_ingest_ts`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — Setup: catalog/schema + confirm the 7 raw files are present

# COMMAND ----------

# Notebook: 02_bronze
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA  = "taxi_faysal"
RAW_VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

# Always make sure we are writing/reading in the right catalog/schema.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE {SCHEMA}")

display(dbutils.fs.ls(RAW_VOL))  # should list 7 files (matches 01_land's "Done when" check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — `bronze_yellow`: all three yellow files, as-is, plus audit columns

# COMMAND ----------

yellow_df = (
    spark.read
         .option("mergeSchema", True)  # be tolerant of tiny schema diffs between months
         .parquet(f"{RAW_VOL}/yellow_tripdata_2024-*.parquet")
         # Capture the source file name via _metadata (UC-friendly).
         .withColumn("_source_file", F.col("_metadata.file_name"))
         # Simple audit column: when we ingested into Bronze.
         .withColumn("_ingest_ts",  F.current_timestamp())
)

# Idempotent write: drop if exists, then overwrite (Bronze = raw as-arrived + audit cols)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.bronze_yellow")
(
    yellow_df.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_yellow")
)

print("bronze_yellow rows:", spark.table(f"{CATALOG}.{SCHEMA}.bronze_yellow").count())

spark.table(f"{CATALOG}.{SCHEMA}.bronze_yellow").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — `bronze_green`: all three green files, same treatment as yellow

# COMMAND ----------

green_df = (
    spark.read
         .option("mergeSchema", True)
         .parquet(f"{RAW_VOL}/green_tripdata_2024-*.parquet")
         .withColumn("_source_file", F.col("_metadata.file_name"))
         .withColumn("_ingest_ts",  F.current_timestamp())
)

spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.bronze_green")
(
    green_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_green")
)

print("bronze_green rows:", spark.table(f"{CATALOG}.{SCHEMA}.bronze_green").count())
spark.table(f"{CATALOG}.{SCHEMA}.bronze_green").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — `bronze_zone_lookup`: the zone CSV, headers + inferred types

# COMMAND ----------

# The CSV has headers; we ask Spark to infer the schema.
# We still add the same two audit columns, using _metadata.file_name for lineage.
zone_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(f"{RAW_VOL}/taxi_zone_lookup.csv")
         .withColumn("_source_file", F.col("_metadata.file_name"))
         .withColumn("_ingest_ts",  F.current_timestamp())
)

spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.bronze_zone_lookup")
(
    zone_df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_zone_lookup")
)

print("bronze_zone_lookup rows:", spark.table(f"{CATALOG}.{SCHEMA}.bronze_zone_lookup").count())
spark.table(f"{CATALOG}.{SCHEMA}.bronze_zone_lookup").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — "Done when" checks
# MAGIC `bronze_yellow` should have ~11M rows, and each `_source_file` group (one per month) should
# MAGIC show a sensible row count for both yellow and green.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A) Yellow monthly distribution (should show 3 rows: 2024-10, 2024-11, 2024-12)
# MAGIC SELECT _source_file, COUNT(*) AS rows
# MAGIC FROM workspace.taxi_faysal.bronze_yellow
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;
# MAGIC
# MAGIC -- B) Green monthly distribution (3 rows)
# MAGIC SELECT _source_file, COUNT(*) AS rows
# MAGIC FROM workspace.taxi_faysal.bronze_green
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;
# MAGIC
# MAGIC -- C) Distinct boroughs from the lookup (sanity check)
# MAGIC -- SELECT DISTINCT Borough
# MAGIC -- FROM workspace.taxi_faysal.bronze_zone_lookup
# MAGIC -- ORDER BY 1;