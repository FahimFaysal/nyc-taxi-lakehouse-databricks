# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03_silver
# MAGIC ### §6 Task 3 — Silver
# MAGIC Purpose: turn the two differently-shaped Bronze tables (yellow + green) into one clean,
# MAGIC trusted table, `silver_trips`, that anyone can query without worrying about source quirks.
# MAGIC
# MAGIC Output: `workspace.taxi_faysal.silver_trips`, partitioned by `pickup_month` (yyyy-MM).

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "workspace"
SCHEMA  = "taxi_faysal"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE {SCHEMA}")

# ----------------------------
# Read Bronze tables
# ----------------------------
by = spark.table("bronze_yellow")
bg = spark.table("bronze_green")

print("Bronze counts -> yellow:", by.count(), "----- green:", bg.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6.1 — Conform yellow and green
# MAGIC Rename both sources to the shared target schema (see the mapping table in §6.1), add
# MAGIC `service_type` so we know which service a row came from, then stack the two with
# MAGIC `unionByName` (not `union`, which matches columns by position and would silently
# MAGIC misalign fare/distance columns if the two select lists ever drift out of order).
# MAGIC
# MAGIC Per §6.1, yellow's `airport_fee` and green's `trip_type` are intentionally dropped —
# MAGIC neither has an equivalent in the other service, so they are simply not selected.

# COMMAND ----------

# Helper casts (some fields arrive as LONG -> INT)
def to_int(col):  return F.col(col).cast(T.IntegerType())
def to_dbl(col):  return F.col(col).cast(T.DoubleType())
def to_ts(col):   return F.col(col).cast(T.TimestampType())

yellow_sel = (
    by.select(
        to_int("VendorID").alias("vendor_id"),
        to_ts("tpep_pickup_datetime").alias("pickup_ts"),
        to_ts("tpep_dropoff_datetime").alias("dropoff_ts"),
        to_int("passenger_count").alias("passenger_count"),
        to_dbl("trip_distance").alias("trip_distance"),
        to_int("RatecodeID").alias("rate_code"),
        to_int("payment_type").alias("payment_type"),
        to_int("PULocationID").alias("pu_location_id"),
        to_int("DOLocationID").alias("do_location_id"),
        to_dbl("fare_amount").alias("fare_amount"),
        to_dbl("tip_amount").alias("tip_amount"),
        to_dbl("tolls_amount").alias("tolls_amount"),
        to_dbl("total_amount").alias("total_amount"),
        to_dbl("congestion_surcharge").alias("congestion_surcharge"),
        F.lit("yellow").alias("service_type"),
        F.col("_source_file"),
        F.col("_ingest_ts")
        # airport_fee exists in yellow; spec says to drop it explicitly (we just don't select it)
    )
)

green_sel = (
    bg.select(
        to_int("VendorID").alias("vendor_id"),
        to_ts("lpep_pickup_datetime").alias("pickup_ts"),
        to_ts("lpep_dropoff_datetime").alias("dropoff_ts"),
        to_int("passenger_count").alias("passenger_count"),
        to_dbl("trip_distance").alias("trip_distance"),
        to_int("RatecodeID").alias("rate_code"),
        to_int("payment_type").alias("payment_type"),
        to_int("PULocationID").alias("pu_location_id"),
        to_int("DOLocationID").alias("do_location_id"),
        to_dbl("fare_amount").alias("fare_amount"),
        to_dbl("tip_amount").alias("tip_amount"),
        to_dbl("tolls_amount").alias("tolls_amount"),
        to_dbl("total_amount").alias("total_amount"),
        to_dbl("congestion_surcharge").alias("congestion_surcharge"),
        F.lit("green").alias("service_type"),
        F.col("_source_file"),
        F.col("_ingest_ts")
        # trip_type exists in green; spec says to drop it (we just don't select it)
    )
)

conformed = yellow_sel.unionByName(green_sel, allowMissingColumns=True)

total_conformed = conformed.count()
print("Conformed (pre-dedup, pre-QC) rows:", total_conformed)
conformed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6.2 — Derived columns
# MAGIC `pickup_date`, `pickup_hour`, `pickup_dow`, `pickup_month` (partition key for §6.5),
# MAGIC `duration_min`, and `is_airport` (JFK=132, LaGuardia=138).

# COMMAND ----------

derived = (
    conformed
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn("pickup_hour", F.hour("pickup_ts"))
    # Day-of-week: full name. If your runtime shows short names, switch to 'E'.
    .withColumn("pickup_dow", F.date_format(F.col("pickup_ts"), "EEEE"))
    .withColumn("pickup_month", F.date_format(F.col("pickup_ts"), "yyyy-MM"))  # STRING for partition pruning demo
    .withColumn("duration_min", (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / F.lit(60.0))
    .withColumn("is_airport", F.col("pu_location_id").isin([132, 138]))
)

display(derived.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6.3 — Remove duplicates
# MAGIC There is no natural id column, so a trip is considered a duplicate of another when it shares
# MAGIC the same service, vendor, pickup/dropoff timestamps, pickup/dropoff zones, and total amount
# MAGIC (the exact rule suggested in §6.3). The number removed is reported in the README.

# COMMAND ----------

dedup_key = [
    "service_type", "vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "total_amount"
]

pre_dedup = derived.count()
silver_nodup = derived.dropDuplicates(dedup_key)
post_dedup = silver_nodup.count()

dupes_removed = pre_dedup - post_dedup
print("Duplicates removed:", dupes_removed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6.4 — Apply four quality checks
# MAGIC Four of the checks suggested in §6.4, applied consistently:
# MAGIC 1. `dropoff_ts > pickup_ts` — a trip cannot end before it starts.
# MAGIC 2. `duration_min BETWEEN 1 AND 360` — drop zero-length trips and anything over 6 hours.
# MAGIC 3. `trip_distance BETWEEN 0 AND 200` — drop unrealistic distances (thousands of miles).
# MAGIC 4. `total_amount >= 0` — drop clearly corrupt negative totals (genuine refunds aside, §3.3).
# MAGIC
# MAGIC Per §6.4, a rule/rows_checked/rows_failed/pct_failed summary is printed **before** any
# MAGIC filtering happens, then pasted into the README (§9 item 3).

# COMMAND ----------

from collections import OrderedDict
from pyspark.sql import functions as F

# Step 1: define the checks clearly (names + SQL expressions).
CHECKS = OrderedDict([
    ("dropoff_after_pickup",    "dropoff_ts > pickup_ts"),
    ("duration_between_1_360",  "duration_min BETWEEN 1 AND 360"),
    ("distance_0_to_200",       "trip_distance BETWEEN 0 AND 200"),
    ("total_amount_non_negative","total_amount >= 0"),
])

# Step 2: total rows we are checking (after dedup, before QC filters).
total = silver_nodup.count()

# Step 3: count failures per rule.
rows = []
for name, rule in CHECKS.items():
    failed = silver_nodup.filter(~F.expr(rule)).count()   # UC-safe: use expr(), NOT input_file_name()
    pct    = 0.0 if total == 0 else round(100.0 * failed / total, 3)
    rows.append((name, total, failed, pct))

summary_df = (spark.createDataFrame(
                 rows,
                 schema="rule string, rows_checked long, rows_failed long, pct_failed double"
             )
             .withColumn("pct_failed_str", F.format_string("%.2f%%", F.col("pct_failed")))
)

# Step 4: display the QC summary (this is the table required in the README).
display(summary_df.orderBy("rule"))


# Optional: persist for your README/evidence
# spark.sql("USE CATALOG workspace")
# spark.sql("USE taxi_faysal")
# summary_df.write.mode("overwrite").saveAsTable("silver_qc_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6.4 (cont.) — Filter to only rows that pass all four checks
# MAGIC Built directly from the `CHECKS` dict above (Appendix A pattern) so the QC summary printed
# MAGIC above and the rows actually kept can never drift out of sync.

# COMMAND ----------

keep_expr = " AND ".join(f"({rule})" for rule in CHECKS.values())
silver_clean = silver_nodup.filter(keep_expr)

post_qc = silver_clean.count()
print("Rows after QC:", post_qc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6.5 — Save `silver_trips`
# MAGIC Written as a Delta table partitioned by `pickup_month` (three partitions, one per month) so
# MAGIC Task 5's partition-pruning observation (§8.2) has something to prune.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS silver_trips")  # idempotent rebuild
(
    silver_clean
      .write
      .format("delta")
      .mode("overwrite")
      .partitionBy("pickup_month")   # critical for the pruning demo later
      .option("overwriteSchema", "true")
      .saveAsTable("silver_trips")
)

print("Silver rows:", spark.table("silver_trips").count())
spark.table("silver_trips").printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## §6.5 — "Done when" checks
# MAGIC `silver_trips` should have both service types, be readable by partition, and have sensible
# MAGIC durations after QC. Each check below is run (not just commented out) so the result is
# MAGIC visible as evidence.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A) One row per service_type (yellow/green)
# MAGIC SELECT service_type, COUNT(*) AS rows
# MAGIC FROM workspace.taxi_faysal.silver_trips
# MAGIC GROUP BY service_type
# MAGIC ORDER BY service_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- B) Confirm partition keys present and readable
# MAGIC SELECT pickup_month, COUNT(*) AS rows
# MAGIC FROM workspace.taxi_faysal.silver_trips
# MAGIC GROUP BY pickup_month
# MAGIC ORDER BY pickup_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- C) Sanity: durations should be mostly reasonable (min/max after QC)
# MAGIC SELECT
# MAGIC   MIN(duration_min) AS min_min,
# MAGIC   MAX(duration_min) AS max_min
# MAGIC FROM workspace.taxi_faysal.silver_trips;

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6.5 — "Done when": no nulls in the three required columns
# MAGIC Expected result: all three counts below should be 0.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN pickup_ts IS NULL THEN 1 ELSE 0 END) AS null_pickup_ts,
# MAGIC     SUM(CASE WHEN pu_location_id IS NULL THEN 1 ELSE 0 END) AS null_pu_location_id,
# MAGIC     SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) AS null_total_amount
# MAGIC FROM workspace.taxi_faysal.silver_trips;

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6.5 — "Done when": zero defects survive the QC filter
# MAGIC Re-checks the same three rules from `CHECKS` (§6.4) directly against the saved
# MAGIC `silver_trips` table. Expected result: all three counts below should be 0 — if a rule
# MAGIC ever drifted out of sync with the filter that produced `silver_clean`, this would catch it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN dropoff_ts <= pickup_ts THEN 1 ELSE 0 END) AS bad_order,
# MAGIC     SUM(CASE WHEN duration_min NOT BETWEEN 1 AND 360 THEN 1 ELSE 0 END) AS bad_duration,
# MAGIC     SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END) AS negative_total
# MAGIC FROM workspace.taxi_faysal.silver_trips;