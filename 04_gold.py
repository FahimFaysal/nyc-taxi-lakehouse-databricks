# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04_gold
# MAGIC ### §7.1 Task 4 — Gold tables
# MAGIC Builds the three Gold tables from `silver_trips` + `bronze_zone_lookup`. The zone join is
# MAGIC resolved here (a left join, so unknown zones are kept rather than dropped) so nobody
# MAGIC answering a business question in `05_answers` needs to join to a dimension table themselves.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.1 — Setup: read Silver + the zone lookup

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA  = "taxi_faysal"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE {SCHEMA}")

silver = spark.table("silver_trips")
zones  = spark.table("bronze_zone_lookup")

print("Silver rows:", silver.count(), "| Zones:", zones.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.1 — `dim_zone`
# MAGIC One row per zone: `zone_id`, `zone_name`, `borough`, `service_zone`. `LocationID` arrives from
# MAGIC the CSV with an inferred type, so it is cast to `int` here to match `pu_location_id` /
# MAGIC `do_location_id` in Silver — otherwise the join below would silently return all nulls
# MAGIC (a common gotcha called out in Appendix C).

# COMMAND ----------

dim_zone = (
    zones
    .select(
          F.col("LocationID").cast("int").alias("zone_id"),
          F.trim(F.col("Zone")).alias("zone_name"),
          F.trim(F.col("Borough")).alias("borough"),
          F.trim(F.col("service_zone")).alias("service_zone")
    )
)

spark.sql("DROP TABLE IF EXISTS dim_zone")
dim_zone.write.format("delta").mode("overwrite").saveAsTable("dim_zone")

print("dim_zone:", spark.table("dim_zone").count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.1 — `gold_zone_hour`
# MAGIC One row per pickup zone × pickup date × pickup hour × service type. Holds the required
# MAGIC measures (`trips`, `total_revenue`, `avg_fare`, `avg_distance`, `avg_duration_min`) plus three
# MAGIC extra card-only measures used later to answer BQ2 (§7.2) without recomputing from Silver:
# MAGIC cash tips are never recorded (§3.2), so tip % must be based on card fares/tips only.

# COMMAND ----------

s = spark.table("silver_trips").alias("s")
dz = spark.table("dim_zone").alias("dz")

gzh = (
    s.join(
        F.broadcast(dz),                              # small dimension -> broadcast join
        F.col("s.pu_location_id") == F.col("dz.zone_id"),
        "left"                                          # left join: unknown zones are kept, not dropped
    )
    .groupBy(
        F.col("s.pickup_month"),
        F.to_date("s.pickup_ts").alias("pickup_date"),
        F.hour("s.pickup_ts").alias("pickup_hour"),
        F.coalesce(F.col("dz.borough"), F.lit("Unknown")).alias("borough"),
        F.coalesce(F.col("dz.zone_name"), F.lit("Unknown")).alias("zone_name"),
        F.col("s.service_type")
    )
    .agg(
        # Required Gold measures (§7.1)
        F.count("*").alias("trips"),
        F.sum("s.total_amount").alias("total_revenue"),
        F.avg("s.fare_amount").alias("avg_fare"),
        F.avg("s.trip_distance").alias("avg_distance"),
        F.avg("s.duration_min").alias("avg_duration_min"),

        # Extra measures needed for BQ2 (payment_type = 1 means credit-card payment):
        F.sum(
            F.when(F.col("s.payment_type") == 1, 1).otherwise(0)
        ).alias("card_trips"),

        # Sum of fares only for card-paid trips.
        # Needed to calculate a weighted, correct card tip percentage.
        F.sum(
            F.when(F.col("s.payment_type") == 1, F.col("s.fare_amount"))
             .otherwise(F.lit(0.0))
        ).alias("card_fare_total"),

        # tip_amount contains card tips only, so we sum it only for card payments.
        F.sum(
            F.when(F.col("s.payment_type") == 1, F.col("s.tip_amount"))
             .otherwise(F.lit(0.0))
        ).alias("card_tip_total")
    )
)

spark.sql("DROP TABLE IF EXISTS gold_zone_hour")

(
    gzh.write
       .format("delta")
       .mode("overwrite")
       .partitionBy("pickup_month")
       .saveAsTable("gold_zone_hour")
)

print("gold_zone_hour rows:", spark.table("gold_zone_hour").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.1 — "Done when": `gold_zone_hour` grain check
# MAGIC The table's grain is one row per zone × date × hour × service type. Expected result:
# MAGIC `total_rows` and `distinct_keys` must be equal — if they differ, some rows were aggregated
# MAGIC into the wrong bucket and the grain is broken.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT borough, zone_name, pickup_date, pickup_hour, service_type) AS distinct_keys
# MAGIC FROM workspace.taxi_faysal.gold_zone_hour;

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.1 — `gold_payment_daily`
# MAGIC One row per borough × date × payment type. Columns: `trips`, `total_revenue`, `avg_total`,
# MAGIC `card_tip_rate` (again restricted to card payments, for the same cash-tip reason as above).

# COMMAND ----------

# If payment_type == 1 (credit card), take the original tip/fare value; otherwise 0.0.
sum_tip_card  = F.sum(F.when(F.col("s.payment_type") == 1, F.col("s.tip_amount")).otherwise(F.lit(0.0)))
sum_fare_card = F.sum(F.when(F.col("s.payment_type") == 1, F.col("s.fare_amount")).otherwise(F.lit(0.0)))

gpd = (
    s.join(F.broadcast(dz), F.col("s.pu_location_id") == F.col("dz.zone_id"), "left")
     .groupBy(
        F.col("s.pickup_month"),
        F.to_date("s.pickup_ts").alias("pickup_date"),
        F.coalesce(F.col("dz.borough"), F.lit("Unknown")).alias("borough"),
        F.col("s.payment_type")
     )
     .agg(
        F.count("*").alias("trips"),
        F.sum("s.total_amount").alias("total_revenue"),
        F.avg("s.total_amount").alias("avg_total"),
        (100.0 * sum_tip_card / F.when(sum_fare_card == 0.0, F.lit(None)).otherwise(sum_fare_card)).alias("card_tip_rate")
     )
)

spark.sql("DROP TABLE IF EXISTS gold_payment_daily")
gpd.write.format("delta").mode("overwrite").partitionBy("pickup_month").saveAsTable("gold_payment_daily")

print("gold_payment_daily rows:", spark.table("gold_payment_daily").count())