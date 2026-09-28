# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05_answers
# MAGIC ### §7.2 Task 4 — The four business questions
# MAGIC Each question is answered with SQL against the Gold tables built in `04_gold`, plus two or
# MAGIC three plain-language sentences a non-technical manager could act on (§7.3: click the chart
# MAGIC icon under any result to visualize it).
# MAGIC
# MAGIC Performance observations for §8 (Task 5) are recorded separately in
# MAGIC `performance_observations.dbquery.ipynb`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.2 — BQ1: Where and when are the taxis busiest?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BQ1 — Query 1: Top 10 busiest pickup zones
# MAGIC
# MAGIC SELECT
# MAGIC     borough,
# MAGIC     zone_name,
# MAGIC     SUM(trips) AS total_trips
# MAGIC FROM workspace.taxi_faysal.gold_zone_hour
# MAGIC GROUP BY
# MAGIC     borough,
# MAGIC     zone_name
# MAGIC ORDER BY
# MAGIC     total_trips DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BQ1 — Query 2: Hourly demand for the top 3 zones
# MAGIC
# MAGIC WITH top_3_zones AS ( -- Find the three busiest zones across the full quarter
# MAGIC     SELECT
# MAGIC         borough,
# MAGIC         zone_name
# MAGIC     FROM workspace.taxi_faysal.gold_zone_hour
# MAGIC     GROUP BY
# MAGIC         borough,
# MAGIC         zone_name
# MAGIC     ORDER BY
# MAGIC         SUM(trips) DESC
# MAGIC     LIMIT 3
# MAGIC )
# MAGIC
# MAGIC -- For those three zones, show trip totals at each hour (0–23)
# MAGIC SELECT
# MAGIC     g.pickup_hour,
# MAGIC     g.borough,
# MAGIC     g.zone_name,
# MAGIC     SUM(g.trips) AS total_trips
# MAGIC FROM workspace.taxi_faysal.gold_zone_hour AS g
# MAGIC
# MAGIC INNER JOIN top_3_zones AS t
# MAGIC     ON g.borough = t.borough
# MAGIC    AND g.zone_name = t.zone_name
# MAGIC
# MAGIC GROUP BY
# MAGIC     g.pickup_hour,
# MAGIC     g.borough,
# MAGIC     g.zone_name
# MAGIC
# MAGIC ORDER BY
# MAGIC     g.pickup_hour,
# MAGIC     g.zone_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.2 — BQ1 (cont.): does the airport hourly curve look different?
# MAGIC The top-3 zones above are all Manhattan. To check whether demand *shape* differs
# MAGIC elsewhere, compare that same hourly curve against JFK and LaGuardia.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BQ1 — Query 3: Manhattan (top-3) vs. airport hourly shape
# MAGIC
# MAGIC SELECT
# MAGIC     pickup_hour,
# MAGIC     CASE
# MAGIC         WHEN zone_name IN ('JFK Airport', 'LaGuardia Airport') THEN zone_name
# MAGIC         ELSE 'Top-3 Manhattan zones'
# MAGIC     END AS zone_group,
# MAGIC     SUM(trips) AS total_trips
# MAGIC FROM workspace.taxi_faysal.gold_zone_hour
# MAGIC WHERE zone_name IN (
# MAGIC     'Upper East Side South', 'Midtown Center', 'Upper East Side North',
# MAGIC     'JFK Airport', 'LaGuardia Airport'
# MAGIC )
# MAGIC GROUP BY
# MAGIC     pickup_hour,
# MAGIC     CASE
# MAGIC         WHEN zone_name IN ('JFK Airport', 'LaGuardia Airport') THEN zone_name
# MAGIC         ELSE 'Top-3 Manhattan zones'
# MAGIC     END
# MAGIC ORDER BY
# MAGIC     zone_group,
# MAGIC     pickup_hour;

# COMMAND ----------

# MAGIC %md
# MAGIC BQ1 — Non-technical manager takeaway
# MAGIC
# MAGIC
# MAGIC - During October through December 2024, the busiest pickup areas were 
# MAGIC     - Upper East Side South
# MAGIC     - Midtown Center
# MAGIC     - Upper East Side North. 
# MAGIC - These Manhattan zones peak sharply in the early evening (around 5–7 PM) and are quietest overnight (around 4–5 AM). By contrast, the JFK/LaGuardia hourly curve is flatter across the day, with smaller bumps in the morning and late evening that line up with flight schedules rather than commuter rush hour.
# MAGIC - Dispatchers should place more taxis near the Manhattan zones shortly before the evening peak, then move spare vehicles to other areas overnight; airport staging can stay steadier throughout the day since airport demand doesn't have the same sharp peak.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.2 — BQ2: What do airport pickups look like?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- BQ2: Compare JFK and LaGuardia airport pickups.
# MAGIC -- Uses only the Gold table: gold_zone_hour.
# MAGIC -- Card-only tip rate avoids treating cash trips as zero-tip trips.
# MAGIC
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN zone_name = 'JFK Airport' THEN 'JFK'
# MAGIC         WHEN zone_name = 'LaGuardia Airport' THEN 'LaGuardia'
# MAGIC     END AS airport,
# MAGIC
# MAGIC     -- Total trips, including every payment type
# MAGIC     SUM(trips) AS total_trips,
# MAGIC
# MAGIC     -- Weighted average fare:
# MAGIC     -- avg_fare is already an average at Gold-table level,
# MAGIC     -- so multiply it by trips before aggregating it again.
# MAGIC     ROUND(
# MAGIC         SUM(avg_fare * trips) / NULLIF(SUM(trips), 0),
# MAGIC         2
# MAGIC     ) AS average_fare,
# MAGIC
# MAGIC     -- Tip percentage is calculated for credit-card trips only.
# MAGIC     -- Cash tips are not recorded in the dataset.
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(card_tip_total) / NULLIF(SUM(card_fare_total), 0),
# MAGIC         2
# MAGIC     ) AS card_only_tip_pct_of_fare
# MAGIC
# MAGIC FROM workspace.taxi_faysal.gold_zone_hour
# MAGIC WHERE zone_name IN ('JFK Airport', 'LaGuardia Airport')
# MAGIC GROUP BY
# MAGIC     CASE
# MAGIC         WHEN zone_name = 'JFK Airport' THEN 'JFK'
# MAGIC         WHEN zone_name = 'LaGuardia Airport' THEN 'LaGuardia'
# MAGIC     END
# MAGIC ORDER BY airport;

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.2 — BQ2 (cont.): are JFK trips actually longer?
# MAGIC The fare/tip comparison above doesn't say whether JFK trips are longer in distance or
# MAGIC time — check that directly using the same Gold measures.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BQ2 — Query 2: trip distance/duration, JFK vs. LaGuardia
# MAGIC
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN zone_name = 'JFK Airport' THEN 'JFK'
# MAGIC         WHEN zone_name = 'LaGuardia Airport' THEN 'LaGuardia'
# MAGIC     END AS airport,
# MAGIC
# MAGIC     SUM(trips) AS total_trips,
# MAGIC
# MAGIC     -- Weighted average distance/duration (avg_* are pre-averaged at Gold-row level)
# MAGIC     ROUND(SUM(avg_distance * trips) / NULLIF(SUM(trips), 0), 2) AS avg_trip_distance_miles,
# MAGIC     ROUND(SUM(avg_duration_min * trips) / NULLIF(SUM(trips), 0), 2) AS avg_trip_duration_minutes
# MAGIC
# MAGIC FROM workspace.taxi_faysal.gold_zone_hour
# MAGIC WHERE zone_name IN ('JFK Airport', 'LaGuardia Airport')
# MAGIC GROUP BY
# MAGIC     CASE
# MAGIC         WHEN zone_name = 'JFK Airport' THEN 'JFK'
# MAGIC         WHEN zone_name = 'LaGuardia Airport' THEN 'LaGuardia'
# MAGIC     END
# MAGIC ORDER BY airport;

# COMMAND ----------

# MAGIC %md
# MAGIC #### BQ2 — Non-technical manager takeaway
# MAGIC
# MAGIC From October to December 2024, 
# MAGIC - JFK had more airport pickups than LaGuardia, with about 480,000 trips compared with about 329,000 trips. 
# MAGIC - JFK passengers paid a higher average fare ($64.94) while LaGuardia passengers tipped more as a share of the fare on card payments (23.73% vs. 18.93%). 
# MAGIC - JFK trips are also longer in distance and time than LaGuardia trips (JFK is farther from Manhattan), which is the main reason the average fare is higher, not just a pricing difference.
# MAGIC - Airport taxi availability should therefore be prioritized at JFK because of its larger demand, while LaGuardia may offer stronger tipping potential for drivers on shorter runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.2 — BQ3: How do people pay, and does it matter?

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %sql
# MAGIC -- BQ3: Payment mix by borough
# MAGIC -- Shows:
# MAGIC -- 1) Share of trips for each payment type in each borough
# MAGIC -- 2) Average passenger total amount for each payment type
# MAGIC
# MAGIC WITH payment_by_borough AS (
# MAGIC
# MAGIC     -- Step 1: Combine daily Gold rows into quarter-level borough/payment totals
# MAGIC     SELECT
# MAGIC         borough,
# MAGIC         payment_type,
# MAGIC
# MAGIC         -- Total number of trips for this borough and payment type
# MAGIC         SUM(trips) AS total_trips,
# MAGIC
# MAGIC         -- Weighted average total amount:
# MAGIC         -- avg_total is calculated at daily level, so multiply by trips
# MAGIC         -- before calculating the quarter-level average.
# MAGIC         SUM(avg_total * trips) / NULLIF(SUM(trips), 0) AS average_total_amount
# MAGIC
# MAGIC     FROM workspace.taxi_faysal.gold_payment_daily
# MAGIC
# MAGIC     GROUP BY
# MAGIC         borough,
# MAGIC         payment_type
# MAGIC ),
# MAGIC
# MAGIC borough_totals AS (
# MAGIC
# MAGIC     -- Step 2: Find total trips in each borough across every payment type
# MAGIC     SELECT
# MAGIC         borough,
# MAGIC         SUM(total_trips) AS borough_total_trips
# MAGIC     FROM payment_by_borough
# MAGIC     GROUP BY borough
# MAGIC )
# MAGIC
# MAGIC -- Step 3: Calculate trip share and convert payment codes to readable labels
# MAGIC SELECT
# MAGIC     p.borough,
# MAGIC
# MAGIC     CASE p.payment_type
# MAGIC         WHEN 1 THEN 'Credit card'
# MAGIC         WHEN 2 THEN 'Cash'
# MAGIC         WHEN 3 THEN 'No charge'
# MAGIC         WHEN 4 THEN 'Dispute'
# MAGIC         WHEN 5 THEN 'Unknown'
# MAGIC         WHEN 6 THEN 'Voided trip'
# MAGIC         WHEN 0 THEN 'Flex fare'
# MAGIC         ELSE 'Other / missing'
# MAGIC     END AS payment_method,
# MAGIC
# MAGIC     p.total_trips,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * p.total_trips / NULLIF(b.borough_total_trips, 0),
# MAGIC         2
# MAGIC     ) AS trip_share_pct,
# MAGIC
# MAGIC     ROUND(p.average_total_amount, 2) AS average_total_amount
# MAGIC
# MAGIC FROM payment_by_borough AS p
# MAGIC
# MAGIC INNER JOIN borough_totals AS b
# MAGIC     ON p.borough = b.borough
# MAGIC
# MAGIC WHERE p.borough NOT IN ('Unknown', 'N/A')
# MAGIC   AND p.borough IS NOT NULL -- todo : apply this filter at silver
# MAGIC
# MAGIC ORDER BY
# MAGIC     p.borough,
# MAGIC     trip_share_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### BQ3 — Non-technical manager takeaway
# MAGIC - Credit cards are the most common way passengers pay
# MAGIC - Passenger payment behaviour is not the same across the city, and service planning should account for each borough’s local pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7.2 — BQ4: Yellow versus green — are they the same business?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BQ4: Compare Yellow and Green taxi operations by borough.
# MAGIC -- Uses the existing Gold table only.
# MAGIC -- Note: This reports trip-weighted averages, not medians.
# MAGIC
# MAGIC SELECT
# MAGIC     service_type,
# MAGIC     borough,
# MAGIC
# MAGIC     -- Total trips for this taxi service in this pickup borough
# MAGIC     SUM(trips) AS total_trips,
# MAGIC
# MAGIC     -- Weighted average distance across all Gold rows
# MAGIC     ROUND(
# MAGIC         SUM(avg_distance * trips) / NULLIF(SUM(trips), 0),
# MAGIC         2
# MAGIC     ) AS avg_trip_distance_miles,
# MAGIC
# MAGIC     -- Weighted average duration across all Gold rows
# MAGIC     ROUND(
# MAGIC         SUM(avg_duration_min * trips) / NULLIF(SUM(trips), 0),
# MAGIC         2
# MAGIC     ) AS avg_trip_duration_minutes,
# MAGIC
# MAGIC     -- Revenue per trip
# MAGIC     ROUND(
# MAGIC         SUM(total_revenue) / NULLIF(SUM(trips), 0),
# MAGIC         2
# MAGIC     ) AS avg_revenue_per_trip
# MAGIC
# MAGIC FROM workspace.taxi_faysal.gold_zone_hour
# MAGIC
# MAGIC -- Exclude unresolved zone mappings, if present
# MAGIC WHERE borough <> 'Unknown'
# MAGIC
# MAGIC GROUP BY
# MAGIC     service_type,
# MAGIC     borough
# MAGIC
# MAGIC ORDER BY
# MAGIC     borough,
# MAGIC     service_type;

# COMMAND ----------

# MAGIC %md
# MAGIC #### BQ4 — Non-technical manager takeaway
# MAGIC
# MAGIC Yellow and Green taxi services show different trip patterns across boroughs. The number of trips, average trip distance, trip duration, and average revenue per trip vary by area for both services. 
# MAGIC - Green trips in Manhattan are a very small share of total Green volume. This is expected, not a data-quality problem: NYC regulation bars street-hail Green taxis from picking up in Manhattan below East 96th Street/West 110th Street, so Green's Manhattan trips are concentrated in the small area above that line (plus pre-arranged/app-based pickups elsewhere in the borough).
# MAGIC - This report uses trip-weighted averages rather than medians. Averages can be pulled upward by a small number of very long or very expensive trips, so they should be read as "typical scale," not "the middle trip" — a median-based cut would be a reasonable follow-up if outlier sensitivity becomes a concern.