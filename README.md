# NYC Taxi Q4 2024 Lakehouse

Deliverable for **Databricks Assignment 1 — Build your first lakehouse** (`Databricks_Assignment_DPE01.pdf`).
Section numbers below (e.g. "§6.3") refer to sections in that PDF so a reviewer can jump straight
to the matching requirement.

## Overview

This project builds a small lakehouse in Databricks using NYC Yellow Taxi, Green Taxi, and taxi-zone lookup data for **October–December 2024**.

The pipeline follows the Medallion architecture:

```text
Raw Volume → Bronze → Silver → Gold → Business Analysis
```

| Layer   | Description                                                                  |
| ------- | ---------------------------------------------------------------------------- |
| Bronze  | Raw source data stored as Delta tables with ingestion metadata.              |
| Silver  | Yellow and Green trips standardized, combined, deduplicated, validated, and partitioned. |
| Gold    | Business-ready zone, hourly-demand, and payment summary tables.              |

## Environment

| Item            | Value                                        |
| --------------- | -------------------------------------------- |
| Platform        | Databricks Free account with Unity Catalog   |
| Catalog         | `workspace`                                  |
| Schema          | `taxi_faysal`                                |
| Raw-data Volume | `/Volumes/workspace/taxi_faysal/raw`         |
| Data period     | October–December 2024                        |

## Adaptation Note

The assignment originally refers to Azure. Because an Azure account was not available, the implementation uses a Databricks Unity Catalog Volume for raw-file storage instead:

```
/Volumes/workspace/taxi_faysal/raw
```

## Source Data

The following seven files were landed in the raw Volume:

| File                          | Type      |
| ----------------------------- | --------- |
| `yellow_tripdata_2024-10.parquet` | Parquet   |
| `yellow_tripdata_2024-11.parquet` | Parquet   |
| `yellow_tripdata_2024-12.parquet` | Parquet   |
| `green_tripdata_2024-10.parquet`  | Parquet   |
| `green_tripdata_2024-11.parquet`  | Parquet   |
| `green_tripdata_2024-12.parquet`  | Parquet   |
| `taxi_zone_lookup.csv`           | CSV       |

## How to Run

Run the five notebooks in this order (§9 "What to submit" — items 1–5 below are covered by these
notebooks plus this README):

| Order | Notebook     | Assignment section | Description                                                                  |
| ---- | ------------ | ------------------- | ----------------------------------------------------------------------------- |
| 1    | `01_land`    | §4 Task 1            | Creates the schema and raw Volume. Lands the seven source files.             |
| 2    | `02_bronze`  | §5 Task 2            | Creates raw Bronze Delta tables.                                             |
| 3    | `03_silver`  | §6 Task 3 (§6.1–§6.5) | Standardizes, combines, deduplicates, quality-checks, and partitions trip data. |
| 4    | `04_gold`    | §7.1 Task 4           | Creates the Gold dimension and aggregate tables.                             |
| 5    | `05_answers` | §7.2 Task 4           | Contains the four business questions, charts/results, and manager summaries. |
| —    | `performance_observations.dbquery.ipynb` | §8 Task 5 | The two performance observations, run against `silver_trips`. |

Or run the Databricks job named **Taxi-Job** (job ID: `261704465765109`) — see [Stretch Goals](#stretch-goals-11--optional-up-to-5-bonus-points) below for the workflow screenshots.

## Silver Layer (§6 Task 3)

### Table Created

`workspace.taxi_faysal.silver_trips`

Yellow and Green taxi records were standardized into one common trip schema. The table includes a `service_type` field with values:

* `yellow`
* `green`

The Silver table is partitioned by `pickup_month` with example values `2024-10`, `2024-11`, `2024-12`.

### Main Transformations

* Standardized Yellow and Green source fields to a shared snake_case schema.
* Created common timestamp columns: `pickup_ts`, `dropoff_ts`.
* Added derived fields: `pickup_date`, `pickup_hour`, `pickup_dow`, `pickup_month`, `duration_min`, `is_airport`.
* Added `service_type` to distinguish Yellow and Green Taxi trips.
* Removed duplicate records.
* Excluded service-specific columns that were not part of the unified schema:
  * Yellow Taxi `Airport_fee`
  * Green Taxi `trip_type`

### Deduplication Result (§6.3)

| Measure                       | Value       |
| ------------------------------ | ----------- |
| Rows before deduplication      | 11,310,874  |
| Duplicates removed             | 4           |
| Rows after dedup (pre-QC)      | 11,310,870  |
| **Final `silver_trips` row count (post-QC)** | **10,991,149** |

### Data-Quality Checks (§6.4)

Quality checks were calculated before records were filtered. The QC summary was saved as `workspace.taxi_faysal.silver_qc_summary`.

| rule                       | rows_checked | rows_failed | pct_failed | pct_failed_str |
| -------------------------- | ------------ | ----------- | ---------- | -------------- |
| distance_0_to_200          | 11310870     | 342         | 0.003      | 0.00%          |
| dropoff_after_pickup       | 11310870     | 4238        | 0.037      | 0.04%          |
| duration_between_1_360     | 11310870     | 148708      | 1.315      | 1.32%          |
| total_amount_non_negative  | 11310870     | 192953      | 1.706      | 1.71%          |

#### Core Null Validation

The following required fields were checked in `silver_trips`:

| Column          | Null count |
| --------------- | --------- |
| `pickup_ts`     | 0         |
| `pu_location_id` | 0        |
| `total_amount`  | 0         |

#### Post-Filter Zero-Defect Check

The same three QC rules were re-run directly against the saved `silver_trips` table (not just the in-memory pre-filter counts above) to confirm no failing rows survived the filter:

| Check           | Expected | Result |
| --------------- | -------- | ------ |
| `bad_order` (`dropoff_ts <= pickup_ts`) | 0 | 0 |
| `bad_duration` (`duration_min NOT BETWEEN 1 AND 360`) | 0 | 0 |
| `negative_total` (`total_amount < 0`) | 0 | 0 |

## Gold Layer (§7.1 Task 4)

### Tables Created

| Table                                | Purpose                                                  |
| ------------------------------------ | -------------------------------------------------------- |
| `workspace.taxi_faysal.dim_zone`       | Taxi zone dimension with zone and borough details.        |
| `workspace.taxi_faysal.gold_zone_hour` | Zone-level hourly demand and trip metrics.               |
| `workspace.taxi_faysal.gold_payment_daily` | Daily payment-type metrics by pickup borough.         |

### dim_zone Columns

`zone_id`, `zone_name`, `borough`, `service_zone`

### Gold-Design Note

The assignment specifies three Gold tables only. The `gold_zone_hour` table contains aggregated distance and duration measures, so BQ4 uses trip-weighted average distance and duration from the Gold layer. An exact overall median cannot be reconstructed from pre-aggregated averages alone without retaining trip-level values or adding another Gold table.

### Gold Grain Check

`gold_zone_hour`'s intended grain is one row per zone × date × hour × service type. A `COUNT(*)` vs. `COUNT(DISTINCT borough, zone_name, pickup_date, pickup_hour, service_type)` check confirms `total_rows == distinct_keys`, so no rows were aggregated into the wrong bucket.



## Business Questions (§7.2 Task 4)

All SQL queries, tables/charts, and manager-friendly explanations are available in notebook `05_answers`.

### BQ1 — Where and When Are Taxis Busiest?

The analysis identifies the ten busiest pickup zones across the quarter, shows the hourly demand pattern for the three busiest zones, and compares that Manhattan hourly shape against JFK/LaGuardia.

> **Manager takeaway:**
> - During October through December 2024, the busiest pickup areas were
>     - Upper East Side South
>     - Midtown Center
>     - Upper East Side North.
> - These Manhattan zones peak sharply in the early evening (around 5–7 PM) and are quietest overnight (around 3–4 AM). By contrast, the JFK/LaGuardia hourly curve is flatter across the day, with smaller bumps in the morning and late evening that line up with flight schedules rather than commuter rush hour.
> - Dispatchers should place more taxis near the Manhattan zones shortly before the evening peak, then move spare vehicles to other areas overnight; airport staging can stay steadier throughout the day since airport demand doesn't have the same sharp peak.

### BQ2 — How Do JFK and LaGuardia Airport Pickups Compare?

The analysis compares airport trip volume, average fare, tip rate, and — as an added check — average trip distance/duration.

> **Note:** Tip rate uses credit-card trips only because cash tips are not recorded in the source data.

> **Manager takeaway:**
> From October to December 2024,
> - JFK had more airport pickups than LaGuardia, with about 480,000 trips compared with about 329,000 trips.
> - JFK passengers paid a higher average fare ($64.94) while LaGuardia passengers tipped more as a share of the fare on card payments (23.73% vs. 18.93%).
> - JFK trips are also longer in distance and time than LaGuardia trips (JFK is farther from Manhattan), which is the main reason the average fare is higher, not just a pricing difference.
> - Airport taxi availability should therefore be prioritized at JFK because of its larger demand, while LaGuardia may offer stronger tipping potential for drivers on shorter runs.

### BQ3 — How Does Payment Mix Vary by Borough?

The analysis compares payment-method trip share and average total trip amount across boroughs. Trip share is calculated **within each borough** (not as a share of all trips citywide), matching the intended denominator.

> **Manager takeaway:**
> - Credit cards are the most common way passengers pay
> - Passenger payment behaviour is not the same across the city, and service planning should account for each borough's local pattern.

### BQ4 — How Do Yellow and Green Taxi Services Differ?

The analysis compares total trips, average trip distance, average trip duration, and average revenue per trip by borough and service type.

> **Manager takeaway:** Yellow and Green taxi services show different trip patterns across boroughs. The number of trips, average trip distance, trip duration, and average revenue per trip vary by area for both services.
> - Green trips in Manhattan are a very small share of total Green volume. This is expected, not a data-quality problem: NYC regulation bars street-hail Green taxis from picking up in Manhattan below East 96th Street/West 110th Street, so Green's Manhattan trips are concentrated in the small area above that line (plus pre-arranged/app-based pickups elsewhere in the borough).
> - This report uses trip-weighted averages rather than medians. Averages can be pulled upward by a small number of very long or very expensive trips, so they should be read as "typical scale," not "the middle trip."

### Important Assignment Conflict — BQ4 Median vs. Gold-Table Design

There is one meaningful issue to note:

* BQ4's hint recommends **median** distance and duration using `percentile_approx` or `percentile_cont`.
* But the specified three Gold tables retain **average** distance and duration in `gold_zone_hour`, not the underlying trip-level fields needed to calculate a true overall median later.
* Since the assignment specifies three Gold tables, the current approach is to use **trip-weighted averages** from `gold_zone_hour` and clearly label them as averages.

## Performance Observations (§8 Task 5)

### Observation A — OPTIMIZE (§8.1)

Table tested: `workspace.taxi_faysal.silver_trips`

| Measure                     | Before OPTIMIZE       | After OPTIMIZE        |
| --------------------------- | -------------------- | -------------------- |
| Number of files             | 9                    | 9                    |
| Total table size            | 200,787,741 bytes    | 200,787,741 bytes    |
| Approximate average file size | 22.31 MB           | 22.31 MB             |

Running OPTIMIZE did not change the Silver table: it remained at 9 files with a total size of 200,787,741 bytes. Spark can be slowed by many small files because it must spend extra time opening, scheduling, and tracking each file before processing data. The average file size remained approximately 22.31 MB, indicating that this table did not require additional file compaction.

### Observation B — Partition Pruning (§8.2)

The `silver_trips` table is partitioned by `pickup_month`.

| Metric        | Direct partition filter           | Timestamp-derived filter                              |
| ------------- | -------------------------------- | ---------------------------------------------------- |
| Filter        | `pickup_month = '2024-11'`       | `date_format(pickup_ts, 'yyyy-MM') = '2024-11'`      |
| Duration      | 6.473 seconds                     | 1.423 seconds                                        |
| Bytes read    | 5.82 MB                           | 50.72 MB                                             |
| Files read    | 1                                | 9                                                    |
| Trip count    | 3,597,093                        | 3,597,093                                            |
| Total amount  | $101,743,747.84                  | $101,743,747.84                                      |

Both queries returned the same November 2024 result: 3,597,093 trips and a total amount of $101,743,747.84. The direct `pickup_month` filter read only 1 file and 5.82 MB, while the timestamp-derived filter read all 9 files and 50.72 MB. This demonstrates partition pruning because Spark can skip the October and December partitions when a query filters directly on the partition column.

The timestamp-derived query finished faster in this individual run, but elapsed duration can vary because of query startup, caching, and compute conditions. Files read and bytes read provide the clearest evidence that partition pruning occurred.

## Stretch Goals (§11 — optional, up to 5 bonus points)

### S1 — Turn it into a job

A Databricks Workflow named **Taxi-Job** (job ID `261704465765109`) chains all five notebooks in
order: `01_land → 02_bronze → 03_silver → 04_gold → 05_answers`, matching the dependency graph
required by §11.

**Graph view** — five tasks chained in order:

![Taxi-Job workflow graph](job1.png)

**Timeline view** — per-task duration for a single run:

![Taxi-Job workflow timeline](job2.png)

**List view** — per-task status and lineage:

![Taxi-Job workflow task list](job3.png)

### S2 — Build a dashboard

A Databricks SQL dashboard, **"NYC Taxi Q4 2024 — Gold Insights,"** built from the Gold tables,
with three charts as required: the hourly demand curve, the payment-method mix by pickup borough,
and airport pickups (JFK vs. LaGuardia) as the "anything else interesting" chart.

![NYC Taxi Q4 2024 Gold Insights dashboard](dashboard.png)

## What I Learned

* Bronze, Silver, and Gold layers have different purposes: Bronze preserves source data, Silver standardizes and validates it, and Gold prepares it for business reporting.
* Partitioning by a frequently filtered field, such as `pickup_month`, can substantially reduce the amount of data Spark must read.
* OPTIMIZE is most useful when a table has many small files; it may make no changes when the existing file layout is already suitable.

## Assignment Notes and Assumptions

* The project uses a Databricks Unity Catalog Volume because Azure storage was unavailable.
* Unity Catalog does not support `input_file_name()` in this environment. Source-file lineage was captured using `_metadata.file_name`.
* The assignment specifies three Gold tables. The BQ4 requirement and its median-related hint create a design limitation because medians cannot be calculated accurately from already aggregated average values. To remain within the required three-Gold-table design, BQ4 reports trip-weighted averages and labels them clearly as averages.
* **Fix applied during review:** `03_silver` previously filtered `total_amount` with an undocumented `<= 1000` upper bound that was not reflected in the printed QC summary (§6.4 only specifies `total_amount >= 0`). The filter now derives directly from the same `CHECKS` dict used to print the summary, so the two can never disagree. Re-run `03_silver` (and everything downstream) after pulling this fix.
* **Fix applied during review:** `01_land`'s `already_in_volume()` helper was missing its `return` statement in the success path, so it always evaluated as falsy and every re-run re-copied all seven files into the Volume instead of skipping ones already present (§4 "Done when"). This is now fixed.

## Final Submission Checklist

* [ ] Cluster auto-termination is set to 10 minutes.
* [ ] Cluster is stopped.
* [x] All seven files are present in the raw Volume.
* [x] Bronze tables exist and preserve raw source data.
* [x] Silver contains both Yellow and Green taxi records.
* [x] No null values remain in `pickup_ts`, `pu_location_id`, or `total_amount`.
* [x] QC rule results are included in this README.
* [x] `dim_zone`, `gold_zone_hour`, and `gold_payment_daily` exist.
* [x] All four business questions include SQL, output, and a manager-friendly explanation.
* [x] BQ2 explains the cash-tip limitation.
* [x] Both performance observations contain real measured values.
* [x] All five notebooks rerun successfully from top to bottom.
* [x] No notebook uses a personal hard-coded storage path.

