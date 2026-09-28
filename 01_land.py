# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Notebook 01_land
# MAGIC ### Assignment §2.4 (catalog/schema/volume) + §4 Task 1 — Land the files
# MAGIC
# MAGIC Downloads the six Parquet trip files and the zone CSV (Appendix A, §3.1) into a Unity Catalog
# MAGIC Volume so later notebooks (Bronze/Silver/Gold) can read them.
# MAGIC
# MAGIC **Adaptation note (see README):** the assignment assumes Azure Databricks. This workspace has
# MAGIC no outbound internet access to `/Volumes` paths for `urllib.request`, so files are downloaded to
# MAGIC a `/Workspace` staging folder first and then copied into the Volume with `dbutils.fs.cp`
# MAGIC (same pattern Appendix C recommends for restricted-network workspaces).

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2.4 — Config + catalog/schema/volume
# MAGIC Creates the schema and Volume (idempotent) and builds the list of files to fetch (§3.1).

# COMMAND ----------

# Notebook: 01_land
import os, urllib.request, shutil

CATALOG, SCHEMA, VOLUME = "workspace", "taxi_faysal", "raw"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

VOL_FS = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"  # destination Volume, e.g. /Volumes/workspace/taxi_faysal/raw

# /Workspace is always writable for the current user, even when a Volume/DBFS
# path is not reachable from urllib -- used purely as a local staging area.
user = spark.sql("select current_user()").collect()[0][0]
WS_TMP = f"/Workspace/Users/{user}/.tmp_taxi_q4_2024"
os.makedirs(WS_TMP, exist_ok=True)

# Source URLs per §3.1: three months of yellow, three months of green, one zone lookup CSV.
BASE = "https://d37ci6vzurychx.cloudfront.net"
wanted  = [f"trip-data/yellow_tripdata_2024-{m:02d}.parquet" for m in (10, 11, 12)]
wanted += [f"trip-data/green_tripdata_2024-{m:02d}.parquet"  for m in (10, 11, 12)]
wanted += ["misc/taxi_zone_lookup.csv"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 Task 1 — Download to `/Workspace`, then copy into the Volume
# MAGIC "Done when" (§4): running the notebook a second time must **skip** files that are already
# MAGIC present, both in the local staging folder and in the destination Volume, rather than
# MAGIC re-downloading / re-copying them.

# COMMAND ----------

# Helper: is `name` already present in the destination Volume?
# Wrapped in try/except because dbutils.fs.ls raises if the Volume path does not exist yet
# (e.g. very first run before the CREATE VOLUME above has propagated).
def already_in_volume(name):
    try:
        return name in [f.name for f in dbutils.fs.ls(VOL_FS)]
    except Exception:
        return False

# Step 1: download anything missing from local /Workspace staging.
for rel in wanted:
    name = rel.split("/")[-1]
    ws_file = f"{WS_TMP}/{name}"

    if os.path.exists(ws_file):
        print("SKIP download (already staged):", name)
    else:
        print("DOWNLOADING ->", name)
        urllib.request.urlretrieve(f"{BASE}/{rel}", ws_file)

# Step 2: copy anything missing from the Volume. Kept as a separate loop so a partially
# failed download in Step 1 never blocks copying files that already succeeded.
for rel in wanted:
    name = rel.split("/")[-1]
    ws_file = f"{WS_TMP}/{name}"

    if already_in_volume(name):
        print("SKIP copy (already in Volume):", name)
    else:
        print("COPY -> Volume:", name)
        dbutils.fs.cp(f"file:{ws_file}", f"{VOL_FS}/{name}", recurse=False)

print("DONE staging + copy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 Task 1 — "Done when" check
# MAGIC `dbutils.fs.ls(VOL)` must list all seven files (six trip Parquet files + the zone CSV).

# COMMAND ----------

files = dbutils.fs.ls(VOL_FS)
display(files)
print("Total files in Volume:", len(files))
assert len(files) == 7, "Expecting 7 files in the Volume (6 trip files + 1 zone lookup CSV)."

# Optional: clean up local staging to save space now that files are safely in the Volume.
# try:
#     shutil.rmtree(WS_TMP)
#     print("Cleaned Workspace staging:", WS_TMP)
# except Exception as e:
#     print("Skip cleanup:", e)