# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7d8e32d8-17fe-4c76-bb9a-3c2893720aa6",
# META       "default_lakehouse_name": "zoho_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795",
# META       "known_lakehouses": [
# META         {
# META           "id": "7d8e32d8-17fe-4c76-bb9a-3c2893720aa6"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "9cec1e09-b29c-ada4-4fe5-a917061e3807",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "zoho_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Zoho Silver Layer
#
# Builds SCD2 tables from zoho-bronze. For now: Leads ("Inquiries").
#
# Output (in zoho_lakehouse):
#   zoho_leads_current   — latest version of each Lead
#   zoho_leads_history   — every version (valid_from / valid_to)
#
# **Reads:** abfss://zoho-bronze@stkipu001.dfs.core.windows.net/leads/*/*/page_*.json
#
# Twin of Dazos_Silver_Layer. The Zoho puller already flattened lookup/owner
# objects and Tag to flat strings, so records sit directly under `data`
# (not `result.data` like Dazos). PK = Zoho `id`; modified col = Modified_Time.
#
# **PREREQUISITE:** create a lakehouse named `zoho_lakehouse` in the KIPU
# Dashboard workspace and attach it as this notebook's default lakehouse.
#
# **Schedule:** 08:00 UTC daily (same window as Dazos silver), after the
# zoho-puller at 06:45 UTC.


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, ArrayType
import re

STORAGE   = "stkipu001"
CONTAINER = "zoho-bronze"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE}.dfs.core.windows.net"

print(f"Zoho silver reading from: {BASE_PATH}")

# ════════════════════════════════════════════════════════════════════════════
# CELL 1
# ════════════════════════════════════════════════════════════════════════════

def sanitize_columns(df):
    """Replace Delta-illegal characters in column names. Keeps casing
    (Zoho uses Title_Case API names like First_Name, Modified_Time)."""
    seen = {}
    new_cols = []
    for c in df.columns:
        clean = re.sub(r"[ ,;{}()\n\t=?]", "_", c).strip("_")
        if clean.lower() in seen:
            seen[clean.lower()] += 1
            clean = f"{clean}_{seen[clean.lower()]}"
        else:
            seen[clean.lower()] = 0
        new_cols.append(clean)
    return df.toDF(*new_cols)


def read_bronze(folder: str):
    """Read all page JSON for a module. Zoho puller writes records under the
    top-level `data` array, so we explode that."""
    path = f"{BASE_PATH}/{folder}/*/*/page_*.json"
    raw = (
        spark.read
        .option("multiline", "true")
        .json(path)
        .withColumn("_source_file", F.input_file_name())
    )
    raw = raw.withColumn(
        "snapshot_date",
        F.to_date(F.regexp_extract("_source_file", r"/(\d{4}-\d{2}-\d{2})/", 1))
    )
    records = (
        raw.select(F.col("snapshot_date"), F.explode("data").alias("rec"))
           .select("snapshot_date", "rec.*")
    )
    return sanitize_columns(records)


def detect_pk(df, candidates=("id", "Id", "ID")):
    for c in candidates:
        if c in df.columns:
            return c
    raise RuntimeError(f"No PK found. Cols: {df.columns[:30]}")


def detect_modified_col(df, candidates=("Modified_Time", "modifiedtime", "Modified_Time__s")):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def add_row_hash(df, exclude_cols):
    exclude = set(exclude_cols)
    cols_to_hash = sorted([c for c in df.columns if c not in exclude])
    expr = F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols_to_hash]),
        256
    )
    return df.withColumn("row_hash", expr)

# ════════════════════════════════════════════════════════════════════════════
# CELL 2
# ════════════════════════════════════════════════════════════════════════════

def build_scd2(folder: str, current_table: str, history_table: str):
    print(f"\n=== {folder} ===")

    df = read_bronze(folder)

    # Drop any nested struct/array cols that survived flattening (safety net)
    nested_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (StructType, ArrayType))]
    if nested_cols:
        print(f"  Dropping nested columns: {nested_cols}")
        df = df.drop(*nested_cols)

    n_raw = df.count()
    print(f"  Raw records (all snapshots): {n_raw:,}")
    if n_raw == 0:
        print("  ⚠ no records — skipping")
        return

    pk = detect_pk(df)
    mod_col = detect_modified_col(df)
    print(f"  PK: {pk}, modified col: {mod_col}")

    order_col = F.col(mod_col).desc_nulls_last() if mod_col else F.col(pk).desc()

    # De-dupe within a snapshot (keep newest by modified time)
    w_dedup = Window.partitionBy("snapshot_date", pk).orderBy(order_col)
    df = df.withColumn("_rn", F.row_number().over(w_dedup)).filter("_rn = 1").drop("_rn")
    print(f"  After intra-snapshot dedupe: {df.count():,}")

    exclude = ["snapshot_date", "row_hash", pk]
    if mod_col:
        exclude.append(mod_col)
    df = add_row_hash(df, exclude_cols=exclude)

    # Detect version changes across snapshots via row_hash
    w_pk = Window.partitionBy(pk).orderBy("snapshot_date")
    df = df.withColumn("_prev_hash", F.lag("row_hash").over(w_pk))
    df = df.withColumn(
        "is_new_version",
        F.when(F.col("_prev_hash").isNull(), F.lit(True))
         .when(F.col("_prev_hash") != F.col("row_hash"), F.lit(True))
         .otherwise(F.lit(False))
    )

    versions = df.filter(F.col("is_new_version")).drop("_prev_hash", "is_new_version")

    w_ver = Window.partitionBy(pk).orderBy("snapshot_date")
    versions = (
        versions
        .withColumn("version", F.row_number().over(w_ver))
        .withColumnRenamed("snapshot_date", "valid_from")
    )

    w_next = Window.partitionBy(pk).orderBy("valid_from")
    versions = versions.withColumn("valid_to", F.date_sub(F.lead("valid_from").over(w_next), 1))

    print(f"  History rows: {versions.count():,}")
    (versions.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(history_table))
    print(f"  ✓ {history_table}")

    current = versions.filter(F.col("valid_to").isNull())
    print(f"  Current rows: {current.count():,}")
    (current.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(current_table))
    print(f"  ✓ {current_table}")

# ════════════════════════════════════════════════════════════════════════════
# CELL 3
# ════════════════════════════════════════════════════════════════════════════

build_scd2("leads", "zoho_leads_current", "zoho_leads_history")

# ════════════════════════════════════════════════════════════════════════════
# CELL 4
# ════════════════════════════════════════════════════════════════════════════

# --- summary + funnel-field sanity ---
print("=== Silver layer summary ===")
for tbl in ["zoho_leads_current", "zoho_leads_history"]:
    try:
        n = spark.table(tbl).count()
        print(f"  {tbl:<24} {n:>8,} rows")
    except Exception as e:
        print(f"  {tbl:<24} ERROR: {e}")

cur = spark.table("zoho_leads_current")

# Confirm the funnel-critical fields survived
need = ["id", "VOB_Status", "Admitted_Status", "Admission_Date", "Level_of_Care",
        "Facility_Admit_to", "Location", "Tag",
        "Referral_Source", "Referring_Company", "BD_Contact_Owner",
        "Phone", "Client_Phone_Number", "Date_of_Birth", "Created_Time", "Modified_Time"]
present = [c for c in need if c in cur.columns]
missing = [c for c in need if c not in cur.columns]
print("\nFunnel fields present:", present)
if missing:
    print("⚠ MISSING (check Zoho API names):", missing)

# Facility-signal distributions (drives EDTC vs Longbranch later)
for col in ["Location", "Facility_Admit_to", "Tag"]:
    if col in cur.columns:
        print(f"\n=== {col} distinct values ===")
        cur.groupBy(col).count().orderBy(F.col("count").desc()).show(40, truncate=False)

# Stage signal
for col in ["VOB_Status", "Admitted_Status"]:
    if col in cur.columns:
        print(f"\n=== {col} ===")
        cur.groupBy(col).count().orderBy(F.col("count").desc()).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
