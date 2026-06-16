# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "98a8d990-4a33-4845-b492-8359b66e5259",
# META       "default_lakehouse_name": "dazos_lakehouse",
# META       "default_lakehouse_workspace_id": "fb72ebcf-98cc-4162-85c9-5d2042b8b795",
# META       "known_lakehouses": [
# META         {
# META           "id": "98a8d990-4a33-4845-b492-8359b66e5259"
# META         }
# META       ]
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
# META       "default_lakehouse_name": "dazos_lakehouse"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Dazos Silver Layer
# 
# Builds SCD2 tables for 4 modules: IntakeOpportunity, Leads, VOB, Accounts.
# 
# Output:
#   intake_opportunity_current / intake_opportunity_history
#   leads_current              / leads_history
#   vob_current                / vob_history
#   accounts_current           / accounts_history

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, ArrayType
import re

STORAGE = "stkipu001"
CONTAINER = "dazos-bronze"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE}.dfs.core.windows.net"

# CELL ********************

def sanitize_columns(df):
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
        raw.select(F.col("snapshot_date"), F.explode("result.data").alias("rec"))
        .select("snapshot_date", "rec.*")
    )
    return sanitize_columns(records)


def detect_pk(df, candidates=("id", "potentialid", "leadid", "vob_id", "accountid")):
    for c in candidates:
        if c in df.columns:
            return c
    raise RuntimeError(f"No PK found. Cols: {df.columns[:30]}")


def detect_modified_col(df, candidates=("Modified_Time", "modifiedtime")):
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


# CELL ********************

def build_scd2(folder: str, current_table: str, history_table: str):
    print(f"\n=== {folder} ===")

    df = read_bronze(folder)

    # Drop nested struct/array columns — Delta can't handle nested field names with spaces
    nested_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (StructType, ArrayType))]
    if nested_cols:
        print(f"  Dropping nested columns: {nested_cols}")
        df = df.drop(*nested_cols)

    n_raw = df.count()
    print(f"  Raw records (all snapshots): {n_raw:,}")

    if n_raw == 0:
        print(f"  ⚠ no records — skipping")
        return

    pk = detect_pk(df)
    mod_col = detect_modified_col(df)
    print(f"  PK: {pk}, modified col: {mod_col}")

    if mod_col:
        order_col = F.col(mod_col).desc_nulls_last()
    else:
        order_col = F.col(pk).desc()

    w_dedup = Window.partitionBy("snapshot_date", pk).orderBy(order_col)
    df = df.withColumn("_rn", F.row_number().over(w_dedup)).filter("_rn = 1").drop("_rn")
    print(f"  After intra-snapshot dedupe: {df.count():,}")

    exclude = ["snapshot_date", "row_hash", pk]
    if mod_col:
        exclude.append(mod_col)
    df = add_row_hash(df, exclude_cols=exclude)

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
    versions = versions.withColumn(
        "valid_to",
        F.date_sub(F.lead("valid_from").over(w_next), 1)
    )

    print(f"  History rows: {versions.count():,}")

    (versions.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(history_table))
    print(f"  ✓ {history_table}")

    current = versions.filter(F.col("valid_to").isNull())
    print(f"  Current rows: {current.count():,}")

    (current.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(current_table))
    print(f"  ✓ {current_table}")


# CELL ********************

build_scd2("intake_opportunity", "intake_opportunity_current", "intake_opportunity_history")

# CELL ********************

build_scd2("leads", "leads_current", "leads_history")

# CELL ********************

build_scd2("vob", "vob_current", "vob_history")

# CELL ********************

build_scd2("accounts", "accounts_current", "accounts_history")

# CELL ********************

print("=== Silver layer summary ===")
for tbl in [
    "intake_opportunity_current", "intake_opportunity_history",
    "leads_current", "leads_history",
    "vob_current", "vob_history",
    "accounts_current", "accounts_history",
]:
    try:
        n = spark.table(tbl).count()
        print(f"  {tbl:<35} {n:>10,} rows")
    except Exception as e:
        print(f"  {tbl:<35} ERROR: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
