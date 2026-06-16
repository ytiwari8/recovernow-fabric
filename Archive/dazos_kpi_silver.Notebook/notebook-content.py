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

# Welcome to your new notebook
# Type here in the cell editor to add code!
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

# # Dazos KPI Silver Layer
#
# Reads bronze IntakeOpportunity JSON pages from `stkipu001/dazos-bronze/`
# and produces three Delta tables in `dazos_lakehouse`:
#
# 1. `intake_opportunity_clean` — flattened, typed, deduplicated facts
# 2. `kpi_points_config` — scoring matrix (KPI category → points)
# 3. `kpi_rep_week_fact` — pivoted rep × week × category fact for Power BI
#
# Run after `dazos-puller`. Idempotent — safe to re-run.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json

# CELL ********************

# ---- Configuration -----------------------------------------------------------
STORAGE_ACCOUNT = "stkipu001"
CONTAINER = "dazos-bronze"
BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/intake_opportunity"

# CELL ********************

# ---- Read all bronze JSON pages ---------------------------------------------
# Each page is a JSON object; records live under result.data
raw = (
    spark.read
    .option("multiline", "true")
    .json(f"{BRONZE_PATH}/*/*/page_*.json")
)

records = raw.select(F.explode("result.data").alias("rec")).select("rec.*")
print(f"Total raw records: {records.count():,}")
print(f"Columns: {len(records.columns)}")

# CELL ********************

# ---- Clean + type-cast core fields ------------------------------------------
# Modified Time format from Dazos: "04-28-2026 12:58 PM"
DAZOS_TS_FMT = "MM-dd-yyyy h:mm a"
DAZOS_DATE_FMT = "MM-dd-yyyy"

clean = records.select(
    # Identifiers
    F.col("id").alias("opportunity_id"),
    F.col("`Potential No`").alias("potential_no"),
    F.col("`Potential Name`").alias("potential_name"),
    F.col("`MR Number`").alias("mr_number"),
    F.col("ParentID").alias("account_parent_id"),

    # Stage / status
    F.col("`Sales Stage`").alias("sales_stage"),
    F.col("`Admitted Status`").alias("admitted_status"),
    F.col("`VOB Status`").alias("vob_status"),
    F.col("`Is Viable VOB or Admit`").alias("is_viable_vob_or_admit"),
    F.col("`Is Internal Transfer?`").alias("is_internal_transfer"),
    F.col("`Is Converted From Lead`").alias("is_converted_from_lead"),

    # Reps (commission attribution)
    F.col("Opener").alias("opener"),
    F.col("Closer").alias("closer"),
    F.col("`BD Rep`").alias("bd_rep"),
    F.col("`Assisting Team Member`").alias("assisting_team_member"),

    # Facility / LOC
    F.col("`Treatment Program`").alias("treatment_program"),
    F.col("`Admitting to LOC`").alias("admitting_to_loc"),
    F.col("`Current LOC`").alias("current_loc"),

    # Payment
    F.col("`Payment Method`").alias("payment_method"),
    F.col("`Cash Pay Rate`").alias("cash_pay_rate"),
    F.col("`Insurance Company`").alias("insurance_company"),

    # Source / attribution
    F.col("`Lead Source`").alias("lead_source"),
    F.col("`Campaign Source`").alias("campaign_source"),
    F.col("Source").alias("source"),
    F.col("`Referring Contact`").alias("referring_contact"),

    # Existing point value
    F.col("`Intake PTS`").alias("intake_pts_raw"),

    # Timestamps
    F.to_timestamp(F.col("`Modified Time`"), DAZOS_TS_FMT).alias("modified_time"),
    F.to_timestamp(F.col("`Created Time`"), DAZOS_TS_FMT).alias("created_time"),
    F.to_timestamp(F.col("`Arrival Date and Time`"), DAZOS_TS_FMT).alias("arrival_time"),
    F.to_date(F.col("`Discharge Date`"), DAZOS_DATE_FMT).alias("discharge_date"),

    # Audit
    F.col("`Last Modified By`").alias("last_modified_by"),
)

# Cast Intake PTS to numeric
clean = clean.withColumn(
    "intake_pts",
    F.when(F.col("intake_pts_raw").rlike("^[0-9.]+$"), F.col("intake_pts_raw").cast("double"))
)

# CELL ********************

# ---- Deduplicate ------------------------------------------------------------
# Same opportunity may appear in multiple pulls; keep most recent by modified_time
w = Window.partitionBy("opportunity_id").orderBy(F.col("modified_time").desc_nulls_last())
clean = (
    clean
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn", "intake_pts_raw")
)

print(f"After dedup: {clean.count():,} records")

# CELL ********************

# ---- Derive admit_date from Modified Time when Sales Stage flipped to Admitted
# (per spec: use modified_time as the admit timestamp)
clean = clean.withColumn(
    "admit_date",
    F.when(F.col("sales_stage") == "Admitted", F.to_date(F.col("modified_time")))
)

# Week start = Monday containing admit_date
clean = clean.withColumn(
    "admit_week_start",
    F.when(
        F.col("admit_date").isNotNull(),
        F.expr("date_sub(admit_date, (dayofweek(admit_date) + 5) % 7)")
    )
)

# Month string for grouping/filtering
clean = clean.withColumn(
    "admit_month",
    F.when(F.col("admit_date").isNotNull(), F.date_format(F.col("admit_date"), "yyyy-MM"))
)

# CELL ********************

# ---- Write intake_opportunity_clean -----------------------------------------
(
    clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("intake_opportunity_clean")
)
print("✓ intake_opportunity_clean written")

# CELL ********************

# ---- KPI scoring matrix (CORRECTED) -----------------------------------------
kpi_rules = [
    # VOB categories (Viable VOBs)
    {"category": "INN Viable VOB (Purple & Red)",     "match_type": "vob",
     "vob_status_in": ["Approved - Purple", "Approved - Red"], "points": 0.5},
    {"category": "OON Viable VOBs (Green and Yellow)", "match_type": "vob",
     "vob_status_in": ["Approved - Green", "Approved - Yellow"], "points": 1.0},
 
    # Facility-group admits — LW/GAW/TED/RNGA tier (10/3 points)
    {"category": "LW - GAW - TED - RNGA (Green & Yellow Admits)", "match_type": "admit",
     "programs": ["Lotus Wellness", "Green Acres Wellness", "Tides Edge Recovery", "Recover Now Greater Atlanta"],
     "vob_status_in": ["Approved - Green", "Approved - Yellow"], "points": 10.0},
    {"category": "LW - GAW - TED - RNGA (Red Admits)", "match_type": "admit",
     "programs": ["Lotus Wellness", "Green Acres Wellness", "Tides Edge Recovery", "Recover Now Greater Atlanta"],
     "vob_status_in": ["Approved - Red"], "points": 3.0},
 
    # CRC/GLR tier (3/1 points)
    {"category": "CRC - GLR (Green & Yellow Admits)", "match_type": "admit",
     "programs": ["Chattanooga Recovery Center", "Graceland Recovery"],
     "vob_status_in": ["Approved - Green", "Approved - Yellow"], "points": 3.0},
    {"category": "CRC GLR (Red Admits)", "match_type": "admit",
     "programs": ["Chattanooga Recovery Center", "Graceland Recovery"],
     "vob_status_in": ["Approved - Red"], "points": 1.0},
 
    # Tides purple
    {"category": "Tides (Purple Admits)", "match_type": "admit",
     "programs": ["Tides Edge Recovery"],
     "vob_status_in": ["Approved - Purple"], "points": 2.0},
 
    # BD admits (inpatient = Detox/Res/Residential, outpatient = PHP/IOP)
    {"category": "BD Inpatient Admits (Green & Yellow)", "match_type": "admit",
     "vob_status_in": ["Approved - Green", "Approved - Yellow"],
     "admitting_to_loc": ["Detox", "Res", "Residential"],
     "bd_rep_required": True, "points": 3.0},
    {"category": "BD Inpatient Admits (Red)", "match_type": "admit",
     "vob_status_in": ["Approved - Red"],
     "admitting_to_loc": ["Detox", "Res", "Residential"],
     "bd_rep_required": True, "points": 1.0},
    {"category": "BD Inpatient Admits (Purple)", "match_type": "admit",
     "vob_status_in": ["Approved - Purple"],
     "admitting_to_loc": ["Detox", "Res", "Residential"],
     "bd_rep_required": True, "points": 0.5},
    {"category": "BD Outpatient Admits (Green & Yellow)", "match_type": "admit",
     "vob_status_in": ["Approved - Green", "Approved - Yellow"],
     "admitting_to_loc": ["PHP", "IOP"],
     "bd_rep_required": True, "points": 2.0},
    {"category": "BD Outpatient Admits (Red)", "match_type": "admit",
     "vob_status_in": ["Approved - Red"],
     "admitting_to_loc": ["PHP", "IOP"],
     "bd_rep_required": True, "points": 0.5},
 
    # Strategic Partner (placeholder — refine when Strategic Partner flag is defined)
    {"category": "Strategic Partner Admits", "match_type": "admit",
     "lead_source_contains": "Strategic", "points": 2.0},
 
    # Self Pay — uses VOB Status, not Payment Method
    {"category": "Selfpay", "match_type": "admit",
     "vob_status_in": ["Self Pay"], "points": 0.0},
]
 
kpi_config_df = spark.createDataFrame([
    {"category": r["category"], "points": r["points"], "rule_json": json.dumps(r)}
    for r in kpi_rules
])
(
    kpi_config_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("kpi_points_config")
)
print("✓ kpi_points_config written")


# CELL ********************

# ---- Build kpi_rep_week_fact ------------------------------------------------
# One row per (rep, week_start, category, opportunity_id) so totals can be
# summed up for any time window. Keep is_internal_transfer for filtering.

admits = clean.filter(F.col("sales_stage") == "Admitted").filter(F.col("admit_week_start").isNotNull())

# Helper to apply a rule against the admits dataframe
def apply_rule(df, rule):
    out = df
    if rule.get("programs"):
        out = out.filter(F.col("treatment_program").isin(rule["programs"]))
    if rule.get("vob_status_in"):
        out = out.filter(F.col("vob_status").isin(rule["vob_status_in"]))
    if rule.get("admitting_to_loc"):
        out = out.filter(F.col("admitting_to_loc").isin(rule["admitting_to_loc"]))
    if rule.get("bd_rep_required"):
        out = out.filter(F.col("bd_rep").isNotNull() & (F.trim(F.col("bd_rep")) != ""))
    if rule.get("selfpay_only"):
        out = out.filter(F.col("payment_method") == "Self Pay")
    if rule.get("lead_source_contains"):
        out = out.filter(F.col("lead_source").contains(rule["lead_source_contains"]))
    return out.withColumn("category", F.lit(rule["category"])).withColumn("points", F.lit(rule["points"]))

# Stack all admit-type rules
admit_rules = [r for r in kpi_rules if r["match_type"] == "admit"]
admit_facts_dfs = [apply_rule(admits, r) for r in admit_rules]
admit_facts = admit_facts_dfs[0]
for df in admit_facts_dfs[1:]:
    admit_facts = admit_facts.unionByName(df, allowMissingColumns=True)

# VOB-type rules: viable VOB rows (any opportunity with viable_vob = Yes within window)
# For the VOB KPIs we use modified_time week (when VOB was last set)
vob_base = clean.filter(F.col("is_viable_vob_or_admit") == "Yes").filter(F.col("modified_time").isNotNull())
vob_base = vob_base.withColumn(
    "vob_week_start",
    F.expr("date_sub(to_date(modified_time), (dayofweek(to_date(modified_time)) + 5) % 7)")
)

def apply_vob_rule(df, rule):
    out = df
    if rule.get("vob_status_in"):
        out = out.filter(F.col("vob_status").isin(rule["vob_status_in"]))
    return out.withColumn("category", F.lit(rule["category"])).withColumn("points", F.lit(rule["points"]))

vob_rules = [r for r in kpi_rules if r["match_type"] == "vob"]
vob_facts_dfs = [apply_vob_rule(vob_base, r) for r in vob_rules]
vob_facts = vob_facts_dfs[0]
for df in vob_facts_dfs[1:]:
    vob_facts = vob_facts.unionByName(df, allowMissingColumns=True)

# CELL ********************

# ---- Combine, attribute to Closer, write fact table -------------------------
admit_attributed = (
    admit_facts.select(
        F.col("category"),
        F.col("points"),
        F.col("opportunity_id"),
        F.col("potential_no"),
        F.col("admit_week_start").alias("week_start"),
        F.col("admit_month").alias("month"),
        F.col("closer").alias("rep"),
        F.col("treatment_program"),
        F.col("vob_status"),
        F.col("admitting_to_loc"),
        F.col("payment_method"),
        F.col("is_internal_transfer"),
        F.col("bd_rep"),
        F.col("opener"),
        F.col("modified_time"),
        F.lit("admit").alias("event_type"),
    )
)

vob_attributed = (
    vob_facts.select(
        F.col("category"),
        F.col("points"),
        F.col("opportunity_id"),
        F.col("potential_no"),
        F.col("vob_week_start").alias("week_start"),
        F.date_format(F.col("modified_time"), "yyyy-MM").alias("month"),
        F.col("closer").alias("rep"),
        F.col("treatment_program"),
        F.col("vob_status"),
        F.col("admitting_to_loc"),
        F.col("payment_method"),
        F.col("is_internal_transfer"),
        F.col("bd_rep"),
        F.col("opener"),
        F.col("modified_time"),
        F.lit("vob").alias("event_type"),
    )
)

fact = admit_attributed.unionByName(vob_attributed)

# Drop rows where rep is null (can't attribute commission)
fact = fact.filter(F.col("rep").isNotNull() & (F.trim(F.col("rep")) != ""))

(
    fact.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("kpi_rep_week_fact")
)
print(f"✓ kpi_rep_week_fact written: {fact.count():,} rows")

# CELL ********************

# ---- Quick QA ---------------------------------------------------------------
print("=" * 60)
print("Per-rep totals (last 90 days):")
display(
    fact.groupBy("rep")
    .agg(
        F.sum("points").alias("total_points"),
        F.countDistinct("opportunity_id").alias("opp_count"),
    )
    .orderBy(F.desc("total_points"))
)

# CELL ********************

print("=" * 60)
print("Per-category totals (last 90 days):")
display(
    fact.groupBy("category")
    .agg(
        F.sum("points").alias("total_points"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.desc("total_points"))
)

# CELL ********************

print("=" * 60)
print("Internal transfer breakdown:")
display(
    fact.groupBy("is_internal_transfer")
    .agg(F.sum("points").alias("total_points"), F.count("*").alias("count"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
